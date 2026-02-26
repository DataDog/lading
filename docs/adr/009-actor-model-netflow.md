# ADR-009: Actor-Model Network Flow Generation

## Status

**Draft**

## Date

2026-02-10

## Context

Lading needs to load test the Datadog Agent's netflow component. This requires
generating NetFlow v5/v9 packets at high throughput with realistic content.

The existing payload generation approach in lading produces data by randomly
filling protocol fields. For formats like DogStatsD or syslog this works: the
target parses individual messages and random field values still exercise the
parser. For payloads with state embedded in messages this approach does not
serve well. Recent [work](https://github.com/DataDog/lading/pull/1755) by @tobz
suggests we can square this by running a model of a stateful scenario, then
encoding the side-effect of that model as the payload. This is the approach
taken here for Netflow. Work by @StephenWakely on
[dogstatsd](https://github.com/DataDog/lading/pull/1732) suggests this might be
of general interest, however only Netflow is considered in this ADR.

Consider what "completely random" means for NetFlow v5 flow records:

- Source IP `224.0.0.1` paired with destination port `22` over protocol `17`
  (UDP). Multicast addresses don't SSH, and SSH is TCP, not UDP.
- TCP flags `0xFF` (every flag set simultaneously). No real TCP implementation
  produces this.
- A flow showing 4 billion octets over a 1-second interval on an interface with
  a reported speed of 10 Mbps.
- Source and destination in different /8 subnets sharing the same next-hop
  router, with AS numbers that don't correspond to either network.

Each field is individually valid. The flow, while technically valid, does not
represent 'realistic' sufficient to make claims about the target, except in
extremis.

## Decision

**Adopt an actor-model simulation where network actors execute behavioral
sequences that incidentally produce NetFlow flow records.**

Rather than generating flow records directly, we simulate a network of
hosts doing things hosts do. Flow records are a byproduct of that simulation,
just as they are on a real network.

### Exporters Model Network Devices

Each exporter in the configuration represents a single network device — a
router, firewall, or L3 switch — that observes traffic and exports flow
records. This matches real-world NetFlow deployment: routers export flows
describing the traffic they forward; the collector (Datadog Agent) aggregates
by exporter address.

An exporter has:

- **Bind address** (`addr`): The IP address the exporter's UDP socket binds to.
  The Datadog Agent uses the source IP of received packets as the `ExporterAddr`
  aggregation key.
- **Protocol version**: `v5` or `v9`. Different exporters can use different
  versions, matching real networks where legacy and modern devices coexist.
- **Source ID** (`source_id`): The v9 observation domain ID or v5
  `engine_type`/`engine_id`. Identifies this exporter in the packet header.
- **Flows per second**: The throttle rate for this exporter.
- **Actor pool**: The set of network hosts whose traffic this exporter observes.
  Each exporter has its own independent actor pool — an edge router sees its
  subnet, a core router sees different subnets.

The generator manages one UDP socket, one throttle, and one actor simulation
per exporter. These run concurrently and independently.

### Actors Have Identity

Each actor represents a host on the simulated network, observed by the
exporter (router) it belongs to. An actor has:

- **IP address**: Drawn from configured subnets (e.g., `10.0.1.0/24` for
  servers, `10.0.2.0/24` for desktops)
- **AS number**: Assigned per subnet, consistent across actors in the same
  network
- **SNMP interface indices**: Input and output interfaces on the router
  observing this actor's traffic
- **Subnet mask**: Matching the configured subnet
- **Next-hop router**: The gateway for the actor's subnet

These fields appear in every flow record the actor produces. Because they come
from the actor's identity rather than random generation, they are internally
consistent: source IP, subnet mask, AS number, and next-hop all agree.

### Actors Have Roles

Each actor is assigned a role that determines its behavioral distribution. The
system ships 23 roles spanning infrastructure, application, IoT, and endpoint
categories. Every role has a configurable weight — roles with weight zero create
no actors.

**Infrastructure:**

| Role | Typical Behaviors | Default Weight |
|------|-------------------|----------------|
| `Desktop` | Web browsing, DNS lookups, email, printing | 50 |
| `WebServer` | Serves HTTP/HTTPS responses | 20 |
| `DnsServer` | Answers DNS queries (UDP/53) | 5 |
| `DatabaseServer` | MySQL/3306, Postgres/5432, MongoDB/27017 | 10 |
| `FileServer` | SMB/445, NFS/2049, FTP/21 | 15 |
| `LoadBalancer` | High fan-in, distributes to backends | 0 |
| `MailServer` | SMTP/25/587, IMAP/143/993 | 0 |
| `CacheServer` | Memcached/11211, Redis/6379 | 0 |
| `MonitoringServer` | SNMP/161, syslog/514 | 0 |
| `CiCdServer` | Pulls repos, pushes artifacts | 0 |
| `VpnGateway` | ESP protocol 50, IKE UDP/500 | 0 |
| `NtpServer` | NTP UDP/123 | 0 |
| `LdapServer` | LDAP TCP/389, LDAPS TCP/636 | 0 |
| `ProxyServer` | WebBrowsing/80/443, CacheLookup/6379/11211, MetricCollection/161/514 | 0 |

**Application-tier:**

| Role | Typical Behaviors | Default Weight |
|------|-------------------|----------------|
| `Microservice` | WebBrowsing/80/443, DatabaseQuery/3306/5432/27017, CacheLookup/6379/11211, MessagePubSub/5672/9092/4222, SearchQuery/9200/9300 | 0 |
| `ApiGateway` | Fan-in from clients, fan-out to services | 0 |
| `MessageQueue` | Kafka/9092, RabbitMQ/5672, NATS/4222 | 0 |
| `SearchCluster` | Elasticsearch/9200, /9300 | 0 |

**IoT / edge:**

| Role | Typical Behaviors | Default Weight |
|------|-------------------|----------------|
| `IotSensor` | Tiny periodic UDP telemetry | 0 |
| `IotGateway` | Aggregates sensor data, forwards upstream | 0 |

**User endpoints:**

| Role | Typical Behaviors | Default Weight |
|------|-------------------|----------------|
| `MobileDevice` | Bursty browsing, frequent DNS | 0 |
| `Printer` | Print jobs on IPP/631, JetDirect/9100 | 0 |
| `VoipPhone` | SIP UDP/5060 | 0 |

Roles with a default weight of zero are opt-in: set their weight to any
positive value to include them in the simulation. A microservices-heavy
deployment might set `Microservice` to 60 and `ApiGateway` to 10 while
zeroing `Desktop`. An IoT simulation might use only `IotSensor` and
`IotGateway`. The configuration controls the topology.

### Configuration Is King

Every parameter in the actor simulation is configurable. Defaults model a
mixed-traffic enterprise network, but the user can override any subset to match
their target environment.

**Network topology:**
- `actor_count`: `ConfRange<u16>`, 1 to 65535.
- `role_weights`: 23 weights controlling the mix of actors.
- `subnets`: `Vec<SubnetConfig>`, one or many, each with `cidr` (IPv4 network
  address), `mask` (prefix length 0-32), and `weight` for actor placement:
  ```yaml
  subnets:
    - cidr: "10.0.1.0"
      mask: 24
      weight: 60
    - cidr: "10.0.2.0"
      mask: 24
      weight: 40
  ```
- `autonomous_system`: `ConfRange<u16>` of AS numbers assigned to actors.
- `interfaces`: `ConfRange<u16>` of SNMP ifIndex values (default 1-48).

**Traffic volume:**
- `behaviors_per_tick`: `ConfRange<u8>`, how many behaviors fire each
  simulation tick. A **tick** is one simulation step producing one NetFlow
  datagram. Each tick advances the simulated router uptime by 1 second
  (`base_uptime = tick_count * 1000` ms). One tick executes per iteration of
  the generator's spin loop — i.e., per packet sent.

**Traffic profile:** Each of the 17 behavior types has per-behavior byte count,
packet count, and duration ranges with realistic defaults. Users can override
any subset. The defaults produce a plausible mixed-traffic enterprise network.

### Behaviors Produce Correlated Flow Pairs

Each behavior is a semantic action that produces one or more pairs of
unidirectional flow records. NetFlow v5 records are unidirectional — a TCP
connection appears as two flows (one per direction). Behaviors encode this:

**Web browsing session:**
1. DNS lookup: desktop sends small UDP packet to DNS server port 53, DNS server
   replies with small UDP packet from port 53
2. HTTP request: desktop sends small TCP SYN+ACK+PSH to web server port 80,
   web server replies with large TCP SYN+ACK+PSH+FIN from port 80

Each step produces a pair of flow records (request direction, response
direction) with:

- **Matching IP pairs**: Source of one is destination of the other
- **Consistent ports**: Server port matches the protocol (53 for DNS, 80 for
  HTTP, 443 for HTTPS, 3306 for MySQL)
- **Appropriate TCP flags**: Progress through connection lifecycle
  (SYN, SYN+ACK, ACK, PSH+ACK, FIN+ACK)
- **Realistic byte/packet ratios**: Requests are small (few hundred bytes),
  responses are large (tens of kilobytes). DNS responses are larger than
  queries but both are small.
- **Protocol consistency**: UDP for DNS, TCP for HTTP, TCP for database
  connections

The system includes 17 behavior types covering common network interaction
patterns:

| Behavior | Protocol | Ports | Description |
|----------|----------|-------|-------------|
| `WebBrowsing` | TCP | 80, 443 | HTTP/HTTPS request-response |
| `DatabaseQuery` | TCP | 3306, 5432, 27017 | SQL/NoSQL queries |
| `BulkTransfer` | TCP | 20 | Large sustained file transfers |
| `DnsLookup` | UDP | 53 | Name resolution |
| `HealthCheck` | TCP | various | SYN+ACK+FIN, zero payload |
| `BackgroundNoise` | ICMP | — | Pings, keep-alives |
| `MailTransfer` | TCP | 25, 143, 587, 993 | SMTP/IMAP email exchange |
| `CacheLookup` | TCP | 11211, 6379 | Redis/Memcached queries |
| `NtpSync` | UDP | 123 | Time synchronization |
| `LdapAuth` | TCP | 389, 636 | Directory authentication |
| `MessagePubSub` | TCP | 5672, 9092, 4222 | AMQP/Kafka/NATS messaging |
| `SearchQuery` | TCP | 9200, 9300 | Elasticsearch queries |
| `FileTransfer` | TCP | 445, 2049 | SMB/NFS file operations |
| `PrintJob` | TCP | 631, 9100 | IPP/JetDirect print submission |
| `VoipCall` | UDP | 5060 | SIP signaling + RTP media |
| `MetricCollection` | UDP | 161, 514 | SNMP polls, syslog |
| `VpnTunnel` | UDP | 1194, 500 | OpenVPN/IKE encrypted tunnels |

Each behavior type has independently configurable byte/packet ranges via
`BehaviorRanges`; defaults are realistic and most users will not need to
override them.

### Dedicated Generator, Not Block Cache

NetFlow **cannot use the pre-computed block cache** described in
[ADR-002](002-precomputation-philosophy.md). The block cache pre-computes all
payload bytes at initialization and replays them verbatim at runtime. This works
for stateless protocols (syslog, DogStatsD, JSON) where the exact byte content
doesn't matter — only the structure and size. NetFlow packets carry temporal
state that collectors actively validate:

- **Header timestamps** (`sys_uptime`, `unix_secs`): Collectors use these to
  convert flow record timestamps to absolute wall-clock time. Frozen or random
  values break aggregation windows and time-series alignment.
- **Sequence numbers** (`flow_sequence` / `sequence_number`): Collectors use
  these for packet loss detection. When the cache cycles and the sequence resets,
  collectors interpret this as catastrophic packet loss.
- **Flow record timestamps** (`first`, `last`): These are milliseconds relative
  to `sys_uptime`. Frozen values loop when the cache cycles.

The temporal compression makes this concrete. Each call to `to_bytes()` advances
the simulation by one tick (1 simulated second), producing one datagram (max
1464 bytes for v5). At 100 MiB/s a generator sends ~71,600 datagrams per
second — burning through ~71,600 simulated seconds (~19.9 hours) per real
second. The entire cache cycles in about 1 second, then timestamps and sequence
numbers loop. A header-patching approach (rewriting timestamps and sequence
numbers at send time) would require patching up to 256 bytes per 1464-byte
packet (header fields plus per-record `first`/`last`), with wire-format-aware
logic for v9 template offsets. This reimplements most of the serialization.

ADR-002 itself acknowledges this class of exception: *"Timestamps (must be
current for many protocols), Sequence numbers (must increment)"*. NetFlow is
the first payload type where these exceptions dominate the packet content.

NetFlow is therefore a **dedicated generator** (`lading/src/generator/netflow.rs`),
not a payload type consumed by the existing UDP generator. This follows the
precedent set by `ProcessTree`, `FileTree`, `Container`, and `Kubernetes` —
generators that own their simulation and use `lading_throttle::Throttle`
directly, bypassing the block cache and `lading_payload::Serialize` entirely.

Each exporter's spin loop:

1. **At init**: Create actor pool, assign identities and roles from
   configuration and seeded RNG. Bind a UDP socket to the exporter's `addr`.
   Initialize the throttle (flows/sec).
2. **At runtime**: Throttle → advance the simulation by one tick → serialize
   flows into a datagram with live wall-clock timestamps and a monotonically
   increasing sequence counter → `send_to(collector_addr)`. The simulation
   (`tick()`) is cheap — RNG sampling and struct fills, no I/O, no complex
   allocation.

Each exporter owns its clock, sequence counter, socket, and throttle. The
`Serialize` trait is not involved — the generator calls the simulation and
serialization functions directly.

### Determinism

Per [ADR-003](003-determinism-requirements.md), all actor simulation state flows
through seeded RNGs:

- Actor pool creation (IP assignment, role assignment) uses the configured seed
- Behavior selection uses per-actor RNGs derived from the master seed
- Flow record field generation (byte counts, packet counts, durations) uses
  the same deterministic RNG chain

Same seed, same configuration = same actor pool = same behaviors = same flow
record content (IPs, ports, byte counts, protocols). The flow content is
deterministic. Header timestamps (`sys_uptime`, `unix_secs`) and sequence
numbers are derived from wall-clock time and send order, so they vary across
runs — this is intentional and consistent with ADR-002's exception for temporal
fields.

### Wire Format

#### v5 Wire Format

NetFlow v5 datagrams follow the [Cisco NetFlow v5 format](https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html):

**Header (24 bytes, big-endian):**

| Field | Size | Description |
|-------|------|-------------|
| `version` | 2 bytes | Always `5` |
| `count` | 2 bytes | Number of flow records (1-30) |
| `sys_uptime` | 4 bytes | Milliseconds since router boot |
| `unix_secs` | 4 bytes | Seconds since epoch |
| `unix_nsecs` | 4 bytes | Nanoseconds remainder |
| `flow_sequence` | 4 bytes | Sequence counter |
| `engine_type` | 1 byte | Flow switching engine type |
| `engine_id` | 1 byte | Flow switching engine ID |
| `sampling_interval` | 2 bytes | Sampling mode and interval |

**Flow Record (48 bytes, big-endian):**

| Field | Size | Description |
|-------|------|-------------|
| `srcaddr` | 4 bytes | Source IP address |
| `dstaddr` | 4 bytes | Destination IP address |
| `nexthop` | 4 bytes | Next-hop router IP |
| `input` | 2 bytes | SNMP input interface index |
| `output` | 2 bytes | SNMP output interface index |
| `dPkts` | 4 bytes | Packet count |
| `dOctets` | 4 bytes | Byte count |
| `first` | 4 bytes | SysUptime at flow start |
| `last` | 4 bytes | SysUptime at flow end |
| `srcport` | 2 bytes | Source port |
| `dstport` | 2 bytes | Destination port |
| `pad1` | 1 byte | Unused (zero) |
| `tcp_flags` | 1 byte | Cumulative TCP flags |
| `prot` | 1 byte | IP protocol (6=TCP, 17=UDP) |
| `tos` | 1 byte | Type of service |
| `src_as` | 2 bytes | Source AS number |
| `dst_as` | 2 bytes | Destination AS number |
| `src_mask` | 1 byte | Source subnet mask bits |
| `dst_mask` | 1 byte | Destination subnet mask bits |
| `pad2` | 2 bytes | Unused (zero) |

Maximum datagram size: 24 + (30 * 48) = 1464 bytes. Well within the UDP
maximum and typical MTU, so no fragmentation concerns.

#### v9 Wire Format

NetFlow v9 ([RFC 3954](https://datatracker.ietf.org/doc/html/rfc3954)) uses a
template-based approach. Instead of a fixed record format, the exporter sends
Template FlowSets that describe which fields appear in subsequent Data FlowSets.

**Header (20 bytes, big-endian):**

| Field | Size | Description |
|-------|------|-------------|
| `version` | 2 bytes | Always `9` |
| `count` | 2 bytes | Number of FlowSets in packet |
| `sys_uptime` | 4 bytes | Milliseconds since device boot |
| `unix_secs` | 4 bytes | Seconds since epoch |
| `sequence_number` | 4 bytes | Sequence counter |
| `source_id` | 4 bytes | Observation domain ID |

Templates define the field types and sizes for data records. A single template
covers all 18 fields in the `Flow` struct, producing 45-byte data records (vs.
48 bytes for v5's fixed format with padding). Data FlowSets are padded to
4-byte boundaries.

The template mechanism is the key architectural difference from v5: the same
flow data is encoded via a self-describing format rather than a fixed layout.

Templates are transmitted **in-band** — in the same UDP datagram as Data
FlowSets. A single packet can contain both a Template FlowSet (ID=0) and one or
more Data FlowSets. Because UDP is unreliable, templates must be retransmitted
periodically; if a collector misses a template packet, subsequent data is
undecodable until the next template arrives. The v9 serializer handles this via
a configurable `template_interval` (default: 1, meaning every packet includes
the template). The exporter's spin loop sends the datagrams produced by the v9
serializer, which includes template and data FlowSets together.

### Protocol-Agnostic Design

The actor simulation produces protocol-agnostic `Flow` structs. The `Flow`
struct captures the semantic content of a network flow — source/destination
addresses, ports, protocols, byte counts, packet counts, flags — without
encoding any wire-format details.

Multiple serializers consume the same `Flow` data:

- `netflow::v5` — fixed 48-byte records, 24-byte header, max 30 records/packet
- `netflow::v9` — template-based variable records, 20-byte header, template
  FlowSets define record format

This proves the actor model is truly decoupled from serialization. Adding a new
wire format (IPFIX, sFlow) requires only a new serializer module — no changes
to the actor model or `Flow` struct.

Each exporter selects its wire format via `protocol_version: v5` or
`protocol_version: v9`.

### Integration with Existing Infrastructure

NetFlow is registered as a generator variant in `lading/src/generator/mod.rs`,
alongside the existing UDP, TCP, HTTP, and other generators. It is configured
at the top level of the YAML config, not nested under a transport generator's
`variant` field:

```yaml
generator:
  - netflow:
      collector_addr: "127.0.0.1:9995"
      exporters:
        - addr: "127.0.0.1"          # exporter bind address (Agent sees as ExporterAddr)
          protocol_version: v9
          source_id: 1
          flows_per_second: 10000
          actors:
            actor_count:
              inclusive:
                min: 10
                max: 100
            role_weights:
              desktop: 50
              web_server: 20
              dns_server: 5
              database_server: 10
              file_server: 15
            subnets:
              - cidr: "10.0.1.0"
                mask: 24
                weight: 60
              - cidr: "10.0.2.0"
                mask: 24
                weight: 40
```

Multiple exporters simulate multiple network devices reporting to the same
collector:

```yaml
generator:
  - netflow:
      collector_addr: "127.0.0.1:9995"
      exporters:
        - addr: "127.0.0.1"          # edge router
          protocol_version: v9
          source_id: 1
          flows_per_second: 8000
          actors:
            actor_count:
              inclusive: { min: 50, max: 200 }
            role_weights:
              desktop: 50
              web_server: 20
            subnets:
              - cidr: "10.0.1.0"
                mask: 24
                weight: 100

        - addr: "127.0.0.2"          # IoT gateway
          protocol_version: v5
          source_id: 2
          flows_per_second: 3000
          actors:
            actor_count:
              inclusive: { min: 10, max: 30 }
            role_weights:
              iot_sensor: 80
              iot_gateway: 20
            subnets:
              - cidr: "10.0.2.0"
                mask: 24
                weight: 100
```

The existing UDP generator and all other generators are unchanged. NetFlow does
not appear in `lading_payload::Config`, `lading_payload::Payload`, or
`block::Cache::fixed_with_max_overhead`. The block cache has no NetFlow match
arm.

**Open question: where does the library code live?** The actor model
(`Simulator`, `Actor`, `Role`, `Behavior`), the protocol-agnostic `Flow`
struct, and the wire-format serializers (`v5::Packet`, `v9::write_header`, etc.)
are reusable library code that could serve tests, tools (e.g., `payloadtool`),
or future generators. Two options:

1. **Keep in `lading_payload`**: The simulation and serialization code stays in
   `lading_payload/src/netflow/` as library code. The generator in
   `lading/src/generator/netflow.rs` imports and orchestrates it. This reuses
   the existing crate structure but means `lading_payload` contains code that
   the `lading_payload::Serialize` trait does not consume.
2. **Move to generator**: All NetFlow code lives in
   `lading/src/generator/netflow/`, self-contained like `ProcessTree` and
   `FileTree`. Simpler dependency graph, but the simulation and serialization
   code is not independently reusable.

## Consequences

### Positive

- **Realistic traffic patterns**: Generated flows represent plausible network
  activity with internally consistent fields
- **Exercises analytical pipeline**: The target's aggregation, correlation, and
  topology-building code sees data resembling production traffic
- **Temporal coherence**: Live timestamps and monotonically increasing sequence
  numbers mean collectors see temporally valid data — no false loss detection,
  no broken aggregation windows, no timestamp looping
- **Reusable pattern**: The actor-model approach can be applied to future
  protocol generators that need semantic coherence (OpenTelemetry traces,
  DNS queries, SNMP traps). The dedicated-generator precedent (alongside
  `ProcessTree`, `FileTree`, `Container`, `Kubernetes`) gives lading a clear
  path for protocols whose state makes the block cache inapplicable.
- **Correlations without complexity**: Behavioral sequences naturally produce
  correlated data without explicit correlation logic
- **Configurable realism**: Actor counts, role distributions, and behavior
  weights are all configurable, allowing tests to emphasize specific traffic
  patterns
- **Protocol-agnostic actor model**: The same simulation drives both v5
  (fixed-format) and v9 (template-based) serializers with zero changes to
  the actor or Flow layers, validating the architectural separation

### Negative

- **More complex than random generation**: The actor simulation adds conceptual
  and implementation complexity compared to filling fields randomly
- **Configuration surface area**: 23 roles, 17 behavior types, per-behavior
  byte/packet ranges, subnet lists, AS ranges — the configuration space is
  deliberately large. To mitigate this, the `actors` field accepts the string
  `random`, which generates the entire actor configuration (role weights,
  subnet layout, AS numbers, actor count, behavior ranges) from lading's
  existing seeded RNG. The actor model still runs — the configuration itself
  becomes the generated artifact. Explicit configuration remains available for
  users who need to control the topology:
  ```yaml
  # Minimal: generate everything randomly
  generator:
    - netflow:
        collector_addr: "127.0.0.1:9995"
        exporters:
          - actors: random

  # Explicit: full control
  generator:
    - netflow:
        collector_addr: "127.0.0.1:9995"
        exporters:
          - addr: "127.0.0.1"
            protocol_version: v5
            source_id: 1
            flows_per_second: 10000
            actors:
              actor_count:
                inclusive:
                  min: 50
                  max: 200
              role_weights:
                desktop: 60
                web_server: 20
              subnets:
                - cidr: "10.0.1.0"
                  mask: 24
                  weight: 70
                - cidr: "10.0.2.0"
                  mask: 24
                  weight: 30
              autonomous_system:  # BGP autonomous system numbers per subnet
                inclusive:
                  min: 64512
                  max: 65534
              interfaces:  # SNMP ifIndex values on the simulated router
                inclusive:
                  min: 1
                  max: 48
              behaviors_per_tick:  # how many behaviors each actor fires per tick
                inclusive:
                  min: 1
                  max: 5
  ```
- **Semantic assumptions**: The behavior definitions encode assumptions about
  "realistic" traffic that may not match all deployment environments
- **Runtime generation cost**: Unlike block-cache generators, the dedicated
  generator runs `tick()` and serialization on every send. The simulation is
  lightweight (RNG sampling, struct fills) and the bottleneck is the `send_to`
  syscall, but this is a departure from [ADR-005](005-performance-first-design.md)'s
  preference for zero runtime computation. If profiling shows the simulation
  becomes a bottleneck at extreme send rates, a producer/consumer buffer
  (dedicated thread running the simulation ahead of the sender) can decouple
  generation from sending.

### Neutral

- The actor model is a new pattern in lading. The closest precedent is the
  container state in `lading_payload/src/splunk_hec.rs`, where generated data
  carries stateful identity (container types). The actor model extends this
  concept from simple identity to full behavioral simulation.
- NetFlow joins `ProcessTree`, `FileTree`, `Container`, and `Kubernetes` as a
  dedicated generator that bypasses the block cache. All existing payload
  types continue to use the block cache unchanged. No new abstractions are
  introduced — the dedicated generator is an established pattern in lading.
- The behavioral simulation is a pure function of configuration and seed for
  flow content (IPs, ports, protocols, byte counts). Temporal fields
  (timestamps, sequence numbers) are derived from wall-clock time and send
  order, consistent with [ADR-002](002-precomputation-philosophy.md)'s
  exception for fields that must be current.

## References

- [ADR-001](001-generator-target-blackhole-architecture.md): Generator-Target-Blackhole Architecture — NetFlow follows the generator pattern
- [ADR-002](002-precomputation-philosophy.md): Pre-computation Philosophy — NetFlow is an acknowledged exception; temporal state dominates packet content
- [ADR-003](003-determinism-requirements.md): Determinism Requirements — all actor RNG is seeded for reproducibility
- [ADR-005](005-performance-first-design.md): Performance-First Design — simulation is lightweight (RNG + struct fills), bottleneck is the `send_to` syscall
- `lading_payload/src/netflow.rs` — Protocol-agnostic `Flow` struct and `Config` enum (`V5`/`V9`)
- `lading/src/generator/process_tree.rs` — Precedent for dedicated generators that bypass the block cache
- `lading_payload/src/splunk_hec.rs` — Container state pattern, closest existing precedent for stateful payload generation
- [Cisco NetFlow v5 format](https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html) — Wire format specification
- [RFC 3954](https://datatracker.ietf.org/doc/html/rfc3954) — NetFlow v9 specification
