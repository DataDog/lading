# NetFlow v5 Support for Lading

This implementation adds NetFlow v5 packet generation support to the lading load testing framework.

## Overview

NetFlow v5 is a network protocol developed by Cisco for collecting IP traffic information. This implementation generates realistic NetFlow v5 export packets that can be used to load test NetFlow collectors.

## Features

- **Standards Compliant**: Generates proper NetFlow v5 packets following Cisco's specification
- **Configurable Data Ranges**: All packet fields can be configured with ranges for realistic data generation
- **Protocol Distribution**: Weighted distribution of TCP, UDP, ICMP, and other protocols
- **Fixed Random Seed**: Uses deterministic random generation for reproducible test data
- **UDP Transport**: Sends packets over UDP (standard for NetFlow exports)
- **MTU Aware**: Respects packet size limits to stay within network MTU

## Packet Structure

Each NetFlow v5 packet contains:
- **Header** (24 bytes): Version, flow count, timestamps, sequence numbers, engine info
- **Flow Records** (48 bytes each): Source/destination IPs, ports, byte/packet counts, timing, protocols, AS numbers

## Configuration

The NetFlow v5 payload type supports extensive configuration:

### Basic Example
```yaml
generator:
  udp:
    addr: "127.0.0.1:9995"
    variant:
      net_flow_v5: {}  # Uses default configuration
```

### Full Configuration Example
```yaml
generator:
  udp:
    variant:
      net_flow_v5:
        flows_per_packet:
          min: 1
          max: 30
        src_ip_range:
          min: 167772161   # 10.0.0.1
          max: 184549374   # 10.255.255.254
        dst_ip_range:
          min: 3232235521  # 192.168.1.1
          max: 3232301054  # 192.168.255.254
        protocol_weights:
          tcp: 70
          udp: 25
          icmp: 3
          other: 2
```

## Configuration Options

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `flows_per_packet` | Range<u16> | Number of flow records per packet | 1-30 |
| `src_ip_range` | Range<u32> | Source IP addresses as u32 | 10.0.0.0/8 |
| `dst_ip_range` | Range<u32> | Destination IP addresses as u32 | 192.168.0.0/16 |
| `src_port_range` | Range<u16> | Source port numbers | 1024-65535 |
| `dst_port_range` | Range<u16> | Destination port numbers | 1-65535 |
| `packet_count_range` | Range<u32> | Packets per flow | 1-10000 |
| `byte_count_range` | Range<u32> | Bytes per flow | 64-1500000 |
| `flow_duration_range` | Range<u32> | Flow duration in milliseconds | 1s-1h |
| `interface_range` | Range<u16> | Interface indices | 1-254 |
| `as_number_range` | Range<u16> | BGP AS numbers | 1-65535 |
| `protocol_weights` | ProtocolWeights | Protocol distribution weights | TCP:70, UDP:25, ICMP:3, Other:2 |
| `engine_type` | u8 | Flow switching engine type | 0 |
| `engine_id` | u8 | Flow switching engine ID | 0 |
| `aggregation` | AggregationConfig | Flow aggregation settings | See below |

## Flow Aggregation Configuration

The NetFlow v5 generator supports flow aggregation features to simulate realistic network traffic patterns:

### Flow Aggregation Ratio

The `flow_aggregation_ratio` setting generates additional flows with identical 5-tuples (source IP, destination IP, source port, destination port, protocol) to simulate traffic aggregation scenarios commonly seen in real networks.

- **Ratio of 1.0**: No aggregation (default)
- **Ratio of 1.6**: For every base flow, generate 0.6 additional identical flows (60% more flows)
- **Ratio of 2.0**: For every base flow, generate 1 additional identical flow (double the flows)

### Port Rollup Ratio

The `port_rollup_ratio` setting generates additional flows with the same source IP, destination IP, and destination port, but with different ephemeral source ports. This simulates scenarios where multiple client connections from the same source reach the same destination service.

- **Ratio of 1.0**: No port rollup (default)
- **Ratio of 5.0**: For every base flow, generate 4 additional flows with different source ports
- **Only applies to TCP and UDP protocols** (ICMP and other protocols do not use ports)

### Aggregation Configuration Options

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `flow_aggregation_ratio` | f32 | Additional flows with identical 5-tuples | 1.0 |
| `port_rollup_ratio` | f32 | Additional flows with different source ports | 1.0 |
| `time_variance_ms` | u32 | Time variance for aggregated flows (ms) | 1000 |
| `vary_counts` | bool | Apply variations to packet/byte counts | true |
| `count_variation_percent` | f32 | Percentage variation for counts (0.0-1.0) | 0.1 |

### Aggregation Example

```yaml
generator:
  udp:
    variant:
      net_flow_v5:
        flows_per_packet:
          min: 5
          max: 20
        aggregation:
          flow_aggregation_ratio: 1.6    # 60% more identical flows
          port_rollup_ratio: 5.0         # 4 additional flows with different src ports
          time_variance_ms: 2000         # 2 second time variance
          vary_counts: true              # Apply count variations
          count_variation_percent: 0.15  # 15% variation
```

## IP Address Configuration

IP addresses are configured as u32 values in network byte order. Helper conversions:

```bash
# Convert IP to u32
python3 -c "import socket, struct; print(struct.unpack('!I', socket.inet_aton('10.0.0.1'))[0])"

# Convert u32 to IP
python3 -c "import socket, struct; print(socket.inet_ntoa(struct.pack('!I', 167772161)))"
```

## Load Testing Scenarios

### Single Router Simulation
```yaml
generator:
  udp:
    addr: "127.0.0.1:9995"
    variant:
      net_flow_v5:
        engine_id: 1
    bytes_per_second: "10MB"
```

### Multiple Router Simulation
Deploy multiple generator instances with different engine_id values to simulate multiple routers.

### Burst Testing
```yaml
generator:
  udp:
    variant:
      net_flow_v5:
        flows_per_packet:
          min: 25
          max: 30  # Maximum flows per packet
    bytes_per_second: "50MB"  # High rate for burst testing
```

## Usage

1. **Start a NetFlow collector** (or use netcat for testing):
   ```bash
   nc -ul 9995  # Listen on UDP port 9995
   ```

2. **Run lading with NetFlow configuration**:
   ```bash
   cargo run -- --config netflow_v5_example.yaml
   ```

3. **Monitor metrics** at http://localhost:9090 (if Prometheus observer is configured)

## Metrics

The generator emits standard UDP generator metrics:
- `bytes_written`: Total bytes sent
- `packets_sent`: Total packets sent
- `request_failure`: Failed send attempts
- `connection_failure`: Socket bind failures
- `bytes_per_second`: Configured transmission rate

## Implementation Details

- Uses deterministic random generation with configurable seed
- Generates realistic flow timing relationships
- Handles protocol-specific fields (TCP flags, port usage)
- Maintains sequence numbers across packets
- Respects MTU limits (max 30 flows per packet = 1464 bytes)
- Network byte order encoding for all multi-byte fields

## Testing

Run NetFlow-specific tests:
```bash
cd lading_payload && cargo test netflow
```

## Future Enhancements

Potential areas for expansion:
- NetFlow v9 template-based format
- IPFIX support
- IPv6 flow records
- Flow sampling configuration
- Custom field templates