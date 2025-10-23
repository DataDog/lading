# Datadog Blackhole

This blackhole supports multiple Datadog intake protocols:

## Variants

### V2 Metrics (HTTP)

HTTP-based metrics intake that accepts protobuf-encoded metric payloads compatible with the Datadog Agent V2 API.

**Configuration:**
```yaml
blackhole:
  - datadog:
      v2:
        binding_addr: "127.0.0.1:9091"
      id: "v2-metrics-blackhole"  # optional
```

**Features:**
- Accepts POST requests to `/api/v2/series` with `Content-Type: application/x-protobuf`
- Supports gzip, zstd, and deflate compression via `Content-Encoding` header
- Records metrics with historical timestamps using lading's capture system
- Tracks COUNT, RATE, and GAUGE metric types
- Returns 202 Accepted for all other endpoints

### Stateful Logs (gRPC)

Bidirectional streaming gRPC service implementing the Datadog stateful logs protocol.

**Configuration:**
```yaml
blackhole:
  - datadog:
      stateful_logs:
        grpc_addr: "127.0.0.1:9092"
      id: "stateful-logs-blackhole"  # optional
```

**Features:**
- Implements `StatefulLogsService` with bidirectional streaming
- Accepts `StatefulBatch` messages containing:
  - Pattern definitions and updates for log templates
  - Dictionary entries for efficient string encoding
  - Structured and raw log messages
- Returns `BatchStatus` acknowledgments for each batch
- Tracks metrics: bytes_received, requests_received, batches_received, data_items_received

## Proto Files

- **V2 Metrics**: `proto/agent_payload.proto`
- **Stateful Logs**: `proto/stateful_encoding.proto`

## Metrics

Both variants emit the following metrics (tagged with component, component_name, protocol, and id):

- `bytes_received`: Total bytes received
- `requests_received`: Total requests/batches received
- `batches_received`: Total batches received (stateful logs only)
- `data_items_received`: Total data items in batches (stateful logs only)
- `datadog_intake_payloads_parsed`: Successfully parsed payloads (V2 only)
- `datadog_intake_parse_failures`: Failed parse attempts (V2 only)

## Architecture

```
datadog/
├── mod.rs       - Main module coordinating HTTP and gRPC servers
├── v2_http.rs   - HTTP V2 metrics implementation
├── grpc.rs      - gRPC stateful logs implementation
└── README.md    - This file
```

The module follows the same pattern as the OTLP blackhole, supporting multiple protocols through a unified configuration interface.



