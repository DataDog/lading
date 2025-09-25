# Datadog Trace Agent

This README explains where trace ingestion code lives in the Datadog Agent, how
to understand the protocol definitions, and how data flows through the
system. Analysis done as of 8cc5eb3e024ee54283efad4614175a065642bd9c. We'll
focus only on v0.4 with msgpack encoding for now.

## The Trace Agent Code

The trace agent code lives in the `pkg/trace` directory of the datadog-agent
repository:

```
pkg/trace/
├── api/           # HTTP API endpoints and request handling
├── agent/         # Main trace agent logic
├── config/        # Configuration management
├── processor/     # Trace processing pipeline
├── proto/         # Protocol buffer definitions (MOVED to pkg/proto)
├── sampler/       # Sampling logic
└── testutil/      # Test utilities for generating fake traces
```

Key files to understand:

- [`pkg/trace/api/api.go`][api_go] - Main request handling logic
- [`pkg/trace/api/endpoints.go`][endpoints_go] - Endpoint routing setup
- [`pkg/trace/api/version.go`][version_go] - Protocol version definitions
- [`pkg/proto/datadog/trace/span.proto`][span_proto] - Span structure definition

## V0.4 Specification

### Protocol Definition

The protocol is documented in [`pkg/trace/api/version.go`][version_go_v04]:

```go
// v04
//
// Request: Trace chunks.
// 	Content-Type: application/msgpack
// 	Payload: An array of arrays of Span (pkg/proto/datadog/trace/span.proto)
//
// Response: Service sampling rates.
// 	Content-Type: application/json
v04 Version = "v0.4"
```

- Uses msgpack encoding (we ignore JSON encoding which is also supported)
- Expects "array of arrays of Span" structure
- Returns JSON service sampling rates

#### Version-Specific Decoding

Version-specific decoding logic is in [`pkg/trace/api/api.go:482`][api_go_decode_default]:

```go
func decodeTracerPayload(v Version, req *http.Request, ...) (*pb.TracerPayload, error) {
    switch v {
    case v01:
        var spans []*pb.Span  // v0.1 expects flat array of spans
        // ... decode and wrap spans

    case v05:
        var traces pb.Traces  // v0.5 uses string dictionary format
        if err = traces.UnmarshalMsgDictionary(buf.Bytes()); err != nil {
            return nil, err
        }

    case V07:
        var tracerPayload pb.TracerPayload  // v0.7 sends full TracerPayload
        _, err = tracerPayload.UnmarshalMsg(buf.Bytes())

    default:  // v0.2, v0.3, v0.4 use this path
        var traces pb.Traces  // Array of arrays of Span for v0.4
        if err = decodeRequest(req, &traces); err != nil {
            return nil, err
        }
        // Agent wraps raw traces in TracerPayload with metadata
        return &pb.TracerPayload{
            LanguageName:    lang,
            LanguageVersion: langVersion,
            Chunks:          traceChunksFromTraces(traces),
        }, nil
    }
}
```

#### Content-Type Handling

Content-Type detection and decoding is in [`pkg/trace/api/api.go:888`][api_go_decode_request]:

```go
func decodeRequest(req *http.Request, dest *pb.Traces) error {
    switch mediaType := getMediaType(req); mediaType {
    case "application/msgpack":
        _, err := dest.UnmarshalMsg(buf.Bytes())  // MessagePack decode for v0.4
        return err
    case "application/json":
        return json.NewDecoder(req.Body).Decode(&dest)  // JSON fallback
    case "":
        return json.NewDecoder(req.Body).Decode(&dest)  // Default to JSON
    }
}
```

Of note trace-agent team states that MessagePack is heavily preferred over JSON,
we skip the JSON implementation then.

- `application/msgpack` Content-Type
- Calls `dest.UnmarshalMsg()` for msgpack decoding

### Data Structure Definitions

#### pb.Traces

Core type definitions are in [`pkg/proto/pbgo/trace/trace.go`][trace_go]:

```go
// Line 13: A trace is an array of span pointers
type Trace []*Span

// Line 16: Traces is an array of traces
type Traces []Trace
```

- [`Trace` definition][trace_go_trace] - Single trace as array of span pointers
- [`Traces` definition][trace_go_traces] - Array of traces

So `pb.Traces` = `[][]Span` = Array of traces, each trace is array of spans.

#### Span

The span structure is defined in [`pkg/proto/datadog/trace/span.proto:101-147`][span_proto_span]:

```protobuf
message Span {
    string service = 1;      // Service name
    string name = 2;         // Operation name
    string resource = 3;     // Resource identifier
    uint64 traceID = 4;      // Trace identifier
    uint64 spanID = 5;       // Span identifier
    uint64 parentID = 6;     // Parent span ID (0 for root)
    int64 start = 7;         // Start time (nanoseconds since epoch)
    int64 duration = 8;      // Duration in nanoseconds
    int32 error = 9;         // Error flag (1 = error, 0 = success)
    map<string, string> meta = 10;     // String tags
    map<string, double> metrics = 11;  // Numeric metrics
    string type = 12;                  // Span type (web, db, cache, etc)
    map<string, bytes> meta_struct = 13;  // Structured data (AppSec, etc)
    // Fields 14-15 are newer additions (SpanLinks, SpanEvents)
}
```

## References

[api_go]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/trace/api/api.go
[api_go_decode_default]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/trace/api/api.go#L522
[api_go_decode_request]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/trace/api/api.go#L888
[endpoints_go]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/trace/api/endpoints.go
[version_go]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/trace/api/version.go
[version_go_v04]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/trace/api/version.go#L22-L57
[span_proto]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/proto/datadog/trace/span.proto
[span_proto_span]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/proto/datadog/trace/span.proto#L101-L147
[trace_go]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/proto/pbgo/trace/trace.go
[trace_go_trace]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/proto/pbgo/trace/trace.go#L13
[trace_go_traces]: https://github.com/DataDog/datadog-agent/blob/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/proto/pbgo/trace/trace.go#L16
