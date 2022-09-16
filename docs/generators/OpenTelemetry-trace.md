# OpenTelemetry-trace Payload

Rough edges: generated timestamps will uniformly cover the entire unix epoch. This will cause skewed performance for consumers that drop old/future traces before processing them.