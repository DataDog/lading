# v1.0 trace payloads

The top-level `trace_agent` generator selects this encoder with
`variant: {v1.0: ...}` and POSTs MessagePack to `/v1.0/traces`.
This is the idx/ETP format: integer-keyed payload, chunk, and span maps,
with strings written inline once and referenced by index thereafter.
The string table is scoped to one payload and starts with the empty string
at index zero. Attributes are flat arrays of key/type/value triples.

## Workload and batching

- `chunks_per_payload` is an exact positive count. One HTTP body contains
  one payload with that many trace chunks. Increasing the block byte budget
  does not add chunks.
- `maximum_block_size` limits encoded bytes. A sampled block budget too
  small for the complete batch rejects it without emitting a partial trace.
  Cache construction fails after 1024 consecutive rejected blocks; increase
  the byte limit or reduce the batch size or service graph.
- Every trace starts at a uniformly selected operation of the first service.
  Calls are independently gated by their configured rates.
- Traversal stops at depth 10 (root depth zero) or 100 spans per chunk.
  Cyclic and broad graphs can therefore be truncated.
- Span IDs are nonzero, child timing is nested inside parent timing, and
  timestamps fit within signed 64-bit bounds. Names and resources must be
  nonempty. Other strings are emitted verbatim; agent normalization can
  still change services, operation names, environments, and attributes.
- Error injection sets the error bit and supplies default `error.type` and
  `error.message` only when those attributes are absent.
- Attributes support static strings, booleans, signed integers, and doubles.

This graph is independent of the OTLP generator. It uses explicit span
names/resources/types, first-service roots, and scalar attributes. It has no
OTLP dictionary pools, per-call attributes, `max_repeat`, or explicit root
selection. Port service topology and call rates semantically; do not assume
identical attribute cardinality or a shared configuration schema.

Lading throttles bytes, so equal byte rates across v0.4 and v1.0 do not imply
equal span rates. Exact batching controls chunks per request, not spans per
second. Use decoded/forwarded span counts when comparing protocol costs.

## Review map and validation

- `config.rs`: public configuration and validation.
- `../v1.rs`: resolved service graph, generation, and serialization sizing.
- `encoder.rs`: streaming string table and MessagePack schema.
- `test.rs`: generation and serialization invariants.
- `golden.rs`: test-only schema reader and assertions over tracer captures.

Golden tests re-encode every chunk of both captures, including errors,
cross-chunk string references, links, and events. Generated-payload tests
decode actual serializer output and check exact batching, span counts,
error attributes, enabled links/events, and determinism.

The v1 fingerprint intentionally changes with exact batching: the fixture
now emits one chunk per payload instead of filling each block with chunks.
Each body repeats its metadata and starts a new string table; geometric
size probes also no longer consume RNG draws. Its measured corpus entropy
changes from 7.0414 to 6.6439 bits/byte. This records the requested batching
change, not an attempt to tune entropy by changing the workload.

The test reader is retained because semantic equality permits different
attribute order and MessagePack integer widths. Byte equality alone would
reject equivalent encodings. It does not replace a live-agent ingestion
check or execution against the production decoder.

## Fixtures and references

`testdata/golden_captures.json` contains raw request bodies recorded from
the `apm-v1-trace-smoke` application using dd-trace-go v2.11.0-dev.1.
The discovery shim advertised `/v1.0/traces` and native span events and
forwarded requests to a recording sink. The captures include nested
services and errors; the second also includes links and all four scalar
event attribute types.

Wire mappings were reviewed against these reference snapshots:

- [Agent idx encoder/decoder](https://github.com/DataDog/datadog-agent/tree/6e10d6cf0e1ef764bfe4390a68c55cee415d264c/pkg/proto/pbgo/trace/idx)
- [Saluki decoder](https://github.com/DataDog/saluki/tree/0af0bdff879c6809985b4e5ddddff31667632c61/lib/saluki-components/src/decoders/datadog)
- [dd-trace-go encoder](https://github.com/DataDog/dd-trace-go/blob/bf08f5f9aafed5797e3666fbdd2325caa57e3704/ddtrace/tracer/payload_v1.go)
