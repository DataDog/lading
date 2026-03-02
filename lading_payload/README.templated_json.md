# Templated JSON payload

Generates JSON records from a standalone YAML template file. The template is
read and parsed once at startup; the hot path only fills in generated values.

## Using it in a lading config

Reference the variant by name with a `template_path` pointing at the template
file. The path can be absolute or relative to the working directory of the
lading process.

**HTTP generator** (variant is nested under `method.post`):

```yaml
generator:
  - http:
      seed: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
      headers: {}
      target_uri: "http://127.0.0.1:8080/"
      bytes_per_second: "1 MiB"
      parallel_connections: 1
      method:
        post:
          maximum_prebuild_cache_size_bytes: "10 MiB"
          variant:
            templated_json:
              template_path: "/path/to/my-template.yaml"
```

**TCP / UDP / Unix stream / Unix datagram / passthru_file / gRPC / file_gen**
(variant is a top-level field on the generator config):

```yaml
generator:
  - tcp:
      seed: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
      addr: "127.0.0.1:8080"
      bytes_per_second: "1 MiB"
      maximum_prebuild_cache_size_bytes: "100 MiB"
      variant:
        templated_json:
          template_path: "/path/to/my-template.yaml"
```

The same template file can be referenced from multiple generators or multiple
lading config files without duplication.

## Template file format

The YAML file has two top-level keys:

- `definitions`: reusable named generators.
- `generator`: the root generator used for each emitted log line.

Each generated line is JSON text (typically a JSON object, depending on your root `generator`).

### Supported generator tags

The config DSL in `opw_tools::json_generator` supports these YAML tags:

- `!const`: fixed value
- `!choose`: random pick from a list (uniform)
- `!reference`: evaluate a named generator from `definitions`
- `!weighted`: weighted random pick
- `!range`: random integer in `[min, max]` (inclusive)
- `!format`: template string replacement using `{}` placeholders
- `!object`: build a JSON object from generated fields
- `!with`: bind generated values to variables, then evaluate `in`
- `!var`: read a bound variable
- `!timestamp`: deterministic monotonically-advancing UTC timestamp (whole-second RFC-3339)

## Example template

A realistic application log template demonstrating common patterns:

```yaml
definitions:
  # Reusable generators referenced by !reference below
  severity:
    !weighted
      - weight: 70
        value: !const "INFO"
      - weight: 20
        value: !const "WARN"
      - weight: 10
        value: !const "ERROR"

  service_name:
    !choose ["auth-service", "api-gateway", "billing-service", "inventory-service"]

  request_id:
    !format
      template: "{}-{}-{}"
      args:
        - !range { min: 1000, max: 9999 }
        - !range { min: 100000, max: 999999 }
        - !range { min: 1000, max: 9999 }

generator:
  !with
    bind:
      svc: !reference service_name
      sev: !reference severity
    in: !object
      timestamp: !timestamp
      level: !var sev
      service: !var svc
      request_id: !reference request_id
      duration_ms: !range { min: 1, max: 2000 }
      message:
        !format
          template: "{} handled request in {}ms"
          args:
            - !var svc
            - !range { min: 1, max: 2000 }
```

This produces newline-delimited JSON like:

```json
{"timestamp":"2024-03-01T12:00:00Z","level":"INFO","service":"api-gateway","request_id":"4821-583920-7341","duration_ms":142,"message":"api-gateway handled request in 89ms"}
```

## Authoring tips

- Use `definitions` to centralize reusable generators and keep templates DRY.
- Use `!with` + `!var` to bind values once and reuse them across multiple fields in one event (the example above binds `svc` and `sev` once, then references them in both the `level`/`service` fields and the `message`).
- Use `!weighted` when you need realistic event distribution (for example, mostly INFO with fewer ERROR logs).
- Keep the top-level `generator` producing one complete event per call so output lines stay self-contained.
- `!timestamp` draws a random starting second from the RNG, then advances by a random 1--1000 ms increment on every call. Only whole seconds appear in the output. Because all randomness comes from the configured seed, output is fully reproducible.

## Missing Features

For future work:

- Strings of bounded random length range pulled from configurable text generators (hexadecimal,
  chosen words, etc).
