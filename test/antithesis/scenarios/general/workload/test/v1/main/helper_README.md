# Test template: main

Antithesis discovers test commands here at `/opt/antithesis/test/v1/main/` inside
the workload container. Compiled command binaries are injected by the Dockerfile
from the shared `harness` crate; they are not checked in.

Current commands:

- `first_sample_config` — samples this timeline's `lading.yaml` into the shared
  volume and releases the sentinel the lading container waits on.

`helper_`-prefixed files (like this one) are ignored by Antithesis.
