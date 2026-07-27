# Test template: main

Antithesis discovers test commands here at `/opt/antithesis/test/v1/main/` inside
the workload container. Compiled command binaries are injected by the Dockerfile
from the shared `harness` crate; they are not checked in.

Current commands:

- `anytime_lading_drained_bounded` -- reads lading's recorded exit code from the
  shared volume and asserts lading drains within the watchdog bound (rc == 0) on
  graceful shutdown while a generator is stuck against an unreachable
  destination. rc == 124 (watchdog kill) is the
  finding: lading did not drain promptly.

`helper_`-prefixed files (like this one) are ignored by Antithesis.
