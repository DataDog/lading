# Test template `main`

Antithesis runs the commands in this directory once the workload emits
`setup_complete`. Files prefixed with `helper_`, like this one, are ignored.

The command binaries are compiled from the `workload-client` crate and injected
here by `antithesis/Dockerfile`. They are not checked in. Current commands:

- `serial_driver_divide_throttle`: drives the `divide-preserves-aggregate-rate`
  property, catalog Category A. Links `lading_throttle` and asserts the real
  `Throttle::divide` preserves aggregate capacity up to the integer remainder,
  across a divisor × capacity value menu.
- `serial_driver_baseline_clean_run`: drives the `rig-runs-lading-cleanly`
  baseline, catalog Category H. In a fault-quiet window, spawns a real `lading`
  run against `lading-probe` and asserts it exits 0, writes a non-empty capture,
  and puts bytes on the wire, as read from the probe's report port.
