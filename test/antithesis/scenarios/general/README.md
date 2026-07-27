# General scenario

This is the MVP scenario. lading generates TCP load into an instrumented sink,
and the run establishes two things across faults: that load actually arrives, and
that lading's capture files stay crash-consistent when lading is hard-killed.

## How it works

This scenario comprises the following components:

* sink (oracle)
* lading, instrumented (system under test)
* workload (driver)

The sink is a TCP server that counts received bytes and owns the "load arrived"
assertion. It is healthchecked and never faulted. lading is the system under
test, faulted at the node level. The workload writes the timeline's config and
validates lading's capture.

The workload's `first_sample_config` command samples a lading config per timeline
from Antithesis randomness (payload variant, bytes per second, parallel
connections, and seed) and writes it, plus a `ready` sentinel, to the shared
volume. lading's entrypoint blocks on the sentinel, then boots under the sampled
config and pushes TCP load at the sink. lading writes its capture to a named
`capture` volume that outlives a `node_termination` of the lading container.

Antithesis injects node faults on lading: a `node_termination` is a SIGKILL of
the whole container, so only artifacts on the named volume survive. The
workload's `anytime_capture_consistent` checker validates every capture file by
its format:

* JSONL: a valid parseable prefix. A torn final line, the interrupted write, is
  tolerated. A broken earlier line or a violated invariant is not.
* Parquet: footer-terminated, so a hard kill leaves it unreadable. That is
  expected, not corruption. A readable Parquet must be internally consistent.
* Multi: both of the above, on the `.jsonl` and `.parquet` files.

The checker asserts no mid-file corruption and monotonic `fetch_index` and
per-series time. The sink asserts `sometimes(total_bytes > 0)`.

# Caveats

1. JSONL survives a kill as a valid prefix because of the 60-second in-memory
   maturity window. Matured intervals are on disk, the unmatured tail is lost on
   abrupt death by design. This scenario checks consistency, not completeness.
2. Parquet and Multi are footer-terminated. An abrupt kill leaves the Parquet
   file unreadable, which the checker treats as expected rather than a violation.
3. There is deliberately no blackhole. The sink is the only receiver, so a config
   that expected a blackhole would not apply here.

# Assumptions

1. The `capture` named volume outlives a `node_termination` of lading.
2. The sink is never faulted and is a healthchecked dependency of lading.
3. Config sampling draws from Antithesis randomness so timelines branch across
   the payload-variant menu.
