---
slug: splunk-hec-response-parse-no-panic
subsystem: no-panic
assertion_type: Unreachable
priority: P1
needs_target: true
needs_sut_fix: replace serde_json::from_slice::<HecResponse>().expect() with error handling; track the detached task's handle
---

# Splunk-HEC response parsing never panics on a non-HecResponse body

## Statement

The Splunk-HEC generator's spawned request task handles a non-HecResponse body (empty, HTML
error page, "ok") without panicking via `.expect` on serde_json parse.

## Code evidence trail

- `splunk_hec.rs:371-374` — `serde_json::from_slice::<HecResponse>(&body_bytes).expect("unable
  to parse response body")`. Any target/blackhole returning a non-HecResponse body panics the
  detached task.

## Assertion-type rationale

**Unreachable**: the parse `.expect` must never be reached with a non-conforming body. Oracle:
blackhole returning a plain-text body + panic hook.

## Mode / observability

A blackhole/target returning a non-JSON body does not abort the detached task. `panic=abort`
makes this whole-process fatal.

## Mechanism

Replace `serde_json::from_slice::<HecResponse>(...).expect(...)` at `splunk_hec.rs:371-374` with
error handling; also track the detached task's handle.

## needs_sut_fix

Yes.

## Investigation Log

- [2026-07-24] Confirmed: the response body is parsed with `.expect`; a non-HecResponse body
  panics the detached task, and `panic=abort` makes it whole-process fatal.
- [2026-07-24] The detached task is also untracked — a known related defect (splunk_hec
  detaches untracked tasks).

## Open Questions

- None specific.
