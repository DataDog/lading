# Differential Testing — Open Questions

> **Status:** parked. Before this can move forward, two prerequisites need
> to be addressed first (see below).

## Motivation

Existing `lading_analysis` checks make single-instance correctness claims:
completeness, fabrication, duplication, latency, truncation, multiline.
Differential testing would let us compare **two captures against each
other** — e.g. upgrade validation (agent v1 vs v2), config perturbation
(same agent, different knobs), or golden-file regression — at a higher
level than any single-instance invariant can catch.

Our pipeline is a natural fit: inputs are deterministic given seed +
config, captures are files, and the same two captures can be diffed
repeatedly under different equivalence rules without re-running
experiments.

## Prerequisites before designing this seriously

1. **Improve the uniqueness guarantees of the generators.** Today's
   content-hash matching leans on block-cache line uniqueness, which is
   already a known limitation (see
   `scratch/captures/README.md` → "Line uniqueness and cross-file
   overlap"). For single-instance checks this is mostly tolerable; for
   differential comparisons it becomes structural because we need stable,
   repeatable identifiers to align outputs across runs. Candidates: per-line
   unique IDs in the payload headers (already listed in Future Work),
   stronger content-level identity than the hash.

2. **Decide where differential testing lives in the system.** Does it
   belong inside `lading_analysis` as a new subcommand / check type, or
   does it want its own crate (`lading_diff`?) sitting alongside
   `lading_analysis` and consuming its output? The answer depends on how
   much shared machinery (input reconstruction, output parsing, matchers)
   it reuses vs. how much net-new it adds (alignment across runs, tolerance
   models, reporting).

Once those two are settled, the questions below become actionable.

## Framing

Differential testing ≈ two (or more) captures, one equivalence notion,
plus an alignment strategy across captures.

Most common shapes:
- **A/B across agent versions** — upgrade validation.
- **A/B across agent configs** — canary / perturbation testing.
- **Baseline-vs-current** — golden-file regression.

## Questions, grouped

### 1. What scenario is driving this

1. What's the concrete motivating case? Agent upgrade validation,
   algorithm rework (sampling, encoding), or something else?
2. Is the primary adversary **regressions in the agent** or **bugs in our
   own generator / analysis**? Those point at different comparisons.
3. Is this meant to run in CI as a gate, or as a dev-time investigation
   tool, or both?

### 2. What are you actually diffing

4. **Layer of comparison:** raw blackhole payload bytes (HTTP framing,
   compression, exact batches), decoded message content, parsed output
   fields (message text + tags + metadata), or higher-level aggregates
   (counts, rates, latency distributions)?
5. Does **reconstructed input** ever enter the diff, or is this purely an
   outputs-vs-outputs comparison? (I.e., is the input-side equivalence
   assumed given the same seed?)
6. Is it useful to diff **derived quantities** — e.g. "per-category
   completeness was 100% in run A but 99.8% in run B" — rather than raw
   artifacts?

### 3. Equivalence and tolerance

7. Do you expect any two runs with the same seed + config to produce
   **byte-identical** blackhole captures, or is there inherent
   non-determinism (batch boundaries, timestamp-dependent batching, tag
   ordering) that needs to be normalized out?
8. What kinds of variance are acceptable without flagging? Reordering of
   messages within a batch? Batch boundary differences? Timestamp jitter?
   Field ordering inside JSON?
9. Is there a distinction between **allowed variance** (normalize before
   diff) and **expected variance bounds** (diff, but allow small
   numbers)?

### 4. Alignment across runs

10. How do you pair message A_i from run A with message B_j from run B?
    Options: content hash (set equivalence), embedded ID (if payload
    variants include one), position in sequence, timestamp proximity.
    This is the question the **uniqueness prerequisite** above is
    blocking on.
11. What happens to messages present in one run but not the other?
    Asymmetric reporting? Treat as fatal vs informational?
12. If the inputs were identical, should the **reconstructed inputs** be
    identical across runs by construction, or do you want to verify that
    as part of the diff too?

### 5. Orchestration

13. Does `lading_analysis` **consume two existing capture sets** the user
    produces separately, or does it also **orchestrate** running lading
    twice with different agent configs?
14. Is "baseline-vs-current" a shape you want — i.e., capture files
    checked into a blob store / git-lfs and diffed against a fresh run?
    Or always two fresh runs?
15. Would it be useful to diff **captures from different experiment
    configurations** (e.g., same agent, different load profiles) and
    assert that some property holds across the perturbation?

### 6. Reporting

16. When a diff is found, what do you want to see — a `diff -u` style
    text dump, a structured JSON report, summary counts by category, or
    graphs / histograms for distributional differences?
17. Exit-code / pass-fail gate, or informational output for a human to
    judge?
18. For distributional diffs (latency histograms, rate curves), do you
    want a named statistical test (KS, chi-squared) or just visible
    side-by-side numbers?

### 7. How it fits with the rest

19. Should differential testing reuse the **matcher + constraint**
    factoring from `scratch/captures/user_defined_checks.md` (i.e., a
    matcher pairs inputs across runs, constraints apply to the pairs),
    or is this a separate mode with its own primitives?
20. Does the differential analysis live as a new subcommand of
    `lading-analysis`, a new crate, or a new check type? The answer
    depends on whether the input shape (two captures) is enough of a
    departure from single-capture analysis that a separate tool is
    cleaner. See prerequisite #2 above.
21. Is there interest in **multi-way** comparison (3+ runs) or just
    2-way? Multi-way is noticeably harder and is usually overkill, but
    worth asking before locking to pairs.

### 8. Edge cases to sanity-check the design against

22. If the agent in run B **drops** some messages the agent in run A
    kept (e.g., sampling change), is that a pass (expected behavior
    under new algorithm) or a fail (unexpected regression)? This is
    where equivalence vs tolerance really matters.
23. If agent B **reformats** messages (e.g., new JSON field ordering,
    added fields), is that a diff to flag or to normalize away?
24. Do you want to be able to diff captures taken from **different host
    environments** (different kernel versions, different Docker
    configurations)? That stresses the equivalence model pretty hard.

## Top three to decide first

When you come back to this, the three questions that gate most of the
others:

- **#1** — motivating case (tells you what regressions you're trying to
  catch, which tells you what layer to diff at).
- **#4** — layer of comparison (raw bytes, decoded content, aggregates —
  each has very different infrastructure requirements).
- **#10** — alignment strategy (requires the uniqueness prerequisite to
  be answered first).
