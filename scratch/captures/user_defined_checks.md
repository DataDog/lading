# User-Defined Checks: Design Exploration

> Status: exploratory. No implementation proposed. This document records ideas
> and trade-offs discussed around making correctness assertions in
> `lading_analysis` data-driven rather than code-defined, so they can be
> refined and revisited later.

## Motivation

`lading_analysis` today ships a fixed set of Rust-coded checks
(`completeness`, `fabrication`, `duplication`, `latency`, `truncation`, and a
planned `multiline`). Each check is a `struct` with a `Check` trait impl. New
checks require a Rust change, a crate build, and a release — which is
acceptable for the framework authors but a high bar for users who want to
express a one-off invariant about their specific agent behavior.

The capture pipeline already gives us a rich offline substrate — every FUSE
read, every blackhole payload, reconstructed inputs as both raw byte ranges
and stitched lines, plus relative timestamps from a shared epoch. The
question is whether we can expose that substrate through a declarative
configuration surface so invariants can be authored without writing Rust.

## The data model behind every check we have

Each existing check operates on some combination of three collections:

- `inputs` — the reconstructed newline-delimited lines, each with a
  contributing-reads list and timestamps.
- `outputs` — parsed messages from the blackhole capture, each with a
  receipt timestamp.
- `matches` — some relation between inputs and outputs, defined by the
  check.

The checks differ mostly in (a) **how they associate** inputs to outputs and
(b) **what they assert** on the matched and unmatched subsets. Summarized:

| Check | Association | Assertion |
|---|---|---|
| completeness | content hash | ratio of matched inputs ≥ threshold |
| fabrication | content hash | count of unmatched outputs ≤ threshold |
| duplication | content hash | outputs-per-input ratio ≤ threshold |
| latency | content hash | per-pair `(out.ts − last_contributing_read.ts)` percentile ≤ threshold |
| truncation | header regex → structural split | each input maps to N pieces with `...TRUNCATED...` markers in correct positions |
| multiline | header regex → structural join | N inputs join to one output with the expected joined bytes |

Two primitives fall out: **matchers** (how to associate) and
**constraints** (what must hold over the matched/unmatched sets). That
factoring is what suggests a data-driven rewrite is tractable.

## A strawman closed schema

The first three checks (completeness, fabrication, duplication) and most of
latency can be expressed in a fully closed YAML schema — no expression
language required:

```yaml
matchers:
  - name: by_content
    type: content_hash

checks:
  - completeness:
      matcher: by_content
      min_ratio: 0.95
      group_by: header.cat            # auto-parsed per-group reporting

  - fabrication:
      matcher: by_content
      max_count: 0
      report_examples: 5

  - duplication:
      matcher: by_content
      max_ratio: 0.01

  - latency:
      matcher: by_content
      from: input.last_contributing_read.ts
      to:   output.ts
      thresholds: { p99: 10000 }
```

Structural checks (truncation, multiline) push on the schema. They need a
matcher that produces not a single pair but a one-to-N (truncation) or
N-to-one (multiline) relation, and assertions that depend on the structural
shape rather than just counts or content equality. A shape for this:

```yaml
matchers:
  - name: by_truncation_header
    type: header_regex
    pattern: "^\\[cat=(?P<cat>\\w+) id=(?P<id>\\d+) size=(?P<size>\\d+)\\]"
    reassemble:
      kind: split_by_marker
      marker: "...TRUNCATED..."
      max_piece_size: 900000

  - name: by_multiline_header
    type: header_regex
    pattern: "^\\[ml_id=(?P<id>\\d+) lines=(?P<n>\\d+)\\]"
    reassemble:
      kind: join_n
      n_from: captures.n
      separator: "\n"

checks:
  - reassembly_correct:
      matcher: by_truncation_header
      report_by: captures.cat

  - reassembly_correct:
      matcher: by_multiline_header
      report_by: captures.ml_id
```

The `reassemble` block is the heart of structural support: it declares how
multiple inputs or outputs relate to a single counterpart, so the check can
validate the relation without the assertion author writing any logic.

## Where the closed schema breaks

Two kinds of invariants don't fit a closed schema cleanly:

### 1. Conditional assertions
"If input.size > 900_000 then expect N pieces, otherwise expect 1" is a
pattern that shows up in truncation today and will show up again. A closed
schema either needs every branching rule pre-named (unbounded) or needs an
embedded predicate language.

### 2. Derived quantities
Constraints like "per-pair receive-order matches input read-order within
`group`" or "rate of output messages in any 1-second window ≤ N" ask for
aggregations the schema doesn't know about. You either enumerate these
aggregations as first-class primitives, or you let users write expressions
over a fixed data model.

## Three shapes and their trade-offs

**(a) Closed schema, no expressions.** The strawman above. Covers 80% of
cases we have today. Simplest to specify, fastest to build, best error
messages (the engine knows what every field means). Fails the moment a user
wants a conditional or a custom aggregation.

**(b) Closed schema + embedded predicate language (CEL, Rego, jq-ish).**
Users hit expressions only when they fall off the structured path. CEL is
the most appealing candidate — it's a well-specified Google policy
expression language with Rust implementations, designed for exactly this
shape of problem. The cost is that users now have two languages (YAML
structure + CEL for conditionals), and errors in expressions require the
engine to surface the failing input bindings.

**(c) Everything-is-expressions (Rego, full Datalog, SQL-like).** Maximum
power. Highest cost to implement, learn, and debug. Overkill for a
correctness harness. Likely outcome: one person on the team becomes the
resident Rego whisperer and everyone else routes questions through them.

Realistic trajectory: start at (a), graduate to (b) when a concrete
invariant shows up that won't fit. Skip (c) unless the requirements genuinely
outgrow (b), which for correctness testing is improbable.

## Case study: adaptive sampling property test

The adaptive sampling property test (low-value patterns are sampled to N
transmissions per interval T; high-value patterns under the threshold pass
through unchanged) maps onto this model cleanly. Sketch:

```yaml
matchers:
  - name: by_group
    type: header_regex
    pattern: "\\[T=(?P<t>\\d+) g=(?P<group>\\w+) m=(?P<m>\\d+)\\]"

checks:
  - sampling_count:
      matcher: by_group
      partition: [captures.group, captures.t]
      expected_output_count: min(input_count_in_cell, config.N)
      config:
        N: 5

  - completeness:               # liveness: all input lines are readable
      matcher: by_content
      min_ratio: 1.0

  - fidelity:                   # transmitted lines are byte-identical
      type: content_equality
      matcher: by_group
      require: bytes_equal
```

The `partition` field on the matcher is the interesting new primitive: it
groups matched pairs by captured header fields, so constraints apply per
cell. `expected_output_count` uses `min(...)` which is the first hint of
expression syntax — in shape (a) this would need to be a named constraint
like `min_of_input_count_and_config_value`, or in shape (b) it would be a
one-line CEL expression.

The property-testing orchestrator (configuration sweep, shrinking on
failure) is a layer **above** the analysis tool, not inside it. It runs
lading + agent + `lading-analysis --json` in a loop and uses the JSON
output to drive shrinkage. The analysis tool stays a pure single-instance
checker.

## Debuggability is the real design tension

When a Rust `Check` impl fails, you can set a breakpoint in the check and
inspect state directly. When a declarative rule fails, the engine has to
tell you **why**: which input didn't match, what output was expected, which
assertion clause was violated, with enough context to reproduce and fix.
This is the part homegrown rules engines most often skimp on and later
regret. Designing for good error output from day one matters more than
designing for expressive power.

Concretely, for every constraint type the schema supports, the engine needs:

- a natural-language statement of what was asserted,
- the specific input and output values that participated in the failing
  instance,
- and — ideally — a short explanation of what shape of result would have
  satisfied the check.

If that is not built in, users will write YAML checks, get failures that say
"check `foo` failed," stare at captures for half an hour, and then rewrite
the check in Rust where they can print things. That defeats the purpose.

## Open questions worth pinning down before any implementation

1. **Who is the audience?** DD-agent engineers authoring checks for their
   specific agent behavior, or lading contributors extending the framework?
   Different audiences tolerate different complexity budgets.

2. **Should checks be composable?** For example, "completeness only on
   records where `category == 'normal'`" can be a first-class `filter:` on
   matchers, or it can push toward a general expression language. The
   choice shapes the schema.

3. **Do we want temporal invariants?** "Output order per `ml_id` must
   reflect input read-order" is absent today but natural for log pipelines.
   Adding it later is much easier if the matcher model exposes timestamps
   as a first-class field on pairs from the outset.

4. **Rebuild-free authoring?** If checks must be writable without rebuilding
   lading, YAML is required. If a separate `lading_user_checks` crate
   compiled against `lading_analysis`'s public API is acceptable, Rust
   suffices — and retains the debuggability advantage.

5. **Inline vs offline.** Our pipeline is offline (captures → analysis).
   Property testing harnesses sometimes want inline (faster feedback,
   shared state). Offline wins on replayability: the same capture can be
   re-analyzed under a new check definition without re-running the
   experiment. That's especially valuable when shrinking failures.

## One-line summary of the position

The data model our checks already operate on factors cleanly into matchers
and constraints; a closed YAML schema over those two primitives covers
everything we have today except the structural checks, which fit with a
`reassemble:` extension to matchers; conditional invariants and derived
quantities are the natural trigger for adding an embedded predicate
language; and the engineering risk that matters most is not expressive
power but failure messages that let authors diagnose what went wrong
without dropping into Rust.
