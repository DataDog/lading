# Adaptive Sampling Testing

Testing the Datadog Agent's experimental adaptive sampling feature — a per-pattern credit-based rate limiter that groups structurally similar logs and limits throughput per pattern.

## How Adaptive Sampling Works

### Overview

The agent tokenizes each log line into a structural pattern (classifying bytes as digits, punctuation, character runs, timestamps, etc. — but not the actual values). Structurally similar lines share a pattern, and each pattern has its own independent credit pool that controls how many logs of that pattern are allowed through.

### Pipeline Position

Adaptive sampling runs **after** multiline aggregation and truncation. The sampler sees the final assembled message content:

```
Tailer → Decoder → Multiline Aggregation → Truncation → ADAPTIVE SAMPLING → Sender
```

### The Credit Model

Each pattern tracks credits (a float counter):

1. **New pattern discovered:** starts with `burst_size` credits (e.g., 10)
2. **Log emitted:** costs 1 credit
3. **Log dropped:** when credits < 1.0; the drop count is tracked
4. **Credit refill:** `elapsed_seconds × rate_limit` credits added since last seen, capped at `burst_size`
5. **Sampled count tag:** when a log is finally emitted after drops, it gets tagged `adaptive_sampler_sampled_count:<N>` where N = number dropped since last emit

### Pattern Matching

Two logs share a pattern if their token sequences match at ≥ `match_threshold` (default 90%) similarity. Tokenization converts log content into abstract categories:

- `"2024-01-15 INFO request id=123"` → `[D4 Dash D2 Dash D2 Space Info Space C7 Space Id Equal D3]`
- `"2024-01-16 INFO request id=456"` → same token sequence → same pattern
- `"2024-01-15 ERROR connection failed"` → different tokens → different pattern

### Important Log Protection

When `protect_important_logs=true` (default), lines containing keywords like `ERROR`, `FATAL`, `PANIC`, `WARN`, `CRITICAL`, `EXCEPTION`, `CRASH`, `FAILURE`, `DEADLOCK`, `TIMEOUT` bypass sampling entirely. No pattern entry is created and the log is always emitted.

## Configuration

All settings are under `logs_config.experimental_adaptive_sampling.*` in `datadog.yaml` or via environment variables prefixed with `DD_LOGS_CONFIG_EXPERIMENTAL_ADAPTIVE_SAMPLING_`.

| Setting | Default | Description |
|---|---|---|
| `enabled` | `false` | Master on/off switch |
| `burst_size` | `1000.0` | Initial credits per pattern and credit cap |
| `rate_limit` | `1.0` | Credits refilled per second per pattern |
| `max_patterns` | `1000` | Max distinct patterns tracked; least-frequent evicted when full |
| `match_threshold` | `0.9` | Token similarity required to share a pattern (0-1] |
| `tokenizer_max_input_bytes` | `2048` | Bytes of each log tokenized for pattern matching |
| `protect_important_logs` | `true` | Bypass sampling for critical severity keywords |

### Per-Source Override

```yaml
logs:
  - type: file
    path: /var/log/app.log
    experimental_adaptive_sampling:
      enabled: true
```

### Test-Friendly Values

For testing we use small values to make behavior observable in short timeframes:

| Setting | Test Value | Why |
|---|---|---|
| `burst_size` | `10` | Don't need 1000 lines to exhaust the burst |
| `rate_limit` | `2.0` | Refills fast enough to observe in 5-10 seconds |

## Current Test Coverage

### Credit Model (implemented)

A model-based property test that simulates the credit state machine and compares predictions against agent output. The model tracks credits per the algorithm above (start with `burst_size`, consume 1 per emit, refill at `rate_limit` per second, cap at `burst_size`). It is intentionally simpler than the agent — no tokenizer, no pattern table, no hot-path optimization.

**What it tests:**
- Initial burst passes through (first `burst_size` lines delivered)
- Rate limiting after burst exhaustion (excess lines dropped)
- Credit refill over time (sleep N seconds, ~N×rate_limit more lines delivered)
- Sampled count tagging (`adaptive_sampler_sampled_count:<N>` tag on first post-drop emit)
- Variable-length action sequences (write, sleep, write, sleep, write, ...)

**Current status:** Passing at tolerance=0 (exact model match) across 5 cases with variable-length action sequences of 3-7 steps.

**Limitations:**
- Single pattern only (all lines have identical token structure via zero-padded numeric IDs)
- Fixed config (`burst_size=10`, `rate_limit=2.0`)
- Does not test tokenizer grouping decisions

### Key Discovery: UUID Tokenization

UUIDs contain varying mixes of hex digits and letters (e.g., `0c7db8bb` vs `3c7c10de`). The agent's tokenizer classifies these as different token sequences, causing lines with different UUIDs to be treated as different patterns. Fix: use zero-padded numeric IDs (`0000000001`, `0000000002`) which always produce identical token sequences.

## Testing Strategy Discussion

There are two separable concerns in adaptive sampling:

### 1. Credit Arithmetic (the model)

Given a known set of patterns and a sequence of writes/sleeps, does the agent correctly apply the credit rules? This is what the current model tests. It needs to scale to:

- **Varying `burst_size` and `rate_limit`** — proptest generates different config values per case, model adapts
- **Multiple patterns** — model tracks independent credit pools per pattern
- **Credit cap behavior** — long sleep beyond burst_size/rate_limit shouldn't accumulate unbounded credits

### 2. Pattern Grouping (the tokenizer)

Given two log lines and a config, does the agent correctly decide "same pattern" or "different pattern"? This is affected by three config values:

- **`match_threshold`** — what percentage of tokens must match
- **`tokenizer_max_input_bytes`** — how many bytes are tokenized
- **`protect_important_logs`** — whether severity keywords bypass everything

#### The Modeling Problem

We can't build a simplified tokenizer model because the agent's tokenizer has special-case keyword recognition (months, timezones, log levels like INFO/ERROR). A simplified model that doesn't know these keywords would constantly disagree with the agent, producing false positive failures.

#### Black-Box Pattern Probing

Instead of modeling the tokenizer, probe it directly. With `burst_size=1`:

1. Send line A → always arrives (new pattern, 1 credit)
2. Send line B → if it arrives, agent sees A and B as different patterns. If dropped, same pattern.

This is a pure black-box test of the tokenizer's grouping decisions. No model needed — just send pairs and observe.

**Properties to assert on probing results:**
- **Symmetry:** if A matches B, then B matches A
- **Consistency:** the same pair always produces the same decision
- **Structural sensitivity:** changing structure (adding fields, changing digit count) should produce different patterns
- **Value insensitivity:** changing just values (different number, different word of same length) should keep the same pattern

These properties don't need to know the tokenizer's rules — they test that the tokenizer behaves *reasonably* and *consistently*, and discover edge cases mechanically.

#### Connecting the Two: Probing Drives the Model

The pattern probing tests can generate knowledge about what the agent considers "same pattern" vs "different pattern." This knowledge can then drive the credit model tests:

1. Probing discovers: "lines with format X and format Y are considered the same pattern by the agent at match_threshold=0.9"
2. Credit model test generates a mix of X and Y lines, knows they share credits, asserts credit behavior correctly

This means the credit model doesn't need to assume pattern groupings — it learns them from the probing tests. The two test layers compose:

```
Pattern Probing (black-box)
  → discovers grouping decisions
  → feeds into ↓

Credit Model (model-based)  
  → uses discovered groupings
  → asserts credit arithmetic
```

This avoids both problems: no simplified tokenizer to maintain, and the credit model doesn't bake in assumptions about what the tokenizer does.

**Open question:** How to operationalize this composition. Options:
- Run probing first, persist results, credit model reads them (complex, stateful)
- Run probing inline at the start of each credit test to discover groupings on the fly (simpler, but adds container time)
- Define pattern groups empirically based on probing results and hardcode them (simplest, but needs updating if tokenizer changes)

## Planned Work (by priority — TBD with team)

### P1: Vary credit config

Make `burst_size` and `rate_limit` proptest-generated instead of constants. The model already accepts these as parameters. This tests that credit arithmetic works across many config combos without any code changes to the model.

Lower bound on `burst_size` to avoid degenerate cases (e.g., ≥ 3).

### P1: Multi-pattern credit testing

Extend the model to track credits per pattern. Generate lines in two+ structurally distinct groups (e.g., all-digit content vs all-letter content). Assert each group gets independent credits. Requires extending `SamplingAction::Write` with pattern tags.

### P2: Pattern probing scenario

Build the `burst_size=1` probing mechanism. Generate diverse line pairs, observe same/different decisions. Assert behavioral properties (symmetry, consistency, structural sensitivity, value insensitivity).

### P2: Important log bypass

Exhaust credits, then send a line with ERROR/FATAL. Assert it arrives. Test with `protect_important_logs=true` and `false` to verify the flag works.

### P3: Pattern eviction

Fill pattern table to `max_patterns`, introduce a new pattern. Verify least-frequent is evicted. Requires generating many distinct patterns.

### P3: Probing-driven credit tests

Use probing results to discover pattern groupings, then feed those groupings into multi-pattern credit tests. The most ambitious goal — composes both test layers.

### P3: match_threshold boundary testing

Generate pairs of lines with known structural similarity. Vary `match_threshold` and verify the grouping decision flips at the expected point.

### P3: tokenizer_max_input_bytes testing

Generate lines identical in the first N bytes but different after. Vary `tokenizer_max_input_bytes` and verify the agent only considers the configured prefix.

### P4: Interaction with multiline

Send multiline entries that share a pattern. Verify sampling operates on the aggregated message, not individual raw lines.

### P3: Agent downtime / ingest-time sampling

Model the scenario where a log pattern that is never sampled under normal operation (written slowly enough that credits refill between lines) gets sampled after agent downtime. During downtime, lines accumulate in the file. When the agent restarts and reads the backlog, it sees them as a burst and samples them. This demonstrates that sampling is based on ingest time, not write time.

Requires: agent stop/restart within a test case (new orchestrator capability), or a simpler simulation where lines that would have been written slowly are instead written all at once (modeling what the agent sees post-restart). Open questions around file offset tracking on restart and whether to demonstrate normal-operation-no-sampling first vs just the post-downtime burst.

### P4: FUSE-based timing control

Use lading's `logrotate_fs` to control exactly when lines appear, removing file tailing latency. Would allow tighter credit refill assertions and testing of sub-second timing behavior.

## Implementation Notes

### Action Sequences

The orchestrator supports variable-length action sequences via `run_action_sequence`. Each case gets a proptest-generated sequence of `Write(N)` and `Sleep(N)` steps. The model walks the same sequence and predicts which lines should be delivered.

### Temp Directory Contents

Each test case preserves (with `LADING_KEEP_TEMP=1`):

```
lading_proptest_XXXXXX/
  config/datadog.yaml
  config/conf.d/proptest.d/conf.yaml
  logs/proptest.log          # raw input
  output.json                # full intake payloads
  output_messages.txt        # one message per line with byte count
  summary.txt                # action sequence, counts, drops
```

### Line ID Strategy

Lines use zero-padded numeric IDs (`0000000001`) instead of UUIDs. This ensures the agent's tokenizer produces identical token sequences for all lines within a pattern group. UUIDs contain varying hex digit/letter mixes that the tokenizer classifies differently.

## Agent Source References

| File | Purpose |
|---|---|
| `pkg/logs/internal/decoder/preprocessor/sampler.go` | Core sampling algorithm |
| `pkg/logs/internal/decoder/preprocessor/tokens.go` | Token types and matching |
| `pkg/logs/internal/decoder/preprocessor/tokenizer.go` | Log line tokenization |
| `pkg/logs/internal/decoder/preprocessor/sampler_test.go` | Unit tests (89 tests) |
| `pkg/config/setup/common_settings.go` | Configuration defaults |
| `pkg/logs/internal/decoder/preprocessor/adaptive_sampler.allium` | Formal specification |
