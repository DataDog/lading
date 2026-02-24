---
name: lading-optimize-find-target
description: Finds a valid optimization target in lading. Returns a filled target.yaml template with pattern, technique, target, file, bench, and fingerprint. Use before /lading-optimize-hunt or when selecting a new optimization target.
allowed-tools: Bash Read Glob Grep
---

## Phase 1: Discover Benchmark-Eligible Modules

A module is eligible if it has a Criterion benchmark in `lading_payload/benches/`.

1. **List benchmarks**: Glob `lading_payload/benches/*.rs` — each filename (minus `.rs`) is a bench name
2. **Resolve sources**: For each bench name, find the corresponding source file(s) under `lading_payload/src/`. Check single-file modules (`{name}.rs`), directory modules (`{name}/`), and parent-module patterns (e.g., `opentelemetry_log` → `opentelemetry/log.rs`)
3. **Match fingerprints**: Glob `ci/fingerprints/*/lading.yaml` — each directory name is a fingerprint. Match fingerprints to modules by reading the `variant:` key from each config

The result is a set of `(bench, source_files, fingerprint_or_none)` triples.

---

## Phase 2: Profile Allocation Intensity

Run the profiling script:

```bash
.claude/skills/lading-optimize-find-target/scripts/profile-modules
```

Output is TSV: `module, allocations, total_bytes, peak_live_bytes`. Record per-module.

---

## Phase 3: Learn from Past Optimizations

Read the optimization history to understand what techniques work and what's already done:

```
Read .claude/skills/lading-optimize-hunt/assets/db.yaml
```

For each entry, read its detail file (`file:` field, relative to `.claude/skills/lading-optimize-hunt/`) to extract:
- **Technique and measurements** — which techniques yielded what % improvements
- **Lessons** — what patterns were optimized and what the before/after looked like
- **Targets already covered** — so you skip them

This history teaches you what to look for. Successful past techniques are strong signals for where to look next. The `lessons` field often suggests next targets explicitly.

---

## Phase 4: Find Opportunities

Scan **every** benchmark-eligible source module for **every** known pattern below.
This is an exhaustive cross-product — do not short-circuit after finding one hit.

### Known Patterns

| Name                      | Pattern                           | Technique                          |
| ------------------------- | --------------------------------- | ---------------------------------- |
| `vec-with-capacity`       | `Vec::new()` + repeated push      | `Vec::with_capacity(n)`            |
| `string-with-capacity`    | `String::new()` + repeated push   | `String::with_capacity(n)`         |
| `map-with-capacity`       | `FxHashMap::default()` hot insert | `FxHashMap::with_capacity(n)`      |
| `buffer-reuse`            | `format!()` in hot loop           | `write!()` to reused buffer        |
| `slice-params`            | `&Vec<T>` or `&String` parameter  | `&[T]` or `&str` slice             |
| `hoist-allocation`        | Allocation in hot loop            | Move allocation outside loop       |
| `object-pool`             | Repeated temp allocations         | Object pool / buffer reuse         |
| `borrow-not-clone`        | Clone where borrow works          | Use reference                      |
| `inline`                  | Hot cross-crate fn call           | `#[inline]` attribute              |
| `lazy-iterators`          | Intermediate `.collect()` calls   | Iterator chains without collect    |
| `box-large-structs`       | Large struct by value             | Box or reference                   |
| `bounded-buffer`          | Unbounded growth                  | Bounded buffer with `.clear()`     |
| `scratch-buffer`          | `encode_to_vec()` per call        | Reusable `BytesMut` scratch buffer |
| `on-demand-serialization` | Deep clone of template in loop    | Incremental mutation / COW         |

### Procedure (must follow exactly)

**Step 1 — Read source files.** For each module's source file(s), Read the full file (excluding `#[cfg(test)]` blocks). Understand the data flow: what structs exist, how serialization works, where the hot path is, and what allocations occur.

**Step 2 — Identify patterns.** For each module, check whether any of the Known Patterns above apply. Record a hit matrix:

```
module × pattern → match count (0 = no hit)
```

Show the full matrix as a table. Every cell must have a value. Do NOT skip any combination.

**Step 3 — Record opportunities.** Each verified hot-path hit becomes an opportunity:
`(pattern, technique, target_function, file, module, allocations_from_profiling)`

---

## Phase 5: Filter

Remove any opportunity that:

1. **Already in db.yaml** — same function + semantically equivalent technique already exists
2. **No benchmark** — module has no matching bench file in `lading_payload/benches/`

If zero survive, STOP: "No valid optimization targets found."

---

## Phase 6: Rank

Sort by two dimensions:

1. **Technique impact** — techniques with measured history in db.yaml rank higher (compute avg % improvement from `measurements.benchmarks.macro`). Unknown techniques rank last.
2. **Allocation intensity** — modules with higher allocation counts from profiling rank higher.

Sort by technique impact first, allocation intensity second.
**Tiebreaker:** Prefer modules with no prior db.yaml entries, then alphabetical file name.

Show sorted results in a table.

---

## Phase 7: Return Result

Pick the top-ranked opportunity. Return as a fenced YAML code block. **Do NOT write to disk.**

```yaml
pattern: "<description of the code pattern found>"
technique: "<pattern name / optimization technique to apply>"
target: "<Module::function>"
file: "<relative path to source file>"
bench: "<relative path to Criterion benchmark .rs file>"
fingerprint: "<relative path to fingerprint lading.yaml config, or null>"
```

---
