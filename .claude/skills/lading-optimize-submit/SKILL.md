---
name: lading-optimize-submit
description: "End-to-end lading performance optimization with git automation: runs the full optimization hunt (profiling, baselines, implementation, review), then creates a git branch, commits with benchmark results, and optionally opens a PR. Use when the user wants to optimize lading and submit the results, or says 'optimize and commit', 'optimize and PR', or 'submit optimization'."
allowed-tools: Bash(git:*) Bash(gh:*) Bash(cat:*) Read Skill
---

# Optimization Submit Workflow

Wraps `/lading-optimize-hunt` with git automation: branch creation, commit with benchmark results, and optional PR.

---

## Phase 0: Pre-flight

Run `/lading-preflight` first to ensure environment is ready.

---

## Phase 1: Prepare Git Environment

```bash
git checkout main && git pull
git status
```

**STOP if working directory is dirty.** Commit or stash changes before proceeding.

---

## Phase 2: Hunt

Run `/lading-optimize-hunt`.

**CRITICAL: After `/lading-optimize-hunt` completes, you MUST return here to Phase 3.** The hunt handles target selection, baselines, implementation, review, and verdict recording -- but NOT git operations (branch/commit/push/PR), which are handled below.

---

## Phase 3: Create Optimization Branch

Create a new branch and add the changes.
```bash
# Create descriptive branch name
# Format: opt/<crate>-<technique>
# Examples:
#   opt/payload-cache-prealloc
#   opt/throttle-avoid-clone
#   opt/syslog-buffer-reuse

git checkout -b opt/<crate>-<technique>
git add .
```

Using the template in `.claude/skills/lading-optimize-submit/assets/commit-template.txt`, commit the changes:

```bash
# Example:
git commit -m "opt: buffer reuse in syslog serialization

Replaced per-iteration format!() with reusable Vec<u8> buffer.

Target: lading_payload/src/syslog.rs::Syslog5424::to_bytes
Technique: buffer-reuse

Micro-benchmarks:
  syslog_100MiB: +42.0% throughput (481 -> 683 MiB/s)

Macro-benchmarks (payloadtool):
  Time: -14.5% (8.3 ms -> 7.1 ms)
  Memory: -35.8% (6.17 MiB -> 3.96 MiB)
  Allocations: -49.3% (67,688 -> 34,331)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
"
```

**Note:**
- First line must be ≤50 characters (Git best practice)
- Replace {MODEL} with the actual Claude model being used (e.g., "Claude Sonnet 4.5", "Claude Opus 4.5").

---

## Phase 4: Push and Create PR (Optional)

```bash
# Push branch to remote
git push -u origin opt/<crate>-<technique>

# Create PR using gh CLI
gh pr create \
  --title "opt: <short description>" \
  --body "$(cat <<'PR_EOF'
## Summary
<What was optimized>

## Benchmark Results

### Micro-benchmarks
- <benchmark_name>: <result>

### Macro-benchmarks (payloadtool)
- Time: <-X%> (<old> ms -> <new> ms)
- Memory: <-X%> (<old> MiB -> <new> MiB)
- Allocations: <-X%> (<old> -> <new>)

## Validation
- [x] ci/validate passes
- [x] Kani proofs pass (or N/A: <reason>)
- [x] Determinism verified
PR_EOF
)"
```
---
