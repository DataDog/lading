---
name: lading-optimize-submit
description: Full optimization workflow with git branch creation, commits, and optional PR. Wraps /lading-optimize-hunt with git automation.
allowed-tools: Bash(git:*) Bash(gh:*) Bash(cat:*) Read Write Edit Skill
---

# Optimization Submit Workflow

**Complete optimization workflow with git automation.** This skill wraps `/lading-optimize-hunt` and handles:
- Git branch creation
- Baseline benchmarking
- Code changes
- Re-benchmarking with changes
- Git commit with formatted results
- Optional push and PR creation

---

## Phase 0: Pre-flight

Run `/lading-preflight` first to ensure environment is ready.

---

## Phase 1: Prepare Git Environment

```bash
# Ensure clean state on main
git checkout main && git pull

# Verify clean working directory
git status
```

**STOP if working directory is dirty.** Commit or stash changes before proceeding.

---

## Phase 2: Hunt

Run `/lading-optimize-hunt`.

**CRITICAL: After `/lading-optimize-hunt` completes, you MUST return here to Phase 3.**

The hunt workflow will:
- Select and analyze optimization targets
- Capture baseline benchmarks
- Implement optimization
- Run basic ci/validate check
- Invoke /lading-optimize-review (which runs post-change benchmarks and judges)

**The hunt will:**
- Record the verdict in `.claude/skills/lading-optimize-hunt/assets/db.yaml` after review returns

**BUT the hunt does NOT:**
- Run post-change benchmarks (review does this)
- Make pass/fail decisions (review does this)
- Create git branches
- Commit changes
- Push to remote
- Create PRs

Those are the responsibility of THIS skill (lading-optimize-submit).

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
  --body "$(cat <<'EOF'
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

Ready for `/lading-optimize-review`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
---
