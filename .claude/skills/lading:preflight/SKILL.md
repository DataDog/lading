---
name: lading:preflight
description: Environment validation checklist. Run FIRST in new sessions.
---

# Pre-flight Checklist

Run at session start. Stop on first failure.

## Required Tools

| Tool | Check | Install if missing |
|------|-------|-------------------|
| rustc >= 1.70 | `rustc --version` | rustup |
| cargo-nextest | `cargo nextest --version` | `cargo install cargo-nextest` |
| hyperfine | `hyperfine --version` | `brew install hyperfine` |
| cargo-criterion | `cargo criterion --version` | `cargo install cargo-criterion` |
| profiler | `which samply \|\| which sample` | `cargo install samply` |

## CI Scripts

Verify executable: `ci/validate`, `ci/check`, `ci/clippy`, `ci/fmt`, `ci/kani`

## Build Check

```bash
ci/check && cargo build --release --bin payloadtool
```

## Worktree

If `../lading-baseline` doesn't exist:
```bash
git worktree add ../lading-baseline main
```

## Git Config

Verify: `git config user.name && git config user.email`

## Quick Pass/Fail

All checks pass → Ready for `/lading:optimize:hunt`
