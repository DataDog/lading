---
name: lading-preflight
description: Environment validation checklist. Run this FIRST when starting a new Claude session to verify the environment is ready for optimization work. Checks Rust toolchain, ci/ scripts, build, benchmarking tools, profilers, memory tools, and git state.
allowed-tools: Bash
---

# Run preflight script

```bash
scripts/preflight
```

**STOP on any failure.** Use the table below to resolve `[X]` items:

# Offer suggestions

| Failed Check                             | Fix                                                               |
| ---------------------------------------- | ----------------------------------------------------------------- |
| rustc not found                          | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| cargo not found                          | Reinstall rustup                                                  |
| cargo-nextest not found                  | `cargo install cargo-nextest`                                     |
| ci/* not executable                      | `chmod +x ci/*`                                                   |
| ci/check failed                          | `cargo update`, check dependency issues                           |
| payloadtool build failed                 | Check Rust version and dependencies                               |
| hyperfine not found                      | `brew install hyperfine`                                          |
| cargo-criterion not found                | `cargo install cargo-criterion`                                   |
| criterion benchmarks missing             | Check `lading_payload/benches/` and `Cargo.toml`                  |
| payloadtool --memory-stats not supported | Rebuild payloadtool from current branch                           |
| git user.name not set                    | `git config user.name "Your Name"`                                |
| git user.email not set                   | `git config user.email "you@example.com"`                         |
