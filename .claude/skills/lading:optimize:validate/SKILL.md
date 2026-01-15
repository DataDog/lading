---
name: lading:optimize:validate
description: Validates discovered bugs with reproducing tests and validates fixes with regression tests. Called by other skills when bugs are found during optimization hunting. Creates property tests (proptest) and Kani proofs when feasible.
---

# Correctness Validation

Called when other skills discover bugs. Creates tests that prove the bug existed and the fix works.

**No bug fix without a test that would have caught it.**

## Workflow

1. Document the bug (file, function, root cause)
2. Attempt Kani proof → if fails, document why
3. Create property test (proptest) → must fail before fix
4. Apply fix → test must pass
5. `ci/validate` → must pass
6. Verify determinism → same seed = same output
7. Record in db.yaml

## Test Strategy

**Always prefer proptest over unit tests** (per CLAUDE.md).

| Bug Category | Test Approach |
|--------------|---------------|
| Off-by-one | proptest: output.len() == expected |
| Capacity | proptest: no panic, size >= requested |
| Determinism | proptest: same seed = same output |
| Overflow | Kani proof (if feasible) |
| Panic path | proptest: no panic for any input |

## Kani First, Then Proptest

```bash
ci/kani lading_throttle  # or lading_payload
```

If Kani fails (compilation error, timeout), document why and use proptest instead.

## Property Test Template

```rust
proptest! {
    #[test]
    fn invariant_name(input in <strategy>) {
        let result = function_under_test(input);
        prop_assert!(<invariant>, "message");
    }
}
```

## Verification Checklist

- [ ] Test FAILS on buggy code (checkout before fix, run test)
- [ ] Test PASSES on fixed code
- [ ] `ci/validate` passes
- [ ] Determinism verified via fingerprints
- [ ] No `.unwrap()` in fix

## Recording

Update `db.yaml`. See [DB_SCHEMA.md](../shared/DB_SCHEMA.md).

```yaml
entries:
  - file: <path>
    function: <name>
    category: <off-by-one|capacity|determinism|overflow|panic>
    status: validated
    file: db/<name>.yaml
```

## Return Status

After validation, return to calling skill with:
- `VALIDATED` → ready for merge
- `INVALID` → test doesn't reproduce bug
- `NEEDS_WORK` → fix incomplete
