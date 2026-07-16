//! Antithesis `serial_driver_` for the `divide-preserves-aggregate-rate`
//! property, catalog Category A.
//!
//! `lading_throttle::Throttle::divide` splits a generator's throttle capacity
//! across `parallel_connections` workers by integer division. The contract,
//! stated in the code, is that the N workers sum back to the single-connection
//! configured rate. The failure it guards against is over-delivery, where
//! `divide` grants more than the configured aggregate, or silent
//! under-delivery at `N>1`, Wildcard #1, where `divide` loses more than the
//! integer-division remainder.
//!
//! This driver links `lading_throttle` and exercises the **real** `divide`
//! across a value menu, reading each result's `maximum_capacity()` and
//! asserting the aggregate invariant here in the harness. Keeping the
//! assertion in the workload, rather than compiled into lading's source, is
//! the whole point. All Antithesis property code lives under `antithesis/`.
//!
//! The check is deterministic, because `divide` is pure arithmetic, so its
//! value is as a cataloged regression tripwire, not a fault-search target.
//! The catalog marks this property fault-independent. Each invocation samples
//! one `(capacity, divisor)` scenario via Antithesis randomness. Re-runs sweep
//! the menu. The runtime block-size interaction, a divided per-worker capacity
//! below the block size that the TCP generator discards, is a separate,
//! oracle-side corroboration deferred until the probe reports per-connection
//! bytes.

// Keep the instrumentation crate linked. It provides the sancov coverage
// runtime the `antithesis` build's rustflags reference, reached through that
// runtime rather than any path we call directly.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;

use std::num::NonZeroU32;

use antithesis_sdk::prelude::*;
use antithesis_sdk::random::random_choice;
use antithesis_sdk::serde_json::json;
use lading_throttle::{Config, Throttle};

/// Menu axis is `parallel_connections`, the `divide` divisor. Boundaries are
/// 1, the trivial no-op split, and 2 and 3, the smallest even and odd cases,
/// plus a family climbing toward a large fan-out. Odd divisors 3 and 7 force a
/// non-zero integer-division remainder. Powers of two force the exact-split
/// case.
const DIVISORS: &[u32] = &[1, 2, 3, 4, 7, 8, 16, 64];

fn main() {
    antithesis_init();

    let divisor = *random_choice(DIVISORS).unwrap_or(&1);
    let capacity = sample_capacity(divisor);

    // `divide` must succeed for exactly the valid splits: `capacity >= divisor`
    // leaves each worker at least one unit, `capacity < divisor` floors to zero
    // and is rejected with `DivisionByZero`.
    let split_is_valid = capacity >= divisor;

    let observed = match divide_stable(capacity, divisor) {
        Some(observed) => observed,
        None => {
            // `divide` rejected the split. That is correct only for a sub-unit
            // split. A rejection of a valid split, where `capacity >= divisor`,
            // is a regression that would otherwise pass silently, so assert on
            // it here rather than swallowing every `Err` as "expected".
            assert_always!(
                !split_is_valid,
                "lading_throttle.divide.rejects_only_sub_unit_split",
                &json!({ "capacity": capacity, "divisor": divisor })
            );
            assert_reachable!(
                "serial_driver_divide_throttle reached the sub-unit divide case",
                &json!({ "capacity": capacity, "divisor": divisor })
            );
            return;
        }
    };

    // Each of the `divisor` workers receives the same `observed` capacity, so
    // together they can draw `aggregate = observed * divisor`. Widen to u64 so
    // the product cannot overflow, since `observed <= capacity <= u32::MAX`.
    let aggregate = u64::from(observed) * u64::from(divisor);
    let capacity = u64::from(capacity);
    let divisor = u64::from(divisor);
    let loss = capacity.saturating_sub(aggregate);

    let details = json!({
        "capacity": capacity,
        "divisor": divisor,
        "per_worker_capacity": observed,
        "aggregate": aggregate,
        "remainder_loss": loss,
    });

    // Safety: the split must never grant more than the single-connection
    // configured rate. Over-delivery, for example a `divide` that rounded up,
    // would make a target look worse than reality.
    assert_always!(
        aggregate <= capacity,
        "lading_throttle.divide.aggregate_not_exceeded",
        &details
    );

    // Safety: the only tolerated loss is the integer-division remainder, which
    // is strictly below `divisor`. A larger shortfall is silent under-delivery.
    // Together these two bounds pin `per_worker_capacity` to `floor(capacity /
    // divisor)`.
    assert_always!(
        loss < divisor,
        "lading_throttle.divide.remainder_loss_bounded",
        &details
    );
}

/// Build a `Stable` throttle of `capacity` tokens, run the real `divide`, and
/// return the resulting per-worker `maximum_capacity`. `None` when `divide`
/// rejects the split because `capacity < divisor`.
fn divide_stable(capacity: u32, divisor: u32) -> Option<u32> {
    let maximum_capacity = NonZeroU32::new(capacity)?;
    let throttle = Throttle::new_with_config(Config::Stable {
        maximum_capacity,
        timeout_micros: 0,
    });
    let divisor = NonZeroU32::new(divisor)?;
    throttle
        .divide(divisor)
        .ok()
        .map(|divided| divided.maximum_capacity())
}

/// Draw a capacity from a menu built around `divisor`. `divisor - 1` sits just
/// below the family floor. For `divisor >= 2` it makes the split floor to
/// zero, exercising `divide`'s `DivisionByZero` rejection. The family at and
/// above the divisor, `divisor`, `divisor + 1`, `2*divisor - 1`, walks the
/// remainder from 0 through its maximum of `divisor - 1`. The larger fixed
/// values exercise realistic byte rates including the evidence file's 3 MiB
/// example and a high boundary well under `u32::MAX`.
fn sample_capacity(divisor: u32) -> u32 {
    let menu: [u32; 8] = [
        divisor.saturating_sub(1),      // sub-unit split -> DivisionByZero, divisor >= 2
        divisor,                        // per-worker capacity 1, remainder 0
        divisor.saturating_add(1),      // remainder 1, or 0 at divisor == 1
        divisor.saturating_mul(2).saturating_sub(1), // remainder divisor - 1
        1000,                           // small round rate
        1 << 20,                        // 1 MiB
        3 * 1024 * 1024,                // 3 MiB, evidence file's example
        1 << 31,                        // large boundary, < u32::MAX
    ];
    random_choice(&menu).copied().unwrap_or(divisor).max(1)
}
