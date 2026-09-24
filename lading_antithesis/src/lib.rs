//! Thin facade over the Antithesis SDK.
//!
//! This crate owns the single `antithesis` feature for the project. It is no-op
//! unless specifically enabled. Each macro forwards to the SDK macro. These
//! macros convert a trailing map into JSON for consumption by the underlying
//! SDK.
//!
//! # Use Guidance
//!
//! A more precise assertion gives Antithesis better exploration. Prefer the
//! numeric macros like `always_gt!(x, y, ...)` over `always!(x > y, ...)`.

#![deny(missing_docs)]

/// SDK re-export. The macros forward through this path so call sites need no
/// `antithesis_sdk` dependency of their own.
#[cfg(feature = "antithesis")]
#[doc(hidden)]
pub use antithesis_sdk;
/// `serde_json::json` re-export. The macros wrap detail maps through this path
/// so call sites need no `serde_json` dependency of their own.
#[cfg(feature = "antithesis")]
#[doc(hidden)]
pub use serde_json::json;

/// Initializes the Antithesis SDK and its assertion catalog. No-op without the
/// `antithesis` feature.
#[cfg(feature = "antithesis")]
pub fn init() {
    antithesis_sdk::antithesis_init();
}

/// Initializes the Antithesis SDK and its assertion catalog. No-op without the
/// `antithesis` feature.
#[cfg(not(feature = "antithesis"))]
pub fn init() {}

// Feature on. Every macro forwards to the live SDK macro at the call site.
#[cfg(feature = "antithesis")]
mod enabled {
    /// Asserts `condition` holds every time this line runs, and that the line runs at least once.
    #[macro_export]
    macro_rules! always {
        ($condition:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always!($condition, $message, &$crate::json!({ $($details)* }))
        };
        ($condition:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_always!($condition, $message)
        };
    }

    /// Asserts `condition` holds every time this line runs. Passes even if the line never runs.
    #[macro_export]
    macro_rules! always_or_unreachable {
        ($condition:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always_or_unreachable!($condition, $message, &$crate::json!({ $($details)* }))
        };
        ($condition:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_always_or_unreachable!($condition, $message)
        };
    }

    /// Asserts `condition` holds at least once across all runs of this line.
    #[macro_export]
    macro_rules! sometimes {
        ($condition:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_sometimes!($condition, $message, &$crate::json!({ $($details)* }))
        };
        ($condition:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_sometimes!($condition, $message)
        };
    }

    /// Asserts this line is reached at least once.
    #[macro_export]
    macro_rules! reachable {
        ($message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_reachable!($message, &$crate::json!({ $($details)* }))
        };
        ($message:literal) => {
            $crate::antithesis_sdk::assert_reachable!($message)
        };
    }

    /// Asserts this line is never reached.
    #[macro_export]
    macro_rules! unreachable {
        ($message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_unreachable!($message, &$crate::json!({ $($details)* }))
        };
        ($message:literal) => {
            $crate::antithesis_sdk::assert_unreachable!($message)
        };
    }

    /// Asserts `left > right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_gt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always_greater_than!($left, $right, $message, &$crate::json!({ $($details)* }))
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_always_greater_than!($left, $right, $message)
        };
    }

    /// Asserts `left >= right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_ge {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always_greater_than_or_equal_to!(
                $left, $right, $message, &$crate::json!({ $($details)* })
            )
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_always_greater_than_or_equal_to!($left, $right, $message)
        };
    }

    /// Asserts `left < right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_lt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always_less_than!($left, $right, $message, &$crate::json!({ $($details)* }))
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_always_less_than!($left, $right, $message)
        };
    }

    /// Asserts `left <= right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_le {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always_less_than_or_equal_to!(
                $left, $right, $message, &$crate::json!({ $($details)* })
            )
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_always_less_than_or_equal_to!($left, $right, $message)
        };
    }

    /// Asserts `left > right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_gt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_sometimes_greater_than!($left, $right, $message, &$crate::json!({ $($details)* }))
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_sometimes_greater_than!($left, $right, $message)
        };
    }

    /// Asserts `left >= right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_ge {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_sometimes_greater_than_or_equal_to!(
                $left, $right, $message, &$crate::json!({ $($details)* })
            )
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_sometimes_greater_than_or_equal_to!($left, $right, $message)
        };
    }

    /// Asserts `left < right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_lt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_sometimes_less_than!($left, $right, $message, &$crate::json!({ $($details)* }))
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_sometimes_less_than!($left, $right, $message)
        };
    }

    /// Asserts `left <= right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_le {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_sometimes_less_than_or_equal_to!(
                $left, $right, $message, &$crate::json!({ $($details)* })
            )
        };
        ($left:expr, $right:expr, $message:literal) => {
            $crate::antithesis_sdk::assert_sometimes_less_than_or_equal_to!($left, $right, $message)
        };
    }

    /// Asserts at least one of the named conditions always holds, with per-name guidance.
    #[macro_export]
    macro_rules! always_some {
        ({ $($conditions:tt)* }, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_always_some!({ $($conditions)* }, $message, &$crate::json!({ $($details)* }))
        };
        ({ $($conditions:tt)* }, $message:literal) => {
            $crate::antithesis_sdk::assert_always_some!({ $($conditions)* }, $message)
        };
    }

    /// Asserts every named condition holds at least once, with per-name guidance.
    #[macro_export]
    macro_rules! sometimes_all {
        ({ $($conditions:tt)* }, $message:literal, { $($details:tt)* }) => {
            $crate::antithesis_sdk::assert_sometimes_all!({ $($conditions)* }, $message, &$crate::json!({ $($details)* }))
        };
        ({ $($conditions:tt)* }, $message:literal) => {
            $crate::antithesis_sdk::assert_sometimes_all!({ $($conditions)* }, $message)
        };
    }
}

// Feature off. Every macro is a no-op that elides its arguments unevaluated.
#[cfg(not(feature = "antithesis"))]
mod disabled {
    /// Asserts `condition` holds every time this line runs, and that the line runs at least once.
    #[macro_export]
    macro_rules! always {
        ($condition:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($condition:expr, $message:literal) => {{}};
    }

    /// Asserts `condition` holds every time this line runs. Passes even if the line never runs.
    #[macro_export]
    macro_rules! always_or_unreachable {
        ($condition:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($condition:expr, $message:literal) => {{}};
    }

    /// Asserts `condition` holds at least once across all runs of this line.
    #[macro_export]
    macro_rules! sometimes {
        ($condition:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($condition:expr, $message:literal) => {{}};
    }

    /// Asserts this line is reached at least once.
    #[macro_export]
    macro_rules! reachable {
        ($message:literal, { $($details:tt)* }) => {{}};
        ($message:literal) => {{}};
    }

    /// Asserts this line is never reached.
    #[macro_export]
    macro_rules! unreachable {
        ($message:literal, { $($details:tt)* }) => {{}};
        ($message:literal) => {{}};
    }

    /// Asserts `left > right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_gt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left >= right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_ge {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left < right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_lt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left <= right` always, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! always_le {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left > right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_gt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left >= right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_ge {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left < right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_lt {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts `left <= right` at least once, with numeric guidance on the two operands.
    #[macro_export]
    macro_rules! sometimes_le {
        ($left:expr, $right:expr, $message:literal, { $($details:tt)* }) => {{}};
        ($left:expr, $right:expr, $message:literal) => {{}};
    }

    /// Asserts at least one of the named conditions always holds, with per-name guidance.
    #[macro_export]
    macro_rules! always_some {
        ({ $($conditions:tt)* }, $message:literal, { $($details:tt)* }) => {{}};
        ({ $($conditions:tt)* }, $message:literal) => {{}};
    }

    /// Asserts every named condition holds at least once, with per-name guidance.
    #[macro_export]
    macro_rules! sometimes_all {
        ({ $($conditions:tt)* }, $message:literal, { $($details:tt)* }) => {{}};
        ({ $($conditions:tt)* }, $message:literal) => {{}};
    }
}

// Expansion guards. A `macro_rules!` body is only type-checked once expanded,
// and nothing in the workspace calls these macros yet, so without a call site
// the forwarding paths into the SDK are never checked. Each macro is invoked
// here in both arities so the compiler resolves every path under whichever
// feature state is being built.
//
// Literals only, no local bindings: with the feature off the macros elide their
// arguments unevaluated, so a binding passed to one would trip
// `unused_variables`, which the workspace denies.
#[cfg(test)]
mod tests {
    #[test]
    fn boolean_macros_expand() {
        crate::always!(1 > 0, "always holds");
        crate::always!(1 > 0, "always holds with details", { "detail": 1 });
        crate::always_or_unreachable!(1 > 0, "always holds or never runs");
        crate::always_or_unreachable!(1 > 0, "always holds or never runs with details", { "detail": 1 });
        crate::sometimes!(1 > 0, "holds at least once");
        crate::sometimes!(1 > 0, "holds at least once with details", { "detail": 1 });
    }

    #[test]
    fn reachability_macros_expand() {
        crate::reachable!("this line is reached");
        crate::reachable!("this line is reached with details", { "detail": 1 });
    }

    // `unreachable!` is exercised for expansion only. Reaching it reports a
    // failed assertion to the SDK handler, which is inert outside an Antithesis
    // run, and these unit tests never execute inside one.
    #[test]
    fn unreachable_macro_expands() {
        if false {
            crate::unreachable!("never reached");
        }
        if false {
            crate::unreachable!("never reached with details", { "detail": 1 });
        }
    }

    #[test]
    fn numeric_always_macros_expand() {
        crate::always_gt!(2, 1, "left exceeds right");
        crate::always_gt!(2, 1, "left exceeds right with details", { "detail": 1 });
        crate::always_ge!(1, 1, "left at least right");
        crate::always_ge!(1, 1, "left at least right with details", { "detail": 1 });
        crate::always_lt!(1, 2, "left below right");
        crate::always_lt!(1, 2, "left below right with details", { "detail": 1 });
        crate::always_le!(1, 1, "left at most right");
        crate::always_le!(1, 1, "left at most right with details", { "detail": 1 });
    }

    #[test]
    fn numeric_sometimes_macros_expand() {
        crate::sometimes_gt!(2, 1, "left exceeds right at least once");
        crate::sometimes_gt!(2, 1, "left exceeds right at least once with details", { "detail": 1 });
        crate::sometimes_ge!(1, 1, "left at least right at least once");
        crate::sometimes_ge!(1, 1, "left at least right at least once with details", { "detail": 1 });
        crate::sometimes_lt!(1, 2, "left below right at least once");
        crate::sometimes_lt!(1, 2, "left below right at least once with details", { "detail": 1 });
        crate::sometimes_le!(1, 1, "left at most right at least once");
        crate::sometimes_le!(1, 1, "left at most right at least once with details", { "detail": 1 });
    }

    #[test]
    fn condition_map_macros_expand() {
        crate::always_some!({ lower: 1 > 0, upper: 2 > 1 }, "some named condition holds");
        crate::always_some!(
            { lower: 1 > 0, upper: 2 > 1 },
            "some named condition holds with details",
            { "detail": 1 }
        );
        crate::sometimes_all!({ lower: 1 > 0, upper: 2 > 1 }, "each named condition holds once");
        crate::sometimes_all!(
            { lower: 1 > 0, upper: 2 > 1 },
            "each named condition holds once with details",
            { "detail": 1 }
        );
    }

    #[test]
    fn init_is_callable() {
        crate::init();
    }
}
