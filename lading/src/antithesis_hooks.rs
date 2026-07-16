//! Antithesis integration hooks.
//!
//! The single place lading talks to the Antithesis SDK. Functions here are
//! meaningful only when built with the `antithesis` feature, which the
//! Antithesis Docker image enables. In every other build they compile to
//! no-ops, so ordinary lading builds are unaffected.
//!
//! Property assertions do NOT belong here. All Antithesis workload and
//! property code lives under `antithesis/`, documented in
//! `antithesis/CLAUDE.md`. Drivers there either observe the SUT over the
//! network or link a SUT crate as a library and assert on its output. This
//! module is limited to the bootstrap reachability check that proves the SDK
//! path, coverage instrumentation, and assertion cataloging are all linked
//! into the SUT correctly.

// Keep the instrumentation crate linked. Only the coverage runtime references
// it, not any path called directly.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;

/// Initialize the Antithesis SDK as early as possible in process startup.
///
/// A no-op unless built with the `antithesis` feature.
pub fn init() {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();
}

/// Emit the bootstrap reachability property.
///
/// The minimal check that the SDK path is wired, required by setup. It lives
/// in a guaranteed startup path and should appear in the first Antithesis
/// run's report. The message is an inline constant literal so assertion
/// cataloging can discover it statically. A no-op unless built with the
/// `antithesis` feature.
pub fn bootstrap_reachable() {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::assert_reachable!("lading startup path executed");
}
