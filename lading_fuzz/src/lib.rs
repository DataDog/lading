//! Common utilities for fuzzing in lading.

#![allow(clippy::print_stderr)]
#![deny(missing_docs)]
// Allow unused crate dependencies: cargo-fuzz --build-std adds std crates as
// dependencies that we don't use, breaks the build.
#![allow(unused_crate_dependencies)]
#![deny(warnings)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

use std::fmt::Debug;

/// Print debug information for fuzz input when FUZZ_DEBUG env var is set.
///
/// This function checks for the `FUZZ_DEBUG` environment variable and if
/// present, prints the input in debug format with header and footer lines for
/// visibility.
pub fn debug_input<T: Debug>(input: &T) {
    if std::env::var("FUZZ_DEBUG").is_ok() {
        eprintln!("=== FUZZ INPUT DEBUG ===");
        eprintln!("{input:#?}");
        eprintln!("========================");
    }
}
