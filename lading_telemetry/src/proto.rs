//! Module containing structs generated from `proto/`
#![allow(clippy::pedantic)]

/// Protobuf versions for our telemetry
pub(crate) mod telemetry {
    /// Version One
    pub(crate) mod v1 {
        include!("proto/telemetry.v1.rs");
    }
}
