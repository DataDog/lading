//! Module containing structs generated from `proto/`

/// Protobuf definitions for our `datadog` blackhole
pub(crate) mod datadog {
    /// Related to the [DataDog](https://www.datadoghq.com/) intake API
    pub(crate) mod intake {
        /// API metrics intake, v2.
        pub(crate) mod metrics {
            #![allow(clippy::pedantic)]
            #![allow(missing_docs)]
            #![allow(unreachable_pub)]
            #![allow(dead_code)]
            include!("proto/datadog.agentpayload.rs");
        }
    }
}
