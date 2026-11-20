//! Module containing structs generated from `proto/`

/// Protobuf definitions for blackhole HTTP payload capture (see
/// `proto/blackhole_capture.proto`).
pub(crate) mod blackhole {
    /// v1 schema.
    pub(crate) mod v1 {
        #![allow(clippy::pedantic)]
        #![allow(missing_docs)]
        #![allow(unreachable_pub)]
        #![allow(dead_code)]
        include!("proto/lading.blackhole.v1.rs");
    }
}

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
        /// Stateful logs intake via gRPC
        pub(crate) mod stateful_encoding {
            #![allow(clippy::pedantic)]
            #![allow(missing_docs)]
            #![allow(unreachable_pub)]
            #![allow(dead_code)]
            #![allow(clippy::unwrap_used)]
            #![allow(clippy::enum_variant_names)]
            include!("proto/datadog.intake.stateful.rs");
        }
    }
}
