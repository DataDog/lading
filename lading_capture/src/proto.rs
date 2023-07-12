//! Protobuf generated structs

/// Protobuf form of a Lading capture payload. Meant for transmission and not
/// archival.
pub mod lading {
    pub mod v1 {
        include!("proto/lading_capture.proto.capture.v1.rs");
    }
}
