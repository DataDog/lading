// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientMessage {
    #[prost(enumeration = "ClientMessageKind", tag = "1")]
    pub kind: i32,
    /// context registration
    #[prost(string, tag = "2")]
    pub metric_name: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "3")]
    pub labels: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// signal message
    #[prost(uint32, tag = "4")]
    pub context_id: u32,
    #[prost(uint64, tag = "5")]
    pub epoch_second: u64,
    #[prost(enumeration = "ValueKind", tag = "6")]
    pub value_kind: i32,
    #[prost(double, tag = "7")]
    pub value: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ServerMessage {
    #[prost(enumeration = "ServerMessageKind", tag = "1")]
    pub kind: i32,
    /// context ack
    #[prost(uint32, tag = "2")]
    pub context_id: u32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ValueKind {
    Unspecified = 0,
    Gauge = 1,
    Counter = 2,
}
impl ValueKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ValueKind::Unspecified => "VALUE_KIND_UNSPECIFIED",
            ValueKind::Gauge => "VALUE_KIND_GAUGE",
            ValueKind::Counter => "VALUE_KIND_COUNTER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "VALUE_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "VALUE_KIND_GAUGE" => Some(Self::Gauge),
            "VALUE_KIND_COUNTER" => Some(Self::Counter),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ClientMessageKind {
    Unspecified = 0,
    ContextRegistration = 1,
    SignalMessage = 2,
}
impl ClientMessageKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ClientMessageKind::Unspecified => "CLIENT_MESSAGE_KIND_UNSPECIFIED",
            ClientMessageKind::ContextRegistration => {
                "CLIENT_MESSAGE_KIND_CONTEXT_REGISTRATION"
            }
            ClientMessageKind::SignalMessage => "CLIENT_MESSAGE_KIND_SIGNAL_MESSAGE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CLIENT_MESSAGE_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "CLIENT_MESSAGE_KIND_CONTEXT_REGISTRATION" => Some(Self::ContextRegistration),
            "CLIENT_MESSAGE_KIND_SIGNAL_MESSAGE" => Some(Self::SignalMessage),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ServerMessageKind {
    Unspecified = 0,
    ContextAck = 1,
}
impl ServerMessageKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ServerMessageKind::Unspecified => "SERVER_MESSAGE_KIND_UNSPECIFIED",
            ServerMessageKind::ContextAck => "SERVER_MESSAGE_KIND_CONTEXT_ACK",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SERVER_MESSAGE_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "SERVER_MESSAGE_KIND_CONTEXT_ACK" => Some(Self::ContextAck),
            _ => None,
        }
    }
}
