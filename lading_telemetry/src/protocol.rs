//! The telemetry protocol library

use std::collections::HashMap;

use crate::proto::telemetry;
use prost;
use prost::Message;

/// Assert that a struct has a protobuf peer
pub trait AsProto
where
    Self::Output: prost::Message,
{
    /// The protobuf peer of `Self`
    type Output;

    /// Convert self to its protobuf form.
    fn to_proto(self) -> Self::Output;
}

#[derive(Debug, Clone)]
pub enum ValueKind {
    Gauge,
    Counter,
}

impl ValueKind {
    pub fn to_proto(&self) -> telemetry::v1::ValueKind {
        match self {
            ValueKind::Gauge => telemetry::v1::ValueKind::Gauge,
            ValueKind::Counter => telemetry::v1::ValueKind::Counter,
        }
    }

    pub fn gauge() -> ValueKind {
        ValueKind::Gauge
    }

    pub fn counter() -> ValueKind {
        ValueKind::Counter
    }
}

impl From<telemetry::v1::ValueKind> for ValueKind {
    fn from(kind: telemetry::v1::ValueKind) -> Self {
        match kind {
            telemetry::v1::ValueKind::Gauge => ValueKind::Gauge,
            telemetry::v1::ValueKind::Counter => ValueKind::Counter,
            telemetry::v1::ValueKind::Unspecified => panic!("unspecified value kind"),
        }
    }
}

pub enum ClientMessageKind {
    ContextRegistration,
    SignalMessage,
}

impl ClientMessageKind {
    pub fn to_proto(&self) -> telemetry::v1::ClientMessageKind {
        match self {
            ClientMessageKind::ContextRegistration => {
                telemetry::v1::ClientMessageKind::ContextRegistration
            }
            ClientMessageKind::SignalMessage => telemetry::v1::ClientMessageKind::SignalMessage,
        }
    }

    pub fn context_registration() -> ClientMessageKind {
        ClientMessageKind::ContextRegistration
    }

    pub fn signal_message() -> ClientMessageKind {
        ClientMessageKind::SignalMessage
    }
}

impl From<telemetry::v1::ClientMessageKind> for ClientMessageKind {
    fn from(kind: telemetry::v1::ClientMessageKind) -> Self {
        match kind {
            telemetry::v1::ClientMessageKind::ContextRegistration => {
                ClientMessageKind::ContextRegistration
            }
            telemetry::v1::ClientMessageKind::SignalMessage => ClientMessageKind::SignalMessage,
            telemetry::v1::ClientMessageKind::Unspecified => {
                panic!("unspecified client message kind")
            }
        }
    }
}

pub enum ServerMessageKind {
    ContextAck,
}

impl ServerMessageKind {
    pub fn to_proto(&self) -> telemetry::v1::ServerMessageKind {
        match self {
            ServerMessageKind::ContextAck => telemetry::v1::ServerMessageKind::ContextAck,
        }
    }

    pub fn context_ack() -> ServerMessageKind {
        ServerMessageKind::ContextAck
    }
}

impl From<telemetry::v1::ServerMessageKind> for ServerMessageKind {
    fn from(kind: telemetry::v1::ServerMessageKind) -> Self {
        match kind {
            telemetry::v1::ServerMessageKind::ContextAck => ServerMessageKind::ContextAck,
            telemetry::v1::ServerMessageKind::Unspecified => {
                panic!("unspecified server message kind")
            }
        }
    }
}

/// Context registration acks
///
/// This message is sent by the server to acknowledge a context registration. It contains the
/// context ID assigned to the context, required for sending signal messages.
#[derive(Debug, Clone)]
pub struct ContextAck {
    inner: telemetry::v1::ServerMessage,
}

impl AsProto for ContextAck {
    type Output = telemetry::v1::ServerMessage;

    fn to_proto(self) -> Self::Output {
        self.inner
    }
}

impl ContextAck {
    /// Create a new `ContextAck` builder
    pub fn builder() -> ContextAckBuilder {
        ContextAckBuilder::default()
    }

    /// Return the context ID from this message    
    pub fn context_id(&self) -> u32 {
        match telemetry::v1::ServerMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ServerMessageKind::ContextAck => self.inner.context_id,
            _ => panic!("invalid server message kind"),
        }
    }
}

/// Builder for `ContextAck`
#[derive(Debug, Clone, Default)]
pub struct ContextAckBuilder {
    context_id: Option<u32>,
}

impl ContextAckBuilder {
    /// Set the context ID for the message, overwriting any existing value.
    pub fn context_id(mut self, context_id: u32) -> Self {
        self.context_id = Some(context_id);
        self
    }

    /// Build the `ContextAck` message
    ///
    /// # Panics
    ///
    /// Panics if the context ID is not set.
    pub fn build(self) -> ContextAck {
        ContextAck {
            inner: telemetry::v1::ServerMessage {
                kind: telemetry::v1::ServerMessageKind::ContextAck as i32,
                context_id: self.context_id.expect("context_id is required"),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContextRegistration {
    inner: telemetry::v1::ClientMessage,
}

impl AsProto for ContextRegistration {
    type Output = telemetry::v1::ClientMessage;

    fn to_proto(self) -> Self::Output {
        self.inner
    }
}

impl ContextRegistration {
    pub fn builder() -> ContextRegistrationBuilder {
        ContextRegistrationBuilder::default()
    }

    pub fn metric_name(&self) -> &str {
        match telemetry::v1::ClientMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ClientMessageKind::ContextRegistration => &self.inner.metric_name,
            _ => panic!("invalid client message kind"),
        }
    }

    pub fn labels(&self) -> &HashMap<String, String> {
        match telemetry::v1::ClientMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ClientMessageKind::ContextRegistration => &self.inner.labels,
            _ => panic!("invalid client message kind"),
        }
    }
}

/// Given an instance of a ContextRegistration return the hash value of it.
///
/// Example of use:
///
/// ```
/// use lading_telemetry::protocol::{AsProto, ContextRegistration, ServerMessage};
/// use std::collections::hash_map::DefaultHasher;
/// use std::hash::{Hash, Hasher};
///
/// let reg = ContextRegistration::builder()
///    .metric_name("cpu_usage")
///    .labels(&[("host", "server1")])
///    .build();
///
/// let mut hasher = DefaultHasher::new();
/// reg.hash(&mut hasher);
/// let hash = hasher.finish();
/// ```
impl std::hash::Hash for ContextRegistration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.metric_name.hash(state);
        // Loop over the key, values of the labels -- preserving order -- and
        // add them to the hash.
        let mut labels = self.inner.labels.iter().collect::<Vec<_>>();
        labels.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in labels {
            k.hash(state);
            v.hash(state);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ContextRegistrationBuilder {
    metric_name: Option<String>,
    labels: Option<std::collections::HashMap<String, String>>,
}

impl ContextRegistrationBuilder {
    pub fn metric_name<S>(mut self, metric_name: S) -> Self
    where
        S: Into<String>,
    {
        self.metric_name = Some(metric_name.into());
        self
    }

    pub fn labels<S>(mut self, labels: &[(S, S)]) -> Self
    where
        S: Into<String> + Clone,
    {
        let mut map = HashMap::new();
        for (k, v) in labels.iter() {
            let k: String = k.clone().into();
            let v: String = v.clone().into();
            map.insert(k, v);
        }

        self.labels = Some(map);
        self
    }

    pub fn build(self) -> ContextRegistration {
        ContextRegistration {
            inner: telemetry::v1::ClientMessage {
                kind: telemetry::v1::ClientMessageKind::ContextRegistration as i32,
                metric_name: self.metric_name.expect("metric_name is required"),
                labels: self.labels.expect("labels are required"),
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignalMessage {
    inner: telemetry::v1::ClientMessage,
}

impl AsProto for SignalMessage {
    type Output = telemetry::v1::ClientMessage;

    fn to_proto(self) -> Self::Output {
        self.inner
    }
}

impl SignalMessage {
    pub fn builder() -> SignalMessageBuilder {
        SignalMessageBuilder::default()
    }

    pub fn context_id(&self) -> u32 {
        match telemetry::v1::ClientMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ClientMessageKind::SignalMessage => self.inner.context_id,
            _ => panic!("invalid client message kind"),
        }
    }

    pub fn epoch_second(&self) -> u64 {
        match telemetry::v1::ClientMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ClientMessageKind::SignalMessage => self.inner.epoch_second,
            _ => panic!("invalid client message kind"),
        }
    }

    pub fn value_kind(&self) -> ValueKind {
        match telemetry::v1::ClientMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ClientMessageKind::SignalMessage => {
                telemetry::v1::ValueKind::try_from(self.inner.value_kind)
                    .unwrap()
                    .into()
            }
            _ => panic!("invalid client message kind"),
        }
    }

    pub fn value(&self) -> f64 {
        match telemetry::v1::ClientMessageKind::try_from(self.inner.kind).unwrap() {
            telemetry::v1::ClientMessageKind::SignalMessage => self.inner.value,
            _ => panic!("invalid client message kind"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignalMessageBuilder {
    context_id: Option<u32>,
    epoch_second: Option<u64>,
    value_kind: Option<ValueKind>,
    value: Option<f64>,
}

impl SignalMessageBuilder {
    pub fn context_id(mut self, context_id: u32) -> Self {
        self.context_id = Some(context_id);
        self
    }

    pub fn epoch_second(mut self, epoch_second: u64) -> Self {
        self.epoch_second = Some(epoch_second);
        self
    }

    pub fn value_kind(mut self, value_kind: ValueKind) -> Self {
        self.value_kind = Some(value_kind);
        self
    }

    pub fn value(mut self, value: f64) -> Self {
        self.value = Some(value);
        self
    }

    pub fn build(self) -> SignalMessage {
        SignalMessage {
            inner: telemetry::v1::ClientMessage {
                kind: telemetry::v1::ClientMessageKind::SignalMessage as i32,
                context_id: self.context_id.expect("context_id is required"),
                epoch_second: self.epoch_second.expect("epoch_second is required"),
                value_kind: self.value_kind.expect("value_kind is required").to_proto() as i32,
                value: self.value.expect("value is required"),
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientMessage {
    ContextRegistration(ContextRegistration),
    SignalMessage(SignalMessage),
}

impl ClientMessage {
    pub fn decode(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        let message = telemetry::v1::ClientMessage::decode(bytes)?;
        match telemetry::v1::ClientMessageKind::try_from(message.kind)? {
            telemetry::v1::ClientMessageKind::ContextRegistration => {
                Ok(ClientMessage::ContextRegistration(ContextRegistration {
                    inner: message,
                }))
            }
            telemetry::v1::ClientMessageKind::SignalMessage => {
                Ok(ClientMessage::SignalMessage(SignalMessage {
                    inner: message,
                }))
            }
            telemetry::v1::ClientMessageKind::Unspecified => {
                panic!("unspecified client message kind")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ServerMessage {
    ContextAck(ContextAck),
}

impl ServerMessage {
    pub fn decode(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        let message = telemetry::v1::ServerMessage::decode(bytes)?;
        match telemetry::v1::ServerMessageKind::try_from(message.kind)? {
            telemetry::v1::ServerMessageKind::ContextAck => {
                Ok(ServerMessage::ContextAck(ContextAck { inner: message }))
            }
            telemetry::v1::ServerMessageKind::Unspecified => {
                panic!("unspecified server message kind")
            }
        }
    }
}
