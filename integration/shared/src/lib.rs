use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ListenConfig {
    Http,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MeasurementConfig {
    GenericHttp,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EmitConfig {
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AssertionConfig {
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DucksConfig {
    pub listen: ListenConfig,
    pub measurements: MeasurementConfig,
    pub emit: EmitConfig,
    pub assertions: AssertionConfig,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct GenericHttpMetrics {
    pub request_count: u64,
    pub total_bytes: u64,
    pub median_entropy: f64,
    pub median_size: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DucksMessage {
    OpenPort(u16),
    Message(String),
    RequestShutdown,
    MetricsReport(GenericHttpMetrics),
}
