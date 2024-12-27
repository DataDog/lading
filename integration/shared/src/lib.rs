use integration_api::TestConfig;
use serde::{Deserialize, Serialize};
use tonic::{IntoRequest, Request};

#[allow(clippy::derive_partial_eq_without_eq)]
pub mod integration_api {
    use tonic::{IntoRequest, Request};
    use integration_api::Empty;

    tonic::include_proto!("integration_api");

    impl IntoRequest<Empty> for () {
        fn into_request(self) -> tonic::Request<Empty> {
            tonic::Request::new(Empty {})
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ListenConfig {
    /// Do not listen on any port
    None,
    /// Listen on a random port for HTTP messages
    Http,
    /// Listen on a random port for TCP messages
    Tcp,
    /// Listen on a random port for UDP messages
    Udp,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EmitConfig {
    /// Do not emit any messages
    None,
    /// Emit HTTP messages
    Http,
    /// Emit TCP messages
    Tcp,
    /// Emit UDP messages
    Udp,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AssertionConfig {
    /// Do not assert on anything
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DucksConfig {
    pub listen: ListenConfig,
    pub emit: EmitConfig,
    pub assertions: AssertionConfig,
}

impl IntoRequest<TestConfig> for DucksConfig {
    fn into_request(self) -> tonic::Request<TestConfig> {
        Request::new(TestConfig {
            json_blob: serde_json::to_string(&self).expect("Failed to convert config to JSON"),
        })
    }
}
impl From<TestConfig> for DucksConfig {
    fn from(val: TestConfig) -> Self {
        serde_json::from_str(&val.json_blob).expect("Failed to convert to JSON")
    }
}
