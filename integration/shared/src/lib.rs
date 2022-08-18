use integration_api::TestConfig;
use serde::{Deserialize, Serialize};
use tonic::{IntoRequest, Request};

pub mod integration_api {
    use tonic::IntoRequest;

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
            json_blob: serde_json::to_string(&self).unwrap(),
        })
    }
}

impl Into<DucksConfig> for TestConfig {
    fn into(self) -> DucksConfig {
        serde_json::from_str(&self.json_blob).unwrap()
    }
}
