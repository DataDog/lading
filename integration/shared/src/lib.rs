use serde::{Deserialize, Serialize};

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
