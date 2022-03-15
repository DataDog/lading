use crate::signals::Shutdown;
use serde::Deserialize;

pub mod tcp;

pub enum Error {
    Tcp(tcp::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Config {
    Tcp(tcp::Config),
}

pub enum Server {
    Tcp(tcp::Tcp),
}

impl Server {
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(conf, shutdown).unwrap()),
        }
    }

    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.spin().await.map_err(Error::Tcp),
        }
    }
}
