use crate::signals::Shutdown;
use serde::Deserialize;

pub mod http;
pub mod tcp;

pub enum Error {
    Tcp(tcp::Error),
    Http(http::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Config {
    Tcp(tcp::Config),
    Http(http::Config),
}

pub enum Server {
    Tcp(tcp::Tcp),
    Http(http::Http),
}

impl Server {
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(conf, shutdown).unwrap()),
            Config::Http(conf) => Self::Http(http::Http::new(conf, shutdown).unwrap()),
        }
    }

    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.spin().await.map_err(Error::Tcp),
            Server::Http(inner) => inner.spin().await.map_err(Error::Http),
        }
    }
}
