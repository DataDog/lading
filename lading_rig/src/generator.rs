use crate::signals::Shutdown;
use serde::Deserialize;

pub mod http;
pub mod splunk_hec;
pub mod tcp;

pub enum Error {
    Tcp(tcp::Error),
    Http(http::Error),
    SplunkHec(splunk_hec::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Config {
    Tcp(tcp::Config),
    Http(http::Config),
    SplunkHec(splunk_hec::Config),
}

pub enum Server {
    Tcp(tcp::Tcp),
    Http(http::Http),
    SplunkHec(splunk_hec::SplunkHec),
}

impl Server {
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(conf, shutdown).unwrap()),
            Config::Http(conf) => Self::Http(http::Http::new(conf, shutdown).unwrap()),
            Config::SplunkHec(conf) => {
                Self::SplunkHec(splunk_hec::SplunkHec::new(conf, shutdown).unwrap())
            }
        }
    }

    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.spin().await.map_err(Error::Tcp),
            Server::Http(inner) => inner.spin().await.map_err(Error::Http),
            Server::SplunkHec(inner) => inner.spin().await.map_err(Error::SplunkHec),
        }
    }
}
