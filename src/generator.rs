//! Lading generators
//!
//! The lading generator is responsible for pushing load into the target
//! sub-process via a small handful of protocols, the variants of
//! [`Server`]. Each generator variant works in the same basic way: a block of
//! payloads are pre-computed at generator start time which are then spammed
//! into the target according to a user-defined rate limit in a cyclic
//! manner. That is, we avoid runtime delays in payload generation by, well,
//! building a lot of payloads in one shot and rotating through them
//! indefinately, paying higher memory and longer startup for better
//! experimental control.

use serde::Deserialize;

use crate::signals::Shutdown;

pub mod file_gen;
pub mod http;
pub mod kafka;
pub mod splunk_hec;
pub mod tcp;

#[derive(Debug)]
/// Errors produced by [`Server`].
pub enum Error {
    /// See [`crate::generator::tcp::Error`] for details.
    Tcp(tcp::Error),
    /// See [`crate::generator::http::Error`] for details.
    Http(http::Error),
    /// See [`crate::generator::splunk_hec::Error`] for details.
    SplunkHec(splunk_hec::Error),
    /// See [`crate::generator::kafka::Error`] for details.
    Kafka(kafka::Error),
    /// See [`crate::generator::file_gen::Error`] for details.
    FileGen(file_gen::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Configuration for [`Server`]
pub enum Config {
    /// See [`crate::generator::tcp::Config`] for details.
    Tcp(tcp::Config),
    /// See [`crate::generator::http::Config`] for details.
    Http(http::Config),
    /// See [`crate::generator::splunk_hec::Config`] for details.
    SplunkHec(splunk_hec::Config),
    /// See [`crate::generator::kafka::Config`] for details.
    Kafka(kafka::Config),
    /// See [`crate::generator::file_gen::Config`] for details.
    FileGen(file_gen::Config),
}

#[derive(Debug)]
/// The generator server.
///
/// All generators supported by lading are a variant of this enum. Please see
/// variant documentation for details.
pub enum Server {
    /// See [`crate::generator::tcp::Tcp`] for details.
    Tcp(tcp::Tcp),
    /// See [`crate::generator::http::Http`] for details.
    Http(http::Http),
    /// See [`crate::generator::splunk_hec::SplunkHec`] for details.
    SplunkHec(splunk_hec::SplunkHec),
    /// See [`crate::generator::kafka::Kafka`] for details.
    Kafka(kafka::Kafka),
    /// See [`crate::generator::file_gen::FileGen`] for details.
    FileGen(file_gen::FileGen),
}

impl Server {
    /// Create a new [`Server`]
    ///
    /// This function creates a new [`Server`] instance, deferring to the
    /// underlying sub-server.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server creation
    /// signals error.
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let srv = match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(&conf, shutdown).map_err(Error::Tcp)?),
            Config::Http(conf) => Self::Http(http::Http::new(conf, shutdown).map_err(Error::Http)?),
            Config::SplunkHec(conf) => Self::SplunkHec(
                splunk_hec::SplunkHec::new(conf, shutdown).map_err(Error::SplunkHec)?,
            ),
            Config::Kafka(conf) => {
                Self::Kafka(kafka::Kafka::new(conf, shutdown).map_err(Error::Kafka)?)
            }
            Config::FileGen(conf) => {
                Self::FileGen(file_gen::FileGen::new(conf, shutdown).map_err(Error::FileGen)?)
            }
        };
        Ok(srv)
    }

    /// Run this [`Server`] to completion
    ///
    /// This function runs the sub-server its completion, or until a shutdown
    /// signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server signals
    /// error.
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.spin().await.map_err(Error::Tcp),
            Server::Http(inner) => inner.spin().await.map_err(Error::Http),
            Server::SplunkHec(inner) => inner.spin().await.map_err(Error::SplunkHec),
            Server::Kafka(inner) => inner.spin().await.map_err(Error::Kafka),
            Server::FileGen(inner) => inner.spin().await.map_err(Error::FileGen),
        }
    }
}
