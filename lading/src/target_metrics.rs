//! Fetch metrics from the target process
//!
//! This module allows lading to fetch metrics from the target process and
//! include them in the captures file.
//!

use serde::Deserialize;

pub mod expvar;
pub mod prometheus;

#[derive(Debug, Clone, Copy)]
/// Errors produced by [`Server`]
pub enum Error {
    /// See [`crate::target_metrics::expvar::Error`] for details.
    Expvar(expvar::Error),
    /// See [`crate::target_metrics::prometheus::Error`] for details.
    Prometheus(prometheus::Error),
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configuration for [`Server`]
pub enum Config {
    /// See [`crate::target_metrics::expvar::Config`] for details.
    Expvar(expvar::Config),
    /// See [`crate::target_metrics::prometheus::Config`] for details.
    Prometheus(prometheus::Config),
}

/// The `target_metrics` server.
#[derive(Debug)]
pub enum Server {
    /// See [`crate::target_metrics::expvar::Expvar`] for details.
    Expvar(expvar::Expvar),
    /// See [`crate::target_metrics::prometheus::Prometheus`] for details.
    Prometheus(prometheus::Prometheus),
}

impl Server {
    /// Create a new [`Server`] instance
    ///
    /// The `target_metrics::Server` is responsible for scraping metrics from
    /// the target process.
    ///
    #[must_use]
    pub fn new(
        config: Config,
        shutdown: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
    ) -> Self {
        match config {
            Config::Expvar(conf) => {
                Self::Expvar(expvar::Expvar::new(conf, shutdown, target_running))
            }
            Config::Prometheus(conf) => {
                Self::Prometheus(prometheus::Prometheus::new(conf, shutdown, target_running))
            }
        }
    }

    /// Run this [`Server`] to completion
    ///
    /// The `target_metrics` server is responsible for fetching metrics directly
    /// from the target software.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying metrics collector
    /// returns an error.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Expvar(inner) => inner.run().await.map_err(Error::Expvar),
            Server::Prometheus(inner) => inner.run().await.map_err(Error::Prometheus),
        }
    }
}
