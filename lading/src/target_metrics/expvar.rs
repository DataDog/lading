//! Expvar target metrics fetcher
//!
//! This module scrapes Go expvar formatted metrics from the target software.
//! The metrics are formatted as a JSON tree that is fetched over HTTP or HTTPS.

use std::time::Duration;

use metrics::gauge;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use serde_json::Value;
use tracing::{error, info, trace};

#[derive(Debug, Clone, Copy, thiserror::Error)]
/// Errors produced by [`Expvar`]
pub enum Error {
    /// Expvar scraper shut down unexpectedly
    #[error("Unexpected shutdown")]
    EarlyShutdown,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
/// Configuration for collecting Go Expvar based target metrics
pub struct Config {
    /// URI to read expvars from
    uri: String,
    /// Variable names to scrape
    vars: Vec<String>,
    /// Optional additional tags to label target metrics
    tags: Option<FxHashMap<String, String>>,
}

/// The `Expvar` target metrics implementation.
#[derive(Debug)]
pub struct Expvar {
    config: Config,
    shutdown: lading_signal::Watcher,
    experiment_started: lading_signal::Watcher,
    sample_period: Duration,
}

impl Expvar {
    /// Create a new [`ExpVar`] instance
    ///
    /// This is responsible for scraping metrics from the target process
    /// using Go's expvar format.
    ///
    pub(crate) fn new(
        config: Config,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        sample_period: Duration,
    ) -> Self {
        Self {
            config,
            shutdown,
            experiment_started,
            sample_period,
        }
    }

    /// Run this [`Server`] to completion
    ///
    /// Scrape expvars from the target at 1Hz.
    ///
    /// # Errors
    ///
    /// None are known.
    ///
    /// # Panics
    ///
    /// None are known.
    pub(crate) async fn run(self) -> Result<(), Error> {
        info!("Expvar target metrics scraper running, but waiting for warmup to complete");
        self.experiment_started.recv().await; // block until experimental lading_signal::Watcher entered
        info!(
            "Expvar target metrics scraper starting collection at {:?} interval",
            self.sample_period
        );

        // Disable certificate validation
        let client = reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("Failed to build http/https client");

        let server = async move {
            loop {
                tokio::time::sleep(self.sample_period).await;
                let resp = match client
                    .get(&self.config.uri)
                    .timeout(self.sample_period)
                    .send()
                    .await
                {
                    Ok(resp) => resp, // If successful, return the response
                    Err(err) => {
                        info!("Failed to get expvar URI: {}", err);
                        continue; // Skip the iteration on error
                    }
                };

                let json = match resp.json::<Value>().await {
                    Ok(json) => json, // Successfully parsed JSON
                    Err(err) => {
                        info!("Failed to parse expvar JSON: {}", err);
                        continue; // Skip the iteration on error
                    }
                };

                // Add lading labels including user defined tags for this endpoint
                let mut all_labels =
                    vec![("source".to_string(), "target_metrics/expvar".to_string())];
                if let Some(tags) = &self.config.tags {
                    for (tag_name, tag_val) in tags {
                        all_labels.push((tag_name.clone(), tag_val.clone()));
                    }
                }

                for var_name in &self.config.vars {
                    let val = json.pointer(var_name).and_then(serde_json::Value::as_f64);
                    if let Some(val) = val {
                        trace!("expvar: {} = {}", var_name, val);
                        gauge!(
                            format!("target/{name}", name = var_name.trim_start_matches('/'),),
                            &all_labels
                        )
                        .set(val);
                    }
                }
            }
        };

        tokio::select! {
            _res = server => {
                error!("server shutdown unexpectedly");
                 Err(Error::EarlyShutdown)
            }
            () = self.shutdown.recv() => {
                info!("shutdown signal received");
                 Ok(())
            }
        }
    }
}
