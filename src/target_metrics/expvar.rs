//! Word. (todo[gh])

use std::time::Duration;

use metrics::gauge;
use serde::Deserialize;
use serde_json::Value;
use tracing::{info, trace};

use crate::signals::Shutdown;

#[derive(Debug, Clone, Copy)]
/// Errors produced by [`Expvar`]
pub enum Error {}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Configuration for collecting Go Expvar based target metrics
pub struct Config {
    /// URI to read expvars from
    uri: String,
    /// Variable names to scrape
    vars: Vec<String>,
}

/// todo[gh]
#[derive(Debug)]
pub struct Expvar {
    config: Config,
    shutdown: Shutdown,
}

impl Expvar {
    /// Create a new [`ExpVar`] instance
    ///
    /// This is responsible for scraping metrics from the target process
    /// using Go's expvar format.
    ///
    pub(crate) fn new(config: Config, shutdown: Shutdown) -> Self {
        info!("expvar created");
        Self { config, shutdown }
    }

    /// Run this [`Server`] to completion
    ///
    /// todo[gh]
    ///
    /// # Errors
    ///
    ///
    /// # Panics
    ///
    /// None are known.
    pub(crate) async fn run(self) -> Result<(), Error> {
        info!("expvar running");
        let client = reqwest::Client::new();
        loop {
            // todo!("handle shutdown");

            tokio::time::sleep(Duration::from_secs(1)).await;
            info!("expvar loop");

            let resp = if let Ok(resp) = client.get(&self.config.uri).send().await {
                resp
            } else {
                info!("failed to get expvar uri");
                continue;
            };

            let json = if let Ok(json) = resp.json::<Value>().await {
                json
            } else {
                info!("failed to parse expvar json");
                continue;
            };

            for var_name in self.config.vars.iter().cloned() {
                let val = json.pointer(&var_name).and_then(|v| v.as_f64());
                if let Some(val) = val {
                    trace!("expvar: {} = {}", var_name, val);
                    gauge!(var_name, val, "source" => "target_metrics/expvar");
                }
            }
        }
    }
}
