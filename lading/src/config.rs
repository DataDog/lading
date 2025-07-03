//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use std::{net::SocketAddr, path::PathBuf, time::Duration};

use rustc_hash::FxHashMap;
use serde::Deserialize;

use crate::{blackhole, generator, inspector, observer, target, target_metrics};

/// Errors produced by [`Config`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error for a serde [`serde_yaml`].
    #[error("Failed to deserialize yaml: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
    /// Error for a socket address [`std::net::SocketAddr`].
    #[error("Failed to convert to valid SocketAddr: {0}")]
    SocketAddr(#[from] std::net::AddrParseError),
}

fn default_sample_period() -> u64 {
    1_000
}

/// Main configuration struct for this program
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The method by which to express telemetry
    #[serde(default)]
    pub telemetry: Telemetry,
    /// The generator to apply to the target in-rig
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub generator: Vec<generator::Config>,
    /// The observer that watches the target
    #[serde(skip_deserializing)]
    pub observer: observer::Config,
    /// The period on which target observations -- if any -- are made.
    #[serde(default = "default_sample_period")]
    pub sample_period_milliseconds: u64,
    /// The program being targetted by this rig
    #[serde(skip_deserializing)]
    pub target: Option<target::Config>,
    /// The blackhole to supply for the target
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub blackhole: Option<Vec<blackhole::Config>>,
    /// The `target_metrics` to scrape from the target
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub target_metrics: Option<Vec<target_metrics::Config>>,
    /// The target inspector sub-program
    pub inspector: Option<inspector::Config>,
}

/// Default value for [`Telemetry::Log::expiration`]
#[must_use]
pub fn default_expiration() -> Duration {
    Duration::MAX
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
/// Defines the manner of lading's telemetry.
pub enum Telemetry {
    /// In prometheus mode lading will emit its internal telemetry for scraping
    /// at a prometheus poll endpoint.
    Prometheus {
        /// Address and port for prometheus exporter
        addr: SocketAddr,
        /// Additional labels to include in every metric
        global_labels: FxHashMap<String, String>,
    },
    /// In prometheus socket mode lading will emit its internal telemetry for
    /// scraping on a unix socket.
    PrometheusSocket {
        /// Path of the socket for the prometheus exporter
        path: PathBuf,
        /// Additional labels to include in every metric
        global_labels: FxHashMap<String, String>,
    },
    /// In log mode lading will emit its internal telemetry to a structured log
    /// file, the "capture" file.
    Log {
        /// Location on disk to write captures
        path: PathBuf,
        /// Additional labels to include in every metric
        global_labels: FxHashMap<String, String>,
        /// The time metrics that have not been written to will take to expire.
        #[serde(default = "default_expiration")]
        expiration: Duration,
    },
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::Prometheus {
            addr: "0.0.0.0:9000"
                .parse()
                .expect("Not possible to parse to SocketAddr"),
            global_labels: FxHashMap::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use http::HeaderMap;

    use lading_payload::block;

    use super::*;

    #[test]
    fn config_deserializes() -> Result<(), Error> {
        let contents = r#"
generator:
  - id: "Data out"
    http:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      headers: {}
      target_uri: "http://localhost:1000/"
      bytes_per_second: "100 MiB"
      parallel_connections: 5
      method:
        post:
          maximum_prebuild_cache_size_bytes: "8 MiB"
          variant: "fluent"
blackhole:
  - id: "Data in"
    tcp:
      binding_addr: "127.0.0.1:1000"
  - tcp:
      binding_addr: "127.0.0.1:1001"
"#;
        let config: Config = serde_yaml::from_str(contents)?;
        assert_eq!(
            config,
            Config {
                generator: vec![generator::Config {
                    general: generator::General {
                        id: Some(String::from("Data out"))
                    },
                    inner: generator::Inner::Http(generator::http::Config {
                        seed: Default::default(),
                        target_uri: "http://localhost:1000/"
                            .try_into()
                            .expect("Failed to convert to to valid URI"),
                        method: generator::http::Method::Post {
                            variant: lading_payload::Config::Fluent,
                            maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64_with_unit(
                                8,
                                byte_unit::Unit::MiB
                            )
                            .expect("valid bytes"),
                            block_cache_method: block::CacheMethod::Fixed,
                        },
                        headers: HeaderMap::default(),
                        bytes_per_second: Some(
                            byte_unit::Byte::from_u64_with_unit(100, byte_unit::Unit::MiB)
                                .expect("valid bytes")
                        ),
                        maximum_block_size: lading_payload::block::default_maximum_block_size(),
                        parallel_connections: 5,
                        throttle: None,
                    }),
                }],
                blackhole: Some(vec![
                    blackhole::Config {
                        general: blackhole::General {
                            id: Some(String::from("Data in"))
                        },
                        inner: blackhole::Inner::Tcp(blackhole::tcp::Config {
                            binding_addr: SocketAddr::from_str("127.0.0.1:1000")?,
                        })
                    },
                    blackhole::Config {
                        general: blackhole::General { id: None },
                        inner: blackhole::Inner::Tcp(blackhole::tcp::Config {
                            binding_addr: SocketAddr::from_str("127.0.0.1:1001")?,
                        })
                    },
                ]),
                target: Option::default(),
                telemetry: crate::config::Telemetry::default(),
                observer: observer::Config::default(),
                inspector: Option::default(),
                target_metrics: Option::default(),
                sample_period_milliseconds: 1_000,
            },
        );
        Ok(())
    }
}
