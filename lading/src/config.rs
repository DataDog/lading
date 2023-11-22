//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use std::{net::SocketAddr, path::PathBuf};

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::{blackhole, generator, inspector, observer, target, target_metrics};

/// Main configuration struct for this program
#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
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
    /// The program being targetted by this rig
    #[serde(skip_deserializing, skip_serializing)]
    pub target: Option<target::Config>,
    /// The blackhole to supply for the target
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub blackhole: Option<Vec<blackhole::Config>>,
    /// The target_metrics to scrape from the target
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub target_metrics: Option<Vec<target_metrics::Config>>,
    /// The target inspector sub-program
    pub inspector: Option<inspector::Config>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
/// Defines the manner of lading's telemetry.
pub enum Telemetry {
    /// In prometheus mode lading will emit its internal telemetry for scraping
    /// at a prometheus poll endpoint.
    Prometheus {
        /// Address and port for prometheus exporter
        prometheus_addr: SocketAddr,
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
    },
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::Prometheus {
            prometheus_addr: "0.0.0.0:9000".parse().unwrap(),
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
    fn config_deserializes() {
        let contents = r#"
generator:
  - id: "Data out"
    http:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      headers: {}
      target_uri: "http://localhost:1000/"
      bytes_per_second: "100 Mb"
      parallel_connections: 5
      method:
        post:
          maximum_prebuild_cache_size_bytes: "8 Mb"
          variant: "fluent"
blackhole:
  - id: "Data in"
    tcp:
      binding_addr: "127.0.0.1:1000"
  - tcp:
      binding_addr: "127.0.0.1:1001"
"#;
        let config: Config = serde_yaml::from_str(contents).unwrap();
        assert_eq!(
            config,
            Config {
                generator: vec![generator::Config {
                    general: generator::General {
                        id: Some(String::from("Data out"))
                    },
                    inner: generator::Inner::Http(generator::http::Config {
                        seed: Default::default(),
                        target_uri: "http://localhost:1000/".try_into().unwrap(),
                        method: generator::http::Method::Post {
                            variant: lading_payload::Config::Fluent,
                            maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_unit(
                                8_f64,
                                byte_unit::ByteUnit::MB
                            )
                            .unwrap(),
                            block_cache_method: block::CacheMethod::Fixed,
                        },
                        headers: HeaderMap::default(),
                        bytes_per_second: byte_unit::Byte::from_unit(
                            100_f64,
                            byte_unit::ByteUnit::MB
                        )
                        .unwrap(),
                        block_sizes: Option::default(),
                        parallel_connections: 5,
                        throttle: lading_throttle::Config::default(),
                    }),
                }],
                blackhole: Some(vec![
                    blackhole::Config {
                        general: blackhole::General {
                            id: Some(String::from("Data in"))
                        },
                        inner: blackhole::Inner::Tcp(blackhole::tcp::Config {
                            binding_addr: SocketAddr::from_str("127.0.0.1:1000").unwrap(),
                        })
                    },
                    blackhole::Config {
                        general: blackhole::General { id: None },
                        inner: blackhole::Inner::Tcp(blackhole::tcp::Config {
                            binding_addr: SocketAddr::from_str("127.0.0.1:1001").unwrap(),
                        })
                    },
                ]),
                target: Option::default(),
                telemetry: crate::config::Telemetry::default(),
                observer: observer::Config::default(),
                inspector: Option::default(),
                target_metrics: Option::default(),
            },
        );
    }
}
