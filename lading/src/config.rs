//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use std::{
    fs,
    io::{self, ErrorKind},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use rustc_hash::{FxHashMap, FxHashSet};
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
    /// Error for IO operations when reading config directory
    #[error("Failed to read config directory: {0}")]
    Io(#[from] std::io::Error),
    /// Error for duplicate IDs in generators
    #[error("Duplicate generator ID found: {0}")]
    DuplicateGeneratorId(String),
    /// Error for duplicate IDs in blackholes
    #[error("Duplicate blackhole ID found: {0}")]
    DuplicateBlackholeId(String),
    /// Error when no config files found in directory
    #[error("No .yaml config files found in directory: {0}")]
    NoConfigFiles(PathBuf),
    /// Error when telemetry is defined in multiple config files
    #[error("Telemetry cannot be defined in multiple config files")]
    ConflictingTelemetry,
    /// Error when `sample_period_milliseconds` is defined in multiple config files
    #[error("sample_period_milliseconds cannot be defined in multiple config files")]
    ConflictingSamplePeriod,
    /// Error when inspector is defined in multiple config files
    #[error("Inspector cannot be defined in multiple config files")]
    ConflictingInspector,
    /// Error getting metadata for config path
    #[error("Failed to get metadata for config path {path:?}: {source}")]
    Metadata {
        /// Config path
        path: PathBuf,
        /// Underlying IO error
        #[source]
        source: Box<io::Error>,
    },
    /// Error reading config file
    #[error("Failed to read config file {path:?}: {source}")]
    ReadFile {
        /// File path
        path: PathBuf,
        /// Underlying IO error
        #[source]
        source: Box<io::Error>,
    },
    /// Error reading directory entries
    #[error("Failed to read directory entries from {path:?}: {source}")]
    ReadDir {
        /// Directory path
        path: PathBuf,
        /// Underlying IO error
        #[source]
        source: Box<io::Error>,
    },
    /// Error reading directory entry
    #[error("Failed to read directory entry in {dir:?}: {source}")]
    ReadDirEntry {
        /// Directory path
        dir: PathBuf,
        /// Underlying IO error
        #[source]
        source: Box<io::Error>,
    },
}

fn default_sample_period() -> u64 {
    1_000
}

/// Main configuration struct for this program
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The method by which to express telemetry
    pub telemetry: Option<Telemetry>,
    /// The generator to apply to the target in-rig
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub generator: Vec<generator::Config>,
    /// The observer that watches the target
    #[serde(default)]
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
    pub blackhole: Vec<blackhole::Config>,
    /// The `target_metrics` to scrape from the target
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub target_metrics: Option<Vec<target_metrics::Config>>,
    /// The target inspector sub-program
    pub inspector: Option<inspector::Config>,
}

/// Partial configuration used for merging multiple config files
///
/// All fields are optional -- or admit an empty construction -- to allow
/// partial configs that only specify certain sections. This is used internally
/// for directory-based config overlay.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct PartialConfig {
    /// The method by which to express telemetry.
    pub telemetry: Option<Telemetry>,
    /// The generator to apply to the target in-rig.
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub generator: Vec<generator::Config>,
    /// The observer that watches the target.
    #[serde(default)]
    pub observer: observer::Config,
    /// The period on which target observations are made.
    pub sample_period_milliseconds: Option<u64>,
    /// The blackhole to supply for the target.
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub blackhole: Vec<blackhole::Config>,
    /// The `target_metrics` to scrape from the target.
    #[serde(default)]
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub target_metrics: Option<Vec<target_metrics::Config>>,
    /// The target inspector sub-program.
    pub inspector: Option<inspector::Config>,
}

/// Default value for [`Telemetry::Log`] expiration
#[must_use]
pub fn default_expiration() -> Duration {
    Duration::MAX
}

/// Default value for flush seconds
#[must_use]
pub fn default_flush_seconds() -> u64 {
    60
}

/// Default value for zstd compression level
#[must_use]
pub fn default_compression_level() -> i32 {
    3
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Output format for capture files
pub enum CaptureFormat {
    /// JSON Lines format - one JSON object per line
    Jsonl {
        /// Number of seconds to buffer before flushing to disk
        #[serde(default = "default_flush_seconds")]
        flush_seconds: u64,
    },
    /// Multiple formats simultaneously, currently JSONL and Parquet
    Multi {
        /// Number of seconds to buffer before flushing both formats
        #[serde(default = "default_flush_seconds")]
        flush_seconds: u64,
        /// Zstd compression level for Parquet (1-22, default: 3)
        #[serde(default = "default_compression_level")]
        compression_level: i32,
    },
    /// Apache Parquet columnar format
    Parquet {
        /// Number of seconds to buffer before writing row group
        #[serde(default = "default_flush_seconds")]
        flush_seconds: u64,
        /// Zstd compression level (1-22, default: 3)
        #[serde(default = "default_compression_level")]
        compression_level: i32,
    },
}

impl Default for CaptureFormat {
    fn default() -> Self {
        Self::Jsonl {
            flush_seconds: default_flush_seconds(),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
/// Defines the manner of lading's telemetry.
pub enum Telemetry {
    /// In prometheus mode lading will emit its internal telemetry for scraping
    /// at a prometheus poll endpoint.
    Prometheus {
        /// Address and port for prometheus exporter
        addr: SocketAddr,
        /// Additional labels to include in every metric
        #[serde(default)]
        global_labels: FxHashMap<String, String>,
    },
    /// In log mode lading will emit its internal telemetry to a structured log
    /// file, the "capture" file.
    Log {
        /// Location on disk to write captures
        path: PathBuf,
        /// Additional labels to include in every metric
        #[serde(default)]
        global_labels: FxHashMap<String, String>,
        /// The time metrics that have not been written to will take to expire.
        #[serde(default = "default_expiration")]
        expiration: Duration,
        /// Output format for the capture file
        #[serde(default)]
        format: CaptureFormat,
    },
    /// In prometheus socket mode lading will emit its internal telemetry for
    /// scraping on a unix socket.
    PrometheusSocket {
        /// Path of the socket for the prometheus exporter
        path: PathBuf,
        /// Additional labels to include in every metric
        #[serde(default)]
        global_labels: FxHashMap<String, String>,
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

impl Config {
    /// Create a Config from a `PartialConfig`, using defaults for missing fields
    ///
    /// # Errors
    ///
    /// Returns an error if duplicate IDs are found in generators or blackholes.
    pub fn from_partial(partial: PartialConfig) -> Result<Self, Error> {
        // Check for duplicate IDs in generators
        check_duplicate_generator_ids(&partial.generator)?;

        // Check for duplicate IDs in blackholes
        check_duplicate_blackhole_ids(&partial.blackhole)?;

        Ok(Self {
            telemetry: partial.telemetry,
            generator: partial.generator,
            observer: partial.observer,
            sample_period_milliseconds: partial
                .sample_period_milliseconds
                .unwrap_or_else(default_sample_period),
            target: None,
            blackhole: partial.blackhole,
            target_metrics: partial.target_metrics,
            inspector: partial.inspector,
        })
    }

    /// Merge two `PartialConfig` instancestogether
    ///
    /// Partial configs are composed with one another. The merge semantics are as follows:
    ///
    /// * Singletons (telemetry, `sample_period`, inspector) are only allowed once
    ///   in ANY configs. Error otherwise.
    /// * Arrays (generator, blackhole, `target_metrics`) are all appended. If IDs
    ///   are duplicated that is an error.
    ///
    /// Error conditions assert config files are disjoint. Ordering of config
    /// file reads does not matter.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Any singleton field is defined in multiple partials
    /// * Duplicate IDs are found in generators or blackholes
    fn merge_partial(
        mut base: PartialConfig,
        overlay: PartialConfig,
    ) -> Result<PartialConfig, Error> {
        if base.telemetry.is_some() && overlay.telemetry.is_some() {
            return Err(Error::ConflictingTelemetry);
        }
        if overlay.telemetry.is_some() {
            base.telemetry = overlay.telemetry;
        }

        if base.sample_period_milliseconds.is_some() && overlay.sample_period_milliseconds.is_some()
        {
            return Err(Error::ConflictingSamplePeriod);
        }
        if overlay.sample_period_milliseconds.is_some() {
            base.sample_period_milliseconds = overlay.sample_period_milliseconds;
        }

        if base.inspector.is_some() && overlay.inspector.is_some() {
            return Err(Error::ConflictingInspector);
        }
        if overlay.inspector.is_some() {
            base.inspector = overlay.inspector;
        }

        // Merge generators
        base.generator.extend(overlay.generator);
        check_duplicate_generator_ids(&base.generator)?;

        // Merge blackholes
        base.blackhole.extend(overlay.blackhole);
        check_duplicate_blackhole_ids(&base.blackhole)?;

        // Merge target_metrics, deduplicating entries
        if let Some(overlay_metrics) = overlay.target_metrics {
            if let Some(ref mut existing) = base.target_metrics {
                for metric in overlay_metrics {
                    if !existing.contains(&metric) {
                        existing.push(metric);
                    }
                }
            } else {
                base.target_metrics = Some(overlay_metrics);
            }
        }

        Ok(base)
    }
}

/// Check for duplicate IDs in generator configs
fn check_duplicate_generator_ids(generators: &[generator::Config]) -> Result<(), Error> {
    let mut seen_ids = FxHashSet::default();
    for generator_config in generators {
        if let Some(ref id) = generator_config.general.id
            && !seen_ids.insert(id.clone())
        {
            return Err(Error::DuplicateGeneratorId(id.clone()));
        }
    }
    Ok(())
}

/// Check for duplicate IDs in blackhole configs
fn check_duplicate_blackhole_ids(blackholes: &[blackhole::Config]) -> Result<(), Error> {
    let mut seen_ids = FxHashSet::default();
    for bh in blackholes {
        if let Some(ref id) = bh.general.id
            && !seen_ids.insert(id.clone())
        {
            return Err(Error::DuplicateBlackholeId(id.clone()));
        }
    }
    Ok(())
}

/// Load configuration from a path (file or directory)
///
/// If the path points to a file, loads and parses that single config. If the
/// path points to a directory, composes all .yaml files by combining
/// them. Configs must be strictly disjoint. Load order doesn't affect the
/// result.
///
/// # Errors
///
/// Returns an error if:
///
/// * Path does not exist or cannot be read
/// * No .yaml files found in directory
/// * Config files contain invalid YAML
/// * Any singleton field defined in multiple files
/// * Duplicate IDs found in generators or blackholes
pub fn load_config_from_path(path: &Path) -> Result<Config, Error> {
    let metadata = fs::metadata(path).map_err(|source| Error::Metadata {
        path: path.to_path_buf(),
        source: Box::new(source),
    })?;

    if metadata.is_file() {
        // Single file, read only the yaml file and nothing else
        let contents = fs::read_to_string(path).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        serde_yaml::from_str(&contents).map_err(Error::from)
    } else if metadata.is_dir() {
        // Directory, merge all configs found in this directory
        load_directory_configs(path)
    } else {
        Err(Error::Io(io::Error::new(
            ErrorKind::InvalidInput,
            "Path is neither a file nor a directory",
        )))
    }
}

/// Load and compose all .yaml config files from a directory
///
/// # Errors
///
/// Returns an error if:
///
/// * Directory cannot be read
/// * No .yaml files found
/// * Any config file is invalid
/// * Any singleton field defined in multiple files
/// * Duplicate IDs found in generators or blackholes
fn load_directory_configs(dir: &Path) -> Result<Config, Error> {
    let mut merged_partial: Option<PartialConfig> = None;
    let mut found_any = false;

    // Process directory entries one at a time, accumulating as we go
    for entry in fs::read_dir(dir).map_err(|source| Error::ReadDir {
        path: dir.to_path_buf(),
        source: Box::new(source),
    })? {
        let entry = entry.map_err(|source| Error::ReadDirEntry {
            dir: dir.to_path_buf(),
            source: Box::new(source),
        })?;
        let path = entry.path();

        let is_yaml_file = path.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "yaml")
            && !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with('.'));

        if !is_yaml_file {
            continue;
        }

        found_any = true;

        let contents = fs::read_to_string(&path).map_err(|source| Error::ReadFile {
            path: path.clone(),
            source: Box::new(source),
        })?;
        let partial: PartialConfig = serde_yaml::from_str(&contents)?;

        merged_partial = Some(match merged_partial {
            None => partial,
            Some(acc) => Config::merge_partial(acc, partial)?,
        });
    }

    if !found_any {
        return Err(Error::NoConfigFiles(dir.to_path_buf()));
    }

    // Convert final merged PartialConfig to Config
    Config::from_partial(merged_partial.expect("found_any ensures this is Some"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::str::FromStr;

    use http::HeaderMap;
    use proptest::{prop_assert, prop_assert_eq, proptest};

    use lading_payload::block;

    use super::*;
    use crate::target::Behavior;

    /// Create a TCP generator config with the given ID and port
    fn make_tcp_generator(id: Option<&str>, port: u16) -> generator::Config {
        generator::Config {
            general: generator::General {
                id: id.map(String::from),
            },
            inner: generator::Inner::Tcp(generator::tcp::Config {
                seed: [0; 32],
                addr: format!("127.0.0.1:{port}").parse().unwrap(),
                variant: lading_payload::Config::Ascii,
                bytes_per_second: None,
                maximum_block_size: block::default_maximum_block_size(),
                maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64_with_unit(
                    8,
                    byte_unit::Unit::MiB,
                )
                .expect("valid bytes"),
                parallel_connections: 1,
                throttle: None,
            }),
        }
    }

    /// Create a TCP blackhole config with the given ID and port
    fn make_tcp_blackhole(id: Option<&str>, port: u16) -> blackhole::Config {
        blackhole::Config {
            general: blackhole::General {
                id: id.map(String::from),
            },
            inner: blackhole::Inner::Tcp(blackhole::tcp::Config {
                binding_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            }),
        }
    }

    /// Create a minimal PartialConfig with just generators
    fn make_partial_with_generators(generators: Vec<generator::Config>) -> PartialConfig {
        PartialConfig {
            telemetry: None,
            generator: generators,
            observer: observer::Config::default(),
            sample_period_milliseconds: None,
            blackhole: vec![],
            target_metrics: None,
            inspector: None,
        }
    }

    /// Create a minimal PartialConfig with just blackholes
    fn make_partial_with_blackholes(blackholes: Vec<blackhole::Config>) -> PartialConfig {
        PartialConfig {
            telemetry: None,
            generator: vec![],
            observer: observer::Config::default(),
            sample_period_milliseconds: None,
            blackhole: blackholes,
            target_metrics: None,
            inspector: None,
        }
    }

    /// Create a minimal PartialConfig with just target_metrics
    fn make_partial_with_target_metrics(metrics: Vec<target_metrics::Config>) -> PartialConfig {
        PartialConfig {
            telemetry: None,
            generator: vec![],
            observer: observer::Config::default(),
            sample_period_milliseconds: None,
            blackhole: vec![],
            target_metrics: Some(metrics),
            inspector: None,
        }
    }

    /// Create a prometheus target_metrics config
    fn make_prometheus_target_metrics(uri: &str) -> target_metrics::Config {
        target_metrics::Config::Prometheus(target_metrics::prometheus::Config {
            uri: uri.to_string(),
            metrics: None,
            tags: None,
        })
    }

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
                blackhole: vec![
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
                ],
                target: Option::default(),
                telemetry: None,
                observer: observer::Config::default(),
                inspector: Option::default(),
                target_metrics: Option::default(),
                sample_period_milliseconds: 1_000,
            },
        );
        Ok(())
    }

    #[test]
    fn merge_appends_generators() -> Result<(), Error> {
        let base_partial =
            make_partial_with_generators(vec![make_tcp_generator(Some("gen1"), 8080)]);

        let overlay = make_partial_with_generators(vec![make_tcp_generator(Some("gen2"), 8081)]);

        let merged = Config::merge_partial(base_partial, overlay)?;
        assert_eq!(merged.generator.len(), 2);

        // Extract IDs - verify both generators are present without assuming order
        let ids: Vec<Option<String>> = merged
            .generator
            .iter()
            .map(|g| g.general.id.clone())
            .collect();

        assert!(ids.contains(&Some("gen1".to_string())));
        assert!(ids.contains(&Some("gen2".to_string())));
        Ok(())
    }

    #[test]
    fn merge_rejects_conflicting_telemetry() -> Result<(), Error> {
        let base_partial = PartialConfig {
            telemetry: Some(Telemetry::Prometheus {
                addr: "0.0.0.0:9000".parse()?,
                global_labels: FxHashMap::default(),
            }),
            ..Default::default()
        };

        let overlay = PartialConfig {
            telemetry: Some(Telemetry::Log {
                path: "/tmp/capture.jsonl".into(),
                global_labels: FxHashMap::default(),
                expiration: Duration::MAX,
                format: CaptureFormat::default(),
            }),
            ..Default::default()
        };

        let result = Config::merge_partial(base_partial, overlay);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::ConflictingTelemetry)));
        Ok(())
    }

    #[test]
    fn merge_rejects_conflicting_sample_period() {
        let base_partial = PartialConfig {
            sample_period_milliseconds: Some(2000),
            ..Default::default()
        };

        let overlay = PartialConfig {
            sample_period_milliseconds: Some(5000),
            ..Default::default()
        };

        let result = Config::merge_partial(base_partial, overlay);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::ConflictingSamplePeriod)));
    }

    #[test]
    fn load_single_file_works() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        let config_path = temp_dir.path().join("config.yaml");

        let config_content = r#"
generator:
  - tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8080"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut file = fs::File::create(&config_path)?;
        file.write_all(config_content.as_bytes())?;

        let config = load_config_from_path(&config_path)?;
        assert_eq!(config.generator.len(), 1);
        Ok(())
    }

    #[test]
    fn load_directory_merges_configs() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;

        // Create first config file
        let config1_path = temp_dir.path().join("01-base.yaml");
        let config1_content = r#"
generator:
  - id: "gen1"
    tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8080"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut file1 = fs::File::create(&config1_path)?;
        file1.write_all(config1_content.as_bytes())?;

        // Create second config file
        let config2_path = temp_dir.path().join("02-overlay.yaml");
        let config2_content = r#"
generator:
  - id: "gen2"
    tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8081"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut file2 = fs::File::create(&config2_path)?;
        file2.write_all(config2_content.as_bytes())?;

        let config = load_config_from_path(temp_dir.path())?;
        assert_eq!(config.generator.len(), 2);

        // Extract IDs - order doesn't matter with strict composition
        let ids: Vec<Option<String>> = config
            .generator
            .iter()
            .map(|g| g.general.id.clone())
            .collect();

        assert!(ids.contains(&Some("gen1".to_string())));
        assert!(ids.contains(&Some("gen2".to_string())));
        Ok(())
    }

    #[test]
    fn load_directory_skips_hidden_files() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;

        // Create visible config file
        let config1_path = temp_dir.path().join("config.yaml");
        let config1_content = r#"
generator:
  - id: "gen1"
    tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8080"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut file1 = fs::File::create(&config1_path)?;
        file1.write_all(config1_content.as_bytes())?;

        // Create hidden config file that should be skipped
        let hidden_path = temp_dir.path().join(".hidden.yaml");
        let hidden_content = r#"
generator:
  - id: "gen2"
    tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8081"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut hidden_file = fs::File::create(&hidden_path)?;
        hidden_file.write_all(hidden_content.as_bytes())?;

        let config = load_config_from_path(temp_dir.path())?;
        assert_eq!(config.generator.len(), 1);
        // Only one generator exists, so direct index access is safe after len check
        assert_eq!(config.generator[0].general.id, Some("gen1".to_string()));
        Ok(())
    }

    #[test]
    fn load_directory_merges_blackholes() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;

        // Create first config file with blackhole
        let config1_path = temp_dir.path().join("01-base.yaml");
        let config1_content = r#"
blackhole:
  - id: "bh1"
    tcp:
      binding_addr: "127.0.0.1:9000"
"#;
        let mut file1 = fs::File::create(&config1_path)?;
        file1.write_all(config1_content.as_bytes())?;

        // Create second config file with another blackhole
        let config2_path = temp_dir.path().join("02-overlay.yaml");
        let config2_content = r#"
blackhole:
  - id: "bh2"
    tcp:
      binding_addr: "127.0.0.1:9001"
"#;
        let mut file2 = fs::File::create(&config2_path)?;
        file2.write_all(config2_content.as_bytes())?;

        let config = load_config_from_path(temp_dir.path())?;
        assert_eq!(config.blackhole.len(), 2);

        // Extract IDs - order doesn't matter with strict composition
        let ids: Vec<Option<String>> = config
            .blackhole
            .iter()
            .map(|b| b.general.id.clone())
            .collect();

        assert!(ids.contains(&Some("bh1".to_string())));
        assert!(ids.contains(&Some("bh2".to_string())));
        Ok(())
    }

    #[test]
    fn merge_appends_blackholes() -> Result<(), Error> {
        let base_partial =
            make_partial_with_blackholes(vec![make_tcp_blackhole(Some("bh1"), 9000)]);
        let overlay = make_partial_with_blackholes(vec![make_tcp_blackhole(Some("bh2"), 9001)]);

        let merged = Config::merge_partial(base_partial, overlay)?;
        assert_eq!(merged.blackhole.len(), 2);

        // Extract IDs - verify both blackholes are present without assuming order
        let ids: Vec<Option<String>> = merged
            .blackhole
            .iter()
            .map(|b| b.general.id.clone())
            .collect();

        assert!(ids.contains(&Some("bh1".to_string())));
        assert!(ids.contains(&Some("bh2".to_string())));
        Ok(())
    }

    #[test]
    fn merge_rejects_conflicting_inspector() {
        let base_partial = PartialConfig {
            inspector: Some(inspector::Config {
                command: "/bin/inspector1".into(),
                arguments: vec![],
                environment_variables: FxHashMap::default(),
                output: crate::target::Output {
                    stdout: Behavior::Quiet,
                    stderr: Behavior::Quiet,
                },
            }),
            ..Default::default()
        };

        let overlay = PartialConfig {
            inspector: Some(inspector::Config {
                command: "/bin/inspector2".into(),
                arguments: vec![],
                environment_variables: FxHashMap::default(),
                output: crate::target::Output {
                    stdout: Behavior::Quiet,
                    stderr: Behavior::Quiet,
                },
            }),
            ..Default::default()
        };

        let result = Config::merge_partial(base_partial, overlay);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::ConflictingInspector)));
    }

    #[test]
    fn load_directory_rejects_duplicate_ids() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;

        let config1_path = temp_dir.path().join("01-base.yaml");
        let config1_content = r#"
generator:
  - id: "duplicate"
    tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8080"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut file1 = fs::File::create(&config1_path)?;
        file1.write_all(config1_content.as_bytes())?;

        let config2_path = temp_dir.path().join("02-overlay.yaml");
        let config2_content = r#"
generator:
  - id: "duplicate"
    tcp:
      seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
      addr: "127.0.0.1:8081"
      variant: "ascii"
      maximum_prebuild_cache_size_bytes: "8 MiB"
"#;
        let mut file2 = fs::File::create(&config2_path)?;
        file2.write_all(config2_content.as_bytes())?;

        let result = load_config_from_path(temp_dir.path());
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::DuplicateGeneratorId(_))));
        Ok(())
    }

    #[test]
    fn empty_directory_returns_error() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        let result = load_config_from_path(temp_dir.path());
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::NoConfigFiles(_))));
        Ok(())
    }

    proptest! {
        #[test]
        fn merge_with_empty_is_identity(
            num_generators in 0_usize..5,
            base_port in 8000_u16..8100_u16,
        ) {
            // Create a PartialConfig with some generators
            let generators: Vec<_> = (0..num_generators)
                .map(|i| make_tcp_generator(Some(&format!("gen{i}")), base_port + i as u16))
                .collect();
            let partial = make_partial_with_generators(generators);

            // Merge with empty PartialConfig
            let empty = PartialConfig::default();
            let merged = Config::merge_partial(partial, empty)
                .expect("merge with empty should succeed");

            // Should be equivalent to original
            prop_assert_eq!(merged.generator.len(), num_generators);
            for i in 0..num_generators {
                let expected_id = format!("gen{i}");
                prop_assert!(merged.generator.iter().any(|g| g.general.id == Some(expected_id.clone())));
            }
        }

        #[test]
        fn merge_commutativity_for_disjoint_configs(
            num_a in 1_usize..4,
            num_b in 1_usize..4,
        ) {
            // Create two disjoint sets of generators (different IDs, different ports)
            let gens_a: Vec<_> = (0..num_a)
                .map(|i| make_tcp_generator(Some(&format!("a{i}")), 8000 + i as u16))
                .collect();
            let gens_b: Vec<_> = (0..num_b)
                .map(|i| make_tcp_generator(Some(&format!("b{i}")), 9000 + i as u16))
                .collect();

            let partial_a = make_partial_with_generators(gens_a);
            let partial_b = make_partial_with_generators(gens_b);

            // Merge both ways using Clone
            let ab = Config::merge_partial(partial_a.clone(), partial_b.clone())
                .expect("merge should succeed");
            let ba = Config::merge_partial(partial_b, partial_a)
                .expect("merge should succeed");

            // Should have same number of generators
            prop_assert_eq!(ab.generator.len(), ba.generator.len());
            prop_assert_eq!(ab.generator.len(), num_a + num_b);

            // Both should contain all IDs (order doesn't matter)
            for i in 0..num_a {
                let id = Some(format!("a{i}"));
                prop_assert!(ab.generator.iter().any(|g| g.general.id == id));
                prop_assert!(ba.generator.iter().any(|g| g.general.id == id));
            }
            for i in 0..num_b {
                let id = Some(format!("b{i}"));
                prop_assert!(ab.generator.iter().any(|g| g.general.id == id));
                prop_assert!(ba.generator.iter().any(|g| g.general.id == id));
            }
        }

        #[test]
        fn merge_associativity(
            num_a in 1_usize..3,
            num_b in 1_usize..3,
            num_c in 1_usize..3,
        ) {
            // Create three disjoint sets of generators
            let gens_a: Vec<_> = (0..num_a)
                .map(|i| make_tcp_generator(Some(&format!("a{i}")), 8000 + i as u16))
                .collect();
            let gens_b: Vec<_> = (0..num_b)
                .map(|i| make_tcp_generator(Some(&format!("b{i}")), 9000 + i as u16))
                .collect();
            let gens_c: Vec<_> = (0..num_c)
                .map(|i| make_tcp_generator(Some(&format!("c{i}")), 10000 + i as u16))
                .collect();

            let partial_a = make_partial_with_generators(gens_a);
            let partial_b = make_partial_with_generators(gens_b);
            let partial_c = make_partial_with_generators(gens_c);

            // merge(merge(A, B), C)
            let ab = Config::merge_partial(partial_a.clone(), partial_b.clone())
                .expect("merge A+B should succeed");
            let abc_left = Config::merge_partial(ab, partial_c.clone())
                .expect("merge (A+B)+C should succeed");

            // merge(A, merge(B, C))
            let bc = Config::merge_partial(partial_b, partial_c)
                .expect("merge B+C should succeed");
            let abc_right = Config::merge_partial(partial_a, bc)
                .expect("merge A+(B+C) should succeed");

            // Should have same number of generators
            prop_assert_eq!(abc_left.generator.len(), abc_right.generator.len());
            prop_assert_eq!(abc_left.generator.len(), num_a + num_b + num_c);

            // Both should contain all IDs
            for i in 0..num_a {
                let id = Some(format!("a{i}"));
                prop_assert!(abc_left.generator.iter().any(|g| g.general.id == id));
                prop_assert!(abc_right.generator.iter().any(|g| g.general.id == id));
            }
            for i in 0..num_b {
                let id = Some(format!("b{i}"));
                prop_assert!(abc_left.generator.iter().any(|g| g.general.id == id));
                prop_assert!(abc_right.generator.iter().any(|g| g.general.id == id));
            }
            for i in 0..num_c {
                let id = Some(format!("c{i}"));
                prop_assert!(abc_left.generator.iter().any(|g| g.general.id == id));
                prop_assert!(abc_right.generator.iter().any(|g| g.general.id == id));
            }
        }

        #[test]
        fn duplicate_generator_ids_always_rejected(
            num_unique in 2_usize..5,
            dup_index in 0_usize..100,
        ) {
            // Create generators with unique IDs
            let mut generators: Vec<_> = (0..num_unique)
                .map(|i| make_tcp_generator(Some(&format!("gen{i}")), 8000 + i as u16))
                .collect();

            // Add a duplicate ID at a random position
            let dup_idx = dup_index % num_unique;
            let dup_id = format!("gen{dup_idx}");
            generators.push(make_tcp_generator(Some(&dup_id), 9000));

            // Check should fail
            let result = check_duplicate_generator_ids(&generators);
            prop_assert!(result.is_err());
            prop_assert!(matches!(result, Err(Error::DuplicateGeneratorId(_))));
        }

        #[test]
        fn duplicate_blackhole_ids_always_rejected(
            num_unique in 2_usize..5,
            dup_index in 0_usize..100,
        ) {
            // Create blackholes with unique IDs
            let mut blackholes: Vec<_> = (0..num_unique)
                .map(|i| make_tcp_blackhole(Some(&format!("bh{i}")), 9000 + i as u16))
                .collect();

            // Add a duplicate ID at a random position
            let dup_idx = dup_index % num_unique;
            let dup_id = format!("bh{dup_idx}");
            blackholes.push(make_tcp_blackhole(Some(&dup_id), 10000));

            // Check should fail
            let result = check_duplicate_blackhole_ids(&blackholes);
            prop_assert!(result.is_err());
            prop_assert!(matches!(result, Err(Error::DuplicateBlackholeId(_))));
        }

        #[test]
        fn merging_overlapping_singletons_fails(has_telemetry: bool) {
            let partial_a = PartialConfig {
                telemetry: if has_telemetry {
                    Some(Telemetry::Prometheus {
                        addr: "0.0.0.0:9000".parse().expect("static string"),
                        global_labels: FxHashMap::default(),
                    })
                } else {
                    None
                },
                ..Default::default()
            };

            let partial_b = PartialConfig {
                telemetry: Some(Telemetry::Prometheus {
                    addr: "0.0.0.0:9001".parse().expect("static string"),
                    global_labels: FxHashMap::default(),
                }),
                ..Default::default()
            };

            let result = Config::merge_partial(partial_a, partial_b);

            if has_telemetry {
                // Should fail when both have telemetry
                prop_assert!(result.is_err());
                prop_assert!(matches!(result, Err(Error::ConflictingTelemetry)));
            } else {
                // Should succeed when only one has telemetry
                prop_assert!(result.is_ok());
            }
        }

        #[test]
        fn target_metrics_merge_unique_entries(
            num_a in 1_usize..4,
            num_b in 1_usize..4,
        ) {
            // Create two sets of target_metrics with different URIs
            let metrics_a: Vec<_> = (0..num_a)
                .map(|i| make_prometheus_target_metrics(&format!("http://localhost:{}/metrics", 9000 + i)))
                .collect();
            let metrics_b: Vec<_> = (0..num_b)
                .map(|i| make_prometheus_target_metrics(&format!("http://localhost:{}/metrics", 10000 + i)))
                .collect();

            let partial_a = make_partial_with_target_metrics(metrics_a);
            let partial_b = make_partial_with_target_metrics(metrics_b);

            let merged = Config::merge_partial(partial_a, partial_b)
                .expect("merge should succeed");

            // Unique target_metrics should all be present
            let target_metrics = merged.target_metrics.expect("should have target_metrics");
            prop_assert_eq!(target_metrics.len(), num_a + num_b);
        }

        #[test]
        fn target_metrics_merge_deduplicates(
            num_unique in 1_usize..4,
            num_duplicates in 1_usize..4,
        ) {
            // Create metrics that exist in both partials (duplicates)
            let shared_metrics: Vec<_> = (0..num_unique)
                .map(|i| make_prometheus_target_metrics(&format!("http://localhost:{}/metrics", 9000 + i)))
                .collect();

            // partial_a has the shared metrics
            let partial_a = make_partial_with_target_metrics(shared_metrics.clone());

            // partial_b has duplicates of some shared metrics
            let duplicate_metrics: Vec<_> = (0..num_duplicates)
                .map(|i| make_prometheus_target_metrics(&format!("http://localhost:{}/metrics", 9000 + (i % num_unique))))
                .collect();
            let partial_b = make_partial_with_target_metrics(duplicate_metrics);

            let merged = Config::merge_partial(partial_a, partial_b)
                .expect("merge should succeed");

            // Should only have unique metrics, duplicates removed
            let target_metrics = merged.target_metrics.expect("should have target_metrics");
            prop_assert_eq!(target_metrics.len(), num_unique);

            // Verify all original metrics are present
            for metric in &shared_metrics {
                prop_assert!(target_metrics.contains(metric));
            }
        }

        #[test]
        fn none_id_generators_can_merge(
            num_a in 1_usize..5,
            num_b in 1_usize..5,
        ) {
            // Create generators with no IDs (None) - these should merge without conflict
            let gens_a: Vec<_> = (0..num_a)
                .map(|i| make_tcp_generator(None, 8000 + i as u16))
                .collect();
            let gens_b: Vec<_> = (0..num_b)
                .map(|i| make_tcp_generator(None, 9000 + i as u16))
                .collect();

            let partial_a = make_partial_with_generators(gens_a);
            let partial_b = make_partial_with_generators(gens_b);

            let merged = Config::merge_partial(partial_a, partial_b)
                .expect("merge of None-ID generators should succeed");

            prop_assert_eq!(merged.generator.len(), num_a + num_b);
            // All generators should have None IDs
            for generator in &merged.generator {
                prop_assert!(generator.general.id.is_none());
            }
        }
    }
}
