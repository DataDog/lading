use byte_unit::Byte;
use clap::Parser;
use lading_payload::block;
use serde::Deserialize;
use std::path::PathBuf;

fn default_config_path() -> String {
    "/etc/lading/logrotate_fs.yaml".to_string()
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// path on disk to the configuration file
    #[clap(long, default_value_t = default_config_path())]
    config_path: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of [`FileGen`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The root path for writing logs.
    pub root: PathBuf,
    /// Total number of concurrent logs.
    pub concurrent_logs: u16,
    /// The **soft** maximum byte size of each log.
    pub maximum_bytes_per_log: Byte,
    /// The number of rotations per log file.
    pub total_rotations: u8,
    /// The maximum directory depth allowed below the root path. If 0 all log
    /// files will be present in the root path.
    pub max_depth: u8,
    /// Sets the [`crate::payload::Config`] of this template.
    pub variant: lading_payload::Config,
    /// Defines the number of bytes that written in each log file.
    bytes_per_second: Byte,
    /// Defines the maximum internal cache of this log target. `file_gen` will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    maximum_block_size: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    block_cache_method: block::CacheMethod,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Failed to deserialize configuration: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
}

fn main() -> Result<(), Error> {
    let args = Args::parse();
    let config: Config = serde_yaml::from_str(&args.config_path)?;

    Ok(())
}
