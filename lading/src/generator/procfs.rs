//! procfs generator; emits /proc-like filesystem
//!
//! The procfs generator does not "connect" directly with the target, and
//! emulates a `/proc` filesystem, though it is not mounted at `/proc` in the
//! target container so as to avoid interfering in the container's OS.
//!
//! Data types for fields generally assume a 64-bit architecture, rather than
//! use Rust's C-compatible data types.

use std::fs;
use std::path::StripPrefixError;
use std::str::FromStr;
use std::{fs::File, io::Write, num::NonZeroU32, path::PathBuf};

use lading_payload::procfs;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Deserialize;

use crate::signals::Phase;

#[derive(::thiserror::Error, Debug)]
/// Errors emitted by [`Procfs`]
pub enum Error {
    /// Wrapper around [`std::io::Error`]
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
    /// Unable to strip prefix from passed copy file
    #[error(transparent)]
    Strip(#[from] StripPrefixError),
}

fn default_copy_from_host() -> Vec<PathBuf> {
    vec![
        PathBuf::from_str("/proc/uptime")
            .expect("Error: failed to convert /proc/uptime to PathBuf"),
        PathBuf::from_str("/proc/stat").expect("Error: failed to convert /proc/stat to PathBuf"),
    ]
}

#[derive(Debug, Deserialize, PartialEq)]
/// Configuration of [`Procfs`]
pub struct Config {
    /// Seed for random operations against this target
    pub seed: [u8; 32],
    /// Root path for `procfs` filesystem
    pub root: PathBuf,
    /// Total number of processes created
    pub total_processes: NonZeroU32,
    /// Files to copy from host /proc to `root`.
    #[serde(default = "default_copy_from_host")]
    pub copy_from_host: Vec<PathBuf>,
}

/// The procfs generator.
///
/// Generates a fake `procfs` filesystem. Currently incomplete.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ProcFs {
    shutdown: Phase,
}

impl ProcFs {
    /// Create a new [`Procfs`] generator.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be serialized.
    ///
    /// # Panics
    ///
    /// Function should never panic.
    pub fn new(config: &Config, shutdown: Phase) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);

        let total_processes = config.total_processes.get();
        let mut processes = procfs::fixed(&mut rng, total_processes as usize);

        for process in processes.drain(..) {
            let pid = process.pid;

            let mut pid_root: PathBuf = config.root.clone();
            pid_root.push(format!("{pid}"));

            fs::create_dir_all(&pid_root)?;

            // /proc/{pid}/cmdline
            {
                let mut path: PathBuf = pid_root.clone();
                path.push("cmdline");

                let mut file = File::create(path)?;
                file.write_all(process.cmdline.as_bytes())?;
            }

            // /proc/{pid}/comm
            {
                let mut path: PathBuf = pid_root.clone();
                path.push("comm");

                let mut file = File::create(path)?;
                file.write_all(process.comm.as_bytes())?;
            }

            // /proc/{pid}/io
            {
                let mut path: PathBuf = pid_root.clone();
                path.push("io");

                let mut file = File::create(path)?;
                file.write_all(format!("{}", process.io).as_bytes())?;
            }

            // /proc/{pid}/stat
            {
                let mut path: PathBuf = pid_root.clone();
                path.push("stat");

                let mut file = File::create(path)?;
                file.write_all(format!("{}", process.stat).as_bytes())?;
            }

            // /proc/{pid}/statm
            {
                let mut path: PathBuf = pid_root.clone();
                path.push("statm");

                let mut file = File::create(path)?;
                file.write_all(format!("{}", process.statm).as_bytes())?;
            }

            // /proc/{pid}/status
            {
                let mut path: PathBuf = pid_root.clone();
                path.push("status");

                let mut file = File::create(path)?;
                file.write_all(format!("{}", process.status).as_bytes())?;
            }
        }

        // SAFETY: By construction this pathbuf cannot fail to be created.
        let prefix = PathBuf::from_str("/proc").expect("Error: failed to convert /proc to PathBuf");

        for path in &config.copy_from_host {
            let base = path.strip_prefix(&prefix)?;
            let mut new_path = config.root.clone();
            new_path.push(base);

            fs::copy(path, new_path)?;
        }

        Ok(Self { shutdown })
    }

    /// Run [`Procfs`] generator to completion or until a shutdown signal is
    /// received.
    ///
    /// This function will generate a fake `/proc` filesystem (i.e., a `procfs`)
    /// rooted at `self.root`. Currently an incomplete stub.
    ///
    /// # Errors
    ///
    /// This function will terminate with an error if files cannot be written to
    /// the directory tree rooted at `self.root`. Any error from
    /// `std::io::Error` is possible.
    pub async fn spin(mut self) -> Result<(), Error> {
        self.shutdown.recv().await;
        tracing::info!("shutdown signal received");
        Ok(())
    }
}
