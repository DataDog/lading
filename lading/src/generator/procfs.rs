//! procfs generator; emits /proc-like filesystem
//!
//! The procfs generator does not "connect" directly with the target, and
//! emulates a `/proc` filesystem, though it is not mounted at `/proc` in the
//! target container so as to avoid interfering in the container's OS.
//!
//! Data types for fields generally assume a 64-bit architecture, rather than
//! use Rust's C-compatible data types.

use ::std::{num::NonZeroU32, path::PathBuf};

use ::rand::SeedableRng;
use ::serde::Deserialize;

use super::General;
use crate::signals::Shutdown;

#[derive(::thiserror::Error, Debug)]
/// Errors emitted by [`Procfs`]
pub enum Error {
    /// Wrapper around [`std::io::Error`]
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
}

#[derive(Debug, Deserialize, PartialEq)]
/// Configuration of [`Procfs`]
pub struct Config {
    /// Seed for random operations against this target
    pub seed: [u8; 32],
    /// Root path for `procfs` filesystem
    pub root: PathBuf,
    /// Upper bound on processes created
    pub max_processes: NonZeroU32,
}

/// The procfs generator.
///
/// Generates a fake `procfs` filesystem. Currently incomplete.
#[allow(dead_code)]
#[derive(Debug)]
pub struct Procfs {
    config: Config,
    shutdown: Shutdown,
}

impl Procfs {
    /// Create a new [`Procfs`] generator.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be serialized.
    pub fn new(general: General, config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        // TODO(geoffrey.oxberry@datadoghq.com): A more feature-ful version of a
        // procfs generator should check whether the maximum number of arguments
        // setting doesn't violate `sysconf` settings, e.g., `ARG_MAX`.

        // TODO(geoffrey.oxberry@datadoghq.com): Use this concrete RNG to build
        // procfs payloads that are parametrized by an abstract type satisfying
        // the trait bound `rand::Rng + ?Sized`.
        let mut _rng = ::rand::rngs::StdRng::from_seed(config.seed);

        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "procfs".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        Ok(Self { config, shutdown })
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
