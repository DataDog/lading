//! Lading generators
//!
//! The lading generator is responsible for pushing load into the target
//! sub-process via a small handful of protocols, the variants of
//! [`Server`]. Each generator variant works in the same basic way: a block of
//! payloads are pre-computed at generator start time which are then spammed
//! into the target according to a user-defined rate limit in a cyclic
//! manner. That is, we avoid runtime delays in payload generation by, well,
//! building a lot of payloads in one shot and rotating through them
//! indefinitely, paying higher memory and longer startup for better
//! experimental control.

use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::target::TargetPidReceiver;

pub mod container;
pub mod file_gen;
pub mod file_tree;
pub mod grpc;
pub mod http;
pub mod passthru_file;
pub mod process_tree;
pub mod procfs;
pub mod splunk_hec;
pub mod tcp;
pub mod udp;
pub mod unix_datagram;
pub mod unix_stream;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Server`].
pub enum Error {
    /// See [`crate::generator::tcp::Error`] for details.
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    /// See [`crate::generator::udp::Error`] for details.
    #[error(transparent)]
    Udp(#[from] udp::Error),
    /// See [`crate::generator::http::Error`] for details.
    #[error(transparent)]
    Http(#[from] http::Error),
    /// See [`crate::generator::splunk_hec::Error`] for details.
    #[error(transparent)]
    SplunkHec(#[from] splunk_hec::Error),
    /// See [`crate::generator::file_gen::Error`] for details.
    #[error(transparent)]
    FileGen(#[from] file_gen::Error),
    /// See [`crate::generator::file_tree::Error`] for details.
    #[error(transparent)]
    FileTree(#[from] file_tree::Error),
    /// See [`crate::generator::grpc::Error`] for details.
    #[error(transparent)]
    Grpc(#[from] grpc::Error),
    /// See [`crate::generator::unix_stream::Error`] for details.
    #[error(transparent)]
    UnixStream(#[from] unix_stream::Error),
    /// See [`crate::generator::unix_datagram::Error`] for details.
    #[error(transparent)]
    UnixDatagram(#[from] unix_datagram::Error),
    /// See [`crate::generator::passthru_file::Error`] for details.
    #[error(transparent)]
    PassthruFile(#[from] passthru_file::Error),
    /// See [`crate::generator::process_tree::Error`] for details.
    #[error(transparent)]
    ProcessTree(#[from] process_tree::Error),
    /// See [`crate::generator::procfs::Error`] for details.
    #[error(transparent)]
    ProcFs(#[from] procfs::Error),
    /// See [`crate::generator::container::Error`] for details.
    #[error(transparent)]
    Container(#[from] container::Error),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configuration for [`Server`]
pub struct Config {
    /// Common generator configs
    #[serde(flatten)]
    pub general: General,
    /// The generator config
    #[serde(flatten)]
    pub inner: Inner,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configurations common to all [`Server`] variants
pub struct General {
    /// The ID assigned to this generator
    pub id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configuration for [`Server`]
pub enum Inner {
    /// See [`crate::generator::tcp::Config`] for details.
    Tcp(tcp::Config),
    /// See [`crate::generator::udp::Config`] for details.
    Udp(udp::Config),
    /// See [`crate::generator::http::Config`] for details.
    Http(http::Config),
    /// See [`crate::generator::splunk_hec::Config`] for details.
    SplunkHec(splunk_hec::Config),
    /// See [`crate::generator::file_gen::Config`] for details.
    FileGen(file_gen::Config),
    /// See [`crate::generator::file_tree::Config`] for details.
    FileTree(file_tree::Config),
    /// See [`crate::generator::grpc::Config`] for details.
    Grpc(grpc::Config),
    /// See [`crate::generator::unix_stream::Config`] for details.
    UnixStream(unix_stream::Config),
    /// See [`crate::generator::unix_datagram::Config`] for details.
    UnixDatagram(unix_datagram::Config),
    /// See [`crate::generator::passthru_file::Config`] for details.
    PassthruFile(passthru_file::Config),
    /// See [`crate::generator::process_tree::Config`] for details.
    ProcessTree(process_tree::Config),
    /// See [`crate::generator::procfs::Config`] for details.
    ProcFs(procfs::Config),
    /// See [`crate::generator::container::Config`] for details.
    Container(container::Config),
}

#[derive(Debug)]
/// The generator server.
///
/// All generators supported by lading are a variant of this enum. Please see
/// variant documentation for details.
pub enum Server {
    /// See [`crate::generator::tcp::Tcp`] for details.
    Tcp(tcp::Tcp),
    /// See [`crate::generator::udp::Udp`] for details.
    Udp(udp::Udp),
    /// See [`crate::generator::http::Http`] for details.
    Http(http::Http),
    /// See [`crate::generator::splunk_hec::SplunkHec`] for details.
    SplunkHec(splunk_hec::SplunkHec),
    /// See [`crate::generator::file_gen::FileGen`] for details.
    FileGen(file_gen::FileGen),
    /// See [`crate::generator::file_tree::FileTree`] for details.
    FileTree(file_tree::FileTree),
    /// See [`crate::generator::grpc::Grpc`] for details.
    Grpc(grpc::Grpc),
    /// See [`crate::generator::unix_stream::UnixStream`] for details.
    UnixStream(unix_stream::UnixStream),
    /// See [`crate::generator::unix_datagram::UnixDatagram`] for details.
    UnixDatagram(unix_datagram::UnixDatagram),
    /// See [`crate::generator::passthru_file::PassthruFile`] for details.
    PassthruFile(passthru_file::PassthruFile),
    /// See [`crate::generator::process_tree::ProcessTree`] for details.
    ProcessTree(process_tree::ProcessTree),
    /// See [`crate::generator::procfs::Procfs`] for details.
    ProcFs(procfs::ProcFs),
    /// See [`crate::generator::container::Container`] for details.
    Container(container::Container),
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
    pub fn new(config: Config, shutdown: lading_signal::Watcher) -> Result<Self, Error> {
        let srv = match config.inner {
            Inner::Tcp(conf) => Self::Tcp(tcp::Tcp::new(config.general, &conf, shutdown)?),
            Inner::Udp(conf) => Self::Udp(udp::Udp::new(config.general, &conf, shutdown)?),
            Inner::Http(conf) => Self::Http(http::Http::new(config.general, conf, shutdown)?),
            Inner::SplunkHec(conf) => {
                Self::SplunkHec(splunk_hec::SplunkHec::new(config.general, conf, shutdown)?)
            }
            Inner::FileGen(conf) => {
                Self::FileGen(file_gen::FileGen::new(config.general, conf, shutdown)?)
            }
            Inner::FileTree(conf) => Self::FileTree(file_tree::FileTree::new(&conf, shutdown)?),
            Inner::Grpc(conf) => Self::Grpc(grpc::Grpc::new(config.general, conf, shutdown)?),
            Inner::UnixStream(conf) => {
                if let lading_payload::Config::DogStatsD(variant) = conf.variant {
                    if !variant.length_prefix_framed {
                        warn!("Dogstatsd stream requires length prefix framing. You likely want to add `length_prefix_framed: true` to your payload config.");
                    }
                }

                Self::UnixStream(unix_stream::UnixStream::new(
                    config.general,
                    conf,
                    shutdown,
                )?)
            }
            Inner::PassthruFile(conf) => Self::PassthruFile(passthru_file::PassthruFile::new(
                config.general,
                &conf,
                shutdown,
            )?),
            Inner::UnixDatagram(conf) => Self::UnixDatagram(unix_datagram::UnixDatagram::new(
                config.general,
                &conf,
                shutdown,
            )?),
            Inner::ProcessTree(conf) => {
                Self::ProcessTree(process_tree::ProcessTree::new(&conf, shutdown)?)
            }
            Inner::ProcFs(conf) => Self::ProcFs(procfs::ProcFs::new(&conf, shutdown)?),
            Inner::Container(conf) => {
                Self::Container(container::Container::new(config.general, &conf, shutdown)?)
            }
        };
        Ok(srv)
    }

    /// Run this [`Server`] to completion
    ///
    /// This function runs the sub-server its completion, or until a shutdown
    /// signal is received. Target server will transmit its pid via `pid_snd`
    /// once the sub-process has started. This server will only begin once that
    /// PID is sent, implying that the target is online.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server signals
    /// error.
    pub async fn run(self, mut pid_snd: TargetPidReceiver) -> Result<(), Error> {
        // Pause until the target process is running.
        let _ = pid_snd.recv().await;
        drop(pid_snd);

        match self {
            Server::Tcp(inner) => inner.spin().await?,
            Server::Udp(inner) => inner.spin().await?,
            Server::Http(inner) => inner.spin().await?,
            Server::SplunkHec(inner) => inner.spin().await?,
            Server::FileGen(inner) => inner.spin().await?,
            Server::FileTree(inner) => inner.spin().await?,
            Server::Grpc(inner) => inner.spin().await?,
            Server::UnixStream(inner) => inner.spin().await?,
            Server::UnixDatagram(inner) => inner.spin().await?,
            Server::PassthruFile(inner) => inner.spin().await?,
            Server::ProcessTree(inner) => inner.spin().await?,
            Server::ProcFs(inner) => inner.spin().await?,
            // Run the container generator
            Server::Container(inner) => inner.spin().await?,
        };

        Ok(())
    }
}
