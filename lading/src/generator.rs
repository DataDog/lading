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

mod common;
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

/// Configuration for [`Server`] - now a type alias to Inner
pub type Config = Inner;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configurations common to all [`Server`] variants - deprecated, functionality moved to enum variants
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
    Tcp {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The tcp generator configuration
        #[serde(flatten)]
        config: tcp::Config,
    },
    /// See [`crate::generator::udp::Config`] for details.
    Udp {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The udp generator configuration
        #[serde(flatten)]
        config: udp::Config,
    },
    /// See [`crate::generator::http::Config`] for details.
    Http {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The http generator configuration
        #[serde(flatten)]
        config: http::Config,
    },
    /// See [`crate::generator::splunk_hec::Config`] for details.
    SplunkHec {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The splunk_hec generator configuration
        #[serde(flatten)]
        config: splunk_hec::Config,
    },
    /// See [`crate::generator::file_gen::Config`] for details.
    FileGen {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The file_gen generator configuration
        #[serde(flatten)]
        config: file_gen::Config,
    },
    /// See [`crate::generator::file_tree::Config`] for details.
    FileTree {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The file_tree generator configuration
        #[serde(flatten)]
        config: file_tree::Config,
    },
    /// See [`crate::generator::grpc::Config`] for details.
    Grpc {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The grpc generator configuration
        #[serde(flatten)]
        config: grpc::Config,
    },
    /// See [`crate::generator::unix_stream::Config`] for details.
    UnixStream {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The unix_stream generator configuration
        #[serde(flatten)]
        config: unix_stream::Config,
    },
    /// See [`crate::generator::unix_datagram::Config`] for details.
    UnixDatagram {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The unix_datagram generator configuration
        #[serde(flatten)]
        config: unix_datagram::Config,
    },
    /// See [`crate::generator::passthru_file::Config`] for details.
    PassthruFile {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The passthru_file generator configuration
        #[serde(flatten)]
        config: passthru_file::Config,
    },
    /// See [`crate::generator::process_tree::Config`] for details.
    ProcessTree {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The process_tree generator configuration
        #[serde(flatten)]
        config: process_tree::Config,
    },
    /// See [`crate::generator::procfs::Config`] for details.
    ProcFs {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The procfs generator configuration
        #[serde(flatten)]
        config: procfs::Config,
    },
    /// See [`crate::generator::container::Config`] for details.
    Container {
        /// The ID assigned to this generator
        id: Option<String>,
        /// The container generator configuration
        #[serde(flatten)]
        config: container::Config,
    },
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
        let srv = match config {
            Inner::Tcp { id, config: conf } => {
                let general = General { id };
                Self::Tcp(tcp::Tcp::new(general, &conf, shutdown)?)
            }
            Inner::Udp { id, config: conf } => {
                let general = General { id };
                Self::Udp(udp::Udp::new(general, &conf, shutdown)?)
            }
            Inner::Http { id, config: conf } => {
                let general = General { id };
                Self::Http(http::Http::new(general, conf, shutdown)?)
            }
            Inner::SplunkHec { id, config: conf } => {
                let general = General { id };
                Self::SplunkHec(splunk_hec::SplunkHec::new(general, conf, shutdown)?)
            }
            Inner::FileGen { id, config: conf } => {
                let general = General { id };
                Self::FileGen(file_gen::FileGen::new(general, conf, shutdown)?)
            }
            Inner::FileTree { config: conf, .. } => {
                Self::FileTree(file_tree::FileTree::new(&conf, shutdown)?)
            }
            Inner::Grpc { id, config: conf } => {
                let general = General { id };
                Self::Grpc(grpc::Grpc::new(general, conf, shutdown)?)
            }
            Inner::UnixStream { id, config: conf } => {
                if let lading_payload::Config::DogStatsD(variant) = conf.variant
                    && !variant.length_prefix_framed
                {
                    warn!(
                        "Dogstatsd stream requires length prefix framing. You likely want to add `length_prefix_framed: true` to your payload config."
                    );
                }

                let general = General { id };
                Self::UnixStream(unix_stream::UnixStream::new(general, &conf, shutdown)?)
            }
            Inner::PassthruFile { id, config: conf } => {
                let general = General { id };
                Self::PassthruFile(passthru_file::PassthruFile::new(general, &conf, shutdown)?)
            }
            Inner::UnixDatagram { id, config: conf } => {
                let general = General { id };
                Self::UnixDatagram(unix_datagram::UnixDatagram::new(general, &conf, shutdown)?)
            }
            Inner::ProcessTree { config: conf, .. } => {
                Self::ProcessTree(process_tree::ProcessTree::new(&conf, shutdown)?)
            }
            Inner::ProcFs { config: conf, .. } => {
                Self::ProcFs(procfs::ProcFs::new(&conf, shutdown)?)
            }
            Inner::Container { id, config: conf } => {
                let general = General { id };
                Self::Container(container::Container::new(general, &conf, shutdown)?)
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
            Server::Container(inner) => inner.spin().await?,
        }

        Ok(())
    }
}
