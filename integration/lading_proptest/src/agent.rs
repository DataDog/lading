//! Agent abstraction for running the Datadog Agent as a container or binary.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Child;
use tracing::{debug, info, warn};

/// Errors from agent lifecycle operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O error during agent operations.
    #[error("agent I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Docker API error.
    #[error("docker error: {0}")]
    Docker(#[from] bollard::errors::Error),
    /// Agent failed to become ready within timeout.
    #[error("agent readiness timeout after {0:?}")]
    ReadinessTimeout(Duration),
    /// Agent exited unexpectedly.
    #[error("agent exited unexpectedly with status: {0}")]
    UnexpectedExit(String),
}

/// How to run the agent.
#[derive(Debug, Clone)]
pub enum AgentTarget {
    /// Run the agent as a Docker container.
    Container(ContainerAgentConfig),
    /// Run the agent as a local binary.
    Binary(BinaryAgentConfig),
}

/// Configuration for running the agent as a Docker container.
#[derive(Debug, Clone)]
pub struct ContainerAgentConfig {
    /// Docker image, e.g. `"datadog/agent:latest"`.
    pub image: String,
    /// Additional environment variables.
    pub extra_env: Vec<(String, String)>,
}

/// Configuration for running the agent as a local binary.
#[derive(Debug, Clone)]
pub struct BinaryAgentConfig {
    /// Path to the agent binary.
    pub binary_path: PathBuf,
    /// Additional environment variables.
    pub extra_env: Vec<(String, String)>,
}

/// A running agent instance.
///
/// If dropped without calling [`RunningAgent::stop`], containers are
/// force-removed in a background thread to prevent orphans on panic or
/// Ctrl+C.
#[derive(Debug)]
pub struct RunningAgent {
    inner: Option<RunningAgentInner>,
}

#[derive(Debug)]
enum RunningAgentInner {
    Container {
        docker: bollard::Docker,
        container_id: String,
    },
    Binary {
        child: Child,
    },
}

impl Drop for RunningAgent {
    fn drop(&mut self) {
        if let Some(RunningAgentInner::Container { docker, container_id }) = self.inner.take() {
            warn!("RunningAgent dropped without stop() — force-removing container {container_id}");
            // Spawn a blocking thread so this works even outside a tokio context.
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new();
                if let Ok(rt) = rt {
                    let _ = rt.block_on(async {
                        let opts =
                            bollard::query_parameters::RemoveContainerOptionsBuilder::default()
                                .force(true)
                                .build();
                        docker.remove_container(&container_id, Some(opts)).await
                    });
                }
            })
            .join();
        }
    }
}

impl AgentTarget {
    /// Start the agent with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns error if the agent fails to start.
    pub async fn start(
        &self,
        config_dir: &Path,
        log_dir: &Path,
        intake_port: u16,
    ) -> Result<RunningAgent, Error> {
        match self {
            Self::Container(cfg) => start_container(cfg, config_dir, log_dir, intake_port).await,
            Self::Binary(cfg) => start_binary(cfg, config_dir, intake_port),
        }
    }

    /// Build an `AgentTarget` from environment variables.
    ///
    /// Checks `DD_AGENT_BINARY` first (binary mode), then falls back to
    /// `DD_AGENT_IMAGE` (container mode, default `datadog/agent:latest`).
    ///
    /// # Panics
    ///
    /// This function does not panic.
    #[must_use]
    pub fn from_env() -> Self {
        if let Ok(binary) = std::env::var("DD_AGENT_BINARY") {
            Self::Binary(BinaryAgentConfig {
                binary_path: PathBuf::from(binary),
                extra_env: Vec::new(),
            })
        } else {
            let image = std::env::var("DD_AGENT_IMAGE")
                .unwrap_or_else(|_| "datadog/agent:latest".to_string());
            Self::Container(ContainerAgentConfig {
                image,
                extra_env: Vec::new(),
            })
        }
    }
}

impl RunningAgent {
    /// Wait for the agent to become ready.
    ///
    /// # Errors
    ///
    /// Returns error if readiness is not achieved within the timeout.
    ///
    /// # Panics
    ///
    /// Panics if called after [`RunningAgent::stop`].
    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        let inner = self.inner.as_ref().expect("agent already stopped");
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::ReadinessTimeout(timeout));
            }
            match inner {
                RunningAgentInner::Container { docker, container_id } => {
                    if let Ok(info) = docker
                        .inspect_container(
                            container_id,
                            None::<bollard::query_parameters::InspectContainerOptions>,
                        )
                        .await
                        && let Some(state) = &info.state
                        && state.running == Some(true)
                    {
                        debug!("container {container_id} is running");
                        return Ok(());
                    }
                }
                RunningAgentInner::Binary { child } => {
                    if child.id().is_some() {
                        debug!("agent binary process is alive");
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Stop the agent gracefully.
    ///
    /// # Errors
    ///
    /// Returns error if the agent cannot be stopped.
    pub async fn stop(mut self) -> Result<(), Error> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        match inner {
            RunningAgentInner::Container { docker, container_id } => {
                info!("stopping container {container_id}");
                let stop_options =
                    bollard::query_parameters::StopContainerOptionsBuilder::default()
                        .t(30)
                        .build();
                docker
                    .stop_container(&container_id, Some(stop_options))
                    .await?;
                let remove_options =
                    bollard::query_parameters::RemoveContainerOptionsBuilder::default()
                        .force(true)
                        .build();
                docker
                    .remove_container(&container_id, Some(remove_options))
                    .await?;
                Ok(())
            }
            RunningAgentInner::Binary { mut child } => {
                info!("stopping agent binary");
                #[cfg(unix)]
                {
                    if let Some(pid) = child.id() {
                        let nix_pid =
                            nix::unistd::Pid::from_raw(i32::try_from(pid).unwrap_or(0));
                        let _ =
                            nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGTERM);
                    }
                }
                if let Ok(result) =
                    tokio::time::timeout(Duration::from_secs(30), child.wait()).await
                {
                    let status = result?;
                    debug!("agent exited with status: {status}");
                } else {
                    warn!("agent did not exit after SIGTERM, killing");
                    child.kill().await?;
                }
                Ok(())
            }
        }
    }
}

async fn start_container(
    cfg: &ContainerAgentConfig,
    config_dir: &Path,
    log_dir: &Path,
    intake_port: u16,
) -> Result<RunningAgent, Error> {
    use bollard::models::{ContainerCreateBody, HostConfig, Mount, MountTypeEnum};
    use bollard::query_parameters::{CreateContainerOptionsBuilder, StartContainerOptions};

    let docker = bollard::Docker::connect_with_local_defaults()?;

    let host_addr = if cfg!(target_os = "macos") {
        format!("host.docker.internal:{intake_port}")
    } else {
        format!("172.17.0.1:{intake_port}")
    };

    let mut env = vec![
        "DD_API_KEY=fake_key_for_proptest".to_string(),
        "DD_HOSTNAME=lading-proptest".to_string(),
        "DD_LOGS_ENABLED=true".to_string(),
        format!("DD_LOGS_CONFIG_LOGS_DD_URL={host_addr}"),
        "DD_LOGS_CONFIG_LOGS_NO_SSL=true".to_string(),
        "DD_LOGS_CONFIG_FORCE_USE_HTTP=true".to_string(),
        "DD_LOGS_CONFIG_BATCH_WAIT=1".to_string(),
        // Point all non-logs telemetry at a dead endpoint so the agent
        // does not make outbound requests to datadoghq.com.
        "DD_DD_URL=http://localhost:1".to_string(),
        // Disable non-logs subsystems
        "DD_APM_ENABLED=false".to_string(),
        "DD_PROCESS_CONFIG_PROCESS_COLLECTION_ENABLED=false".to_string(),
        "DD_ENABLE_METADATA_COLLECTION=false".to_string(),
        "DD_INVENTORIES_ENABLED=false".to_string(),
    ];
    for (k, v) in &cfg.extra_env {
        env.push(format!("{k}={v}"));
    }

    let config_mount = config_dir
        .to_str()
        .expect("config_dir must be valid UTF-8");
    let log_mount = log_dir.to_str().expect("log_dir must be valid UTF-8");

    let container_config = ContainerCreateBody {
        image: Some(cfg.image.clone()),
        env: Some(env),
        host_config: Some(HostConfig {
            mounts: Some(vec![
                Mount {
                    target: Some("/etc/datadog-agent/datadog.yaml".to_string()),
                    source: Some(format!("{config_mount}/datadog.yaml")),
                    typ: Some(MountTypeEnum::BIND),
                    read_only: Some(true),
                    ..Default::default()
                },
                Mount {
                    target: Some("/etc/datadog-agent/conf.d".to_string()),
                    source: Some(format!("{config_mount}/conf.d")),
                    typ: Some(MountTypeEnum::BIND),
                    read_only: Some(true),
                    ..Default::default()
                },
                Mount {
                    target: Some("/var/log/proptest".to_string()),
                    source: Some(log_mount.to_string()),
                    typ: Some(MountTypeEnum::BIND),
                    read_only: Some(true),
                    ..Default::default()
                },
            ]),
            extra_hosts: if cfg!(target_os = "linux") {
                Some(vec!["host.docker.internal:host-gateway".to_string()])
            } else {
                None
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let name = format!("lading-proptest-{}", uuid::Uuid::new_v4());
    let create_options = CreateContainerOptionsBuilder::default()
        .name(&name)
        .build();
    let container = docker
        .create_container(Some(create_options), container_config)
        .await?;

    docker
        .start_container(&container.id, None::<StartContainerOptions>)
        .await?;

    info!("started container {} ({})", name, container.id);

    Ok(RunningAgent {
        inner: Some(RunningAgentInner::Container {
            docker,
            container_id: container.id,
        }),
    })
}

fn start_binary(
    cfg: &BinaryAgentConfig,
    config_dir: &Path,
    _intake_port: u16,
) -> Result<RunningAgent, Error> {
    use std::process::Stdio;

    let stdout_path = config_dir
        .parent()
        .unwrap_or(config_dir)
        .join("agent_stdout.log");
    let stderr_path = config_dir
        .parent()
        .unwrap_or(config_dir)
        .join("agent_stderr.log");

    let stdout_file = std::fs::File::create(&stdout_path)?;
    let stderr_file = std::fs::File::create(&stderr_path)?;

    let mut cmd = tokio::process::Command::new(&cfg.binary_path);
    cmd.arg("run")
        .arg("--cfgpath")
        .arg(config_dir)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));

    for (k, v) in &cfg.extra_env {
        cmd.env(k, v);
    }

    let child = cmd.spawn()?;
    info!("started agent binary from {:?}", cfg.binary_path);

    Ok(RunningAgent {
        inner: Some(RunningAgentInner::Binary { child }),
    })
}
