//! The container generator
//!
//! This generator is meant to spin up a container from a configured image. For now,
//! it does not actually do anything beyond logging that it's running and then waiting
//! for a shutdown signal.

use bollard::Docker;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, InspectContainerOptionsBuilder,
    RemoveContainerOptionsBuilder, StartContainerOptions, StopContainerOptionsBuilder,
};
use bollard::secret::ContainerCreateResponse;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::General;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of the container generator.
pub struct Config {
    /// The container repository (e.g. "library/nginx")
    pub repository: String,
    /// The container image tag (e.g. "latest")
    pub tag: String,
    /// Arguments to provide to the container
    pub args: Option<Vec<String>>,
    /// Environment variables to set in the container
    pub env: Option<Vec<String>>,
    /// Labels to apply to the container
    pub labels: Option<HashMap<String, String>>,
    /// Network mode to use for the container
    pub network_disabled: Option<bool>,
    /// Ports to expose from the container
    pub exposed_ports: Option<Vec<String>>,
    /// Maximum lifetime of containers before being replaced
    pub max_lifetime_seconds: Option<std::num::NonZeroU64>,
    /// Number of containers to spin up (defaults to 1)
    pub number_of_containers: Option<std::num::NonZeroU32>,
}

/// Errors produced by the `Container` generator.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Generic error produced by the container generator.
    #[error("Generic container error: {0}")]
    Generic(String),
    /// Error produced by the Bollard Docker client.
    #[error("Bollard/Docker error: {0}")]
    Bollard(#[from] bollard::errors::Error),
    /// Error produced by tokio
    #[error("Tokio error: {0}")]
    Tokio(#[from] mpsc::error::SendError<ContainerCreateResponse>),
}

#[derive(Debug)]
/// Represents a container that can be spun up from a configured image.
pub struct Container {
    config: Config,
    shutdown: lading_signal::Watcher,
}

impl Container {
    /// Create a new `Container` instance
    ///
    /// # Errors
    ///
    /// Will return an error if config parsing fails or runtime setup fails
    /// in the future. For now, always succeeds.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        _general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            shutdown,
        })
    }

    /// Run the `Container` generator.
    ///
    /// # Errors
    ///
    /// Will return an error if Docker connection fails, image pulling fails,
    /// container creation fails, container start fails, or container removal fails.
    ///
    /// # Panics
    ///
    /// Steps:
    /// 1. Connect to Docker.
    /// 2. Pull the specified image (if not available).
    /// 3. Create and start the container.
    /// 4. Wait for shutdown signal.
    /// 5. On shutdown, stop and remove the container.
    pub async fn spin(self) -> Result<(), Error> {
        info!(
            "Container generator running: {}:{}",
            self.config.repository, self.config.tag
        );

        let docker = Docker::connect_with_local_defaults()?;

        let full_image = format!("{}:{}", self.config.repository, self.config.tag);
        debug!("Ensuring image is available: {full_image}");

        pull_image(&docker, &full_image).await?;

        #[allow(clippy::unwrap_used)]
        let number_of_containers = self
            .config
            .number_of_containers
            .unwrap_or(std::num::NonZero::new(1).unwrap());

        let mut set = JoinSet::new();
        for _ in 0..number_of_containers.into() {
            let docker = docker.clone();
            let config = self.config.clone();
            let full_image = full_image.clone();
            set.spawn(async move {
                config
                    .create_and_start_container(&docker, &full_image)
                    .await
            });
        }

        let mut containers = set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<VecDeque<ContainerCreateResponse>, Error>>()?;

        // Wait for shutdown signal
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        let (new_container_tx, mut new_container_rx) = mpsc::channel(32);
        let mut recreate_interval = self
            .config
            .max_lifetime_seconds
            .map(|max_lifetime_seconds| {
                tokio::time::interval(
                    tokio::time::Duration::from_secs(max_lifetime_seconds.into())
                        / <std::num::NonZero<u32> as Into<u32>>::into(number_of_containers),
                )
            });
        let mut liveness_interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                // Destroy and replace containers
                _ = if let Some(ref mut interval) = recreate_interval { interval.tick() } else { std::future::pending().await } => {
                    if let Some(container) = containers.pop_front() {
                        let docker = docker.clone();
                        let config = self.config.clone();
                        let full_image = full_image.clone();
                        let new_container_tx = new_container_tx.clone();
                        tokio::spawn(async move {
                            stop_and_remove_container(&docker, &container).await?;
                            new_container_tx.send(config.create_and_start_container(&docker, &full_image).await?).await?;
                            Ok::<(), Error>(())
                        });
                    }
                }

                // Handle new containers from recycling tasks
                new_container = new_container_rx.recv() => {
                    if let Some(container) = new_container {
                        containers.push_back(container);
                    }
                }

                // Check that containers are still running every 10 seconds
                _ = liveness_interval.tick() => {
                    for container in &containers {
                        let inspect_options = InspectContainerOptionsBuilder::default().build();
                        if let Some(state) = docker.inspect_container(&container.id, Some(inspect_options)).await?.state {
                            if !state.running.unwrap_or(false) {
                                return Err(Error::Generic(format!(
                                    "Container {id} is not running anymore",
                                    id = container.id
                                )));
                            }
                        }
                    }
                }

                // Shutdown
                () = &mut shutdown_wait => {
                    debug!("shutdown signal received");
                    drop(new_container_tx);

                    let mut set = JoinSet::new();
                    for container in containers {
                        let docker = docker.clone();
                        set.spawn(async move {
                            stop_and_remove_container(&docker, &container).await
                        });
                    }

                    set.join_all().await;

                    return Ok(())
                }
            }
        }
    }
}

async fn pull_image(docker: &Docker, full_image: &str) -> Result<(), Error> {
    let create_image_options = CreateImageOptionsBuilder::default()
        .from_image(full_image)
        .build();

    let mut pull_stream = docker.create_image(Some(create_image_options), None, None);

    while let Some(item) = pull_stream.next().await {
        match item {
            Ok(status) => {
                if let Some(progress) = status.progress {
                    info!("Pull progress: {progress}");
                }
            }
            Err(e) => {
                warn!("Pull error: {e}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

impl Config {
    /// Convert the `Container` instance to a `ContainerConfig` for the Docker API.
    #[must_use]
    fn to_container_config(&self, full_image: &str) -> ContainerCreateBody {
        // The Docker API requires exposed ports in the format {"<port>/<protocol>": {}}.
        // For example: {"80/tcp": {}, "443/tcp": {}}
        // The port specification is in the key, and the value must be an empty object.
        // Bollard represents the empty object as HashMap<(), ()>, which is required by their API.
        #[allow(clippy::zero_sized_map_values)]
        let exposed_ports = self.exposed_ports.as_ref().map(|ports| {
            ports
                .iter()
                .map(|port| {
                    // Ensure port includes protocol (default to tcp if not specified)
                    let port_with_protocol = if port.contains('/') {
                        port.clone()
                    } else {
                        format!("{port}/tcp")
                    };
                    (port_with_protocol, HashMap::<(), ()>::new())
                })
                .collect()
        });

        ContainerCreateBody {
            image: Some(full_image.to_string()),
            tty: Some(true),
            cmd: self.args.clone(),
            env: self.env.clone(),
            labels: self.labels.clone(),
            network_disabled: self.network_disabled,
            exposed_ports,
            ..Default::default()
        }
    }

    async fn create_and_start_container(
        &self,
        docker: &Docker,
        full_image: &str,
    ) -> Result<ContainerCreateResponse, Error> {
        let container_name = format!("lading_container_{}", Uuid::new_v4());
        debug!("Creating container: {container_name}");

        let create_options = CreateContainerOptionsBuilder::default()
            .name(&container_name)
            .build();

        let container = docker
            .create_container(Some(create_options), self.to_container_config(full_image))
            .await?;

        debug!("Created container with id: {id}", id = container.id);
        for warning in &container.warnings {
            warn!("Container warning: {warning}");
        }

        docker
            .start_container(&container.id, None::<StartContainerOptions>)
            .await?;

        debug!("Started container: {id}", id = container.id);

        Ok(container)
    }
}

async fn stop_and_remove_container(
    docker: &Docker,
    container: &ContainerCreateResponse,
) -> Result<(), Error> {
    info!("Stopping container: {id}", id = container.id);
    let stop_options = StopContainerOptionsBuilder::default().t(5).build();
    if let Err(e) = docker
        .stop_container(&container.id, Some(stop_options))
        .await
    {
        warn!("Error stopping container {id}: {e}", id = container.id);
    }

    debug!("Removing container: {id}", id = container.id);
    let remove_options = RemoveContainerOptionsBuilder::default().force(true).build();
    docker
        .remove_container(&container.id, Some(remove_options))
        .await?;

    debug!("Removed container: {id}", id = container.id);

    Ok(())
}
