//! The container generator
//!
//! This generator creates Docker containers, recycling them when they exceed
//! their configured maximum lifetime.

use bollard::Docker;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
    StartContainerOptions, StopContainerOptionsBuilder,
};
use bollard::secret::ContainerCreateResponse;
use lading_throttle::{Throttle, builder::Builder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use super::General;

mod state_machine;
use state_machine::{Event, Operation, StateMachine};

fn default_number_of_containers() -> NonZeroU32 {
    NonZeroU32::MIN
}

fn default_network_disabled() -> bool {
    false
}

fn default_exposed_ports() -> Vec<String> {
    Vec::new()
}

fn default_max_lifetime_seconds() -> NonZeroU32 {
    NonZeroU32::MAX
}

/// Configuration of the container generator.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Repository in which the container is found, docker.io/datadog/foobar.
    pub repository: String,
    /// Container tag, :v1.0.0.
    pub tag: String,
    /// Arguments for the container entrypoint, maps to docker run CMD.
    pub args: Option<Vec<String>>,
    /// Environment variables for the container, maps to docker run --env.
    pub env: Option<Vec<String>>,
    /// Labels to set on the container, maps to docker run --label.
    pub labels: Option<HashMap<String, String>>,
    /// Whether to disable the container network, maps to docker run --network
    /// none.
    #[serde(default = "default_network_disabled")]
    pub network_disabled: bool,
    /// Ports exposed on the container, maps to docker run --expose.
    #[serde(default = "default_exposed_ports")]
    pub exposed_ports: Vec<String>,
    /// Maximum lifetime before recycling
    #[serde(default = "default_max_lifetime_seconds")]
    pub max_lifetime_seconds: NonZeroU32,
    /// Number of containers to create
    #[serde(default = "default_number_of_containers")]
    pub number_of_containers: NonZeroU32,
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
    /// State machine error
    #[error(transparent)]
    StateMachine(#[from] state_machine::Error),
}

/// Represents a container that can be spun up from a configured image.
#[derive(Debug)]
pub struct Container {
    config: Config,
    concurrent_containers: NonZeroU32,
    throttle: Option<Throttle>,
    shutdown: lading_signal::Watcher,
}

impl Container {
    /// Create a new `Container` instance
    ///
    /// # Errors
    ///
    /// # Panics
    ///
    /// Will return an error if config parsing fails or runtime setup fails
    /// in the future. For now, always succeeds.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        _general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let concurrent_containers = config.number_of_containers;

        // Configure throttle for recycling. The throttle controls how often we recycle containers.
        let throttle = if config.max_lifetime_seconds == NonZeroU32::MAX {
            // MAX means no recycling
            None
        } else {
            // We want to recycle all containers over the lifetime period, so we need
            // concurrent_containers operations over lifetime_seconds. This gives us
            // operations_per_second = concurrent_containers / lifetime_seconds.
            let ops = concurrent_containers
                .get()
                .saturating_div(config.max_lifetime_seconds.get());
            let ops_per_sec = NonZeroU32::new(ops).unwrap_or(NonZeroU32::MIN);

            Some(Builder::stable(ops_per_sec).build())
        };

        Ok(Self {
            config: config.clone(),
            concurrent_containers,
            throttle,
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
    /// Panics if container index cannot be converted to u32.
    #[allow(clippy::too_many_lines)]
    pub async fn spin(mut self) -> Result<(), Error> {
        info!(
            "Container generator running: {repository}:{tag}",
            repository = self.config.repository,
            tag = self.config.tag
        );

        let docker = Docker::connect_with_local_defaults()?;
        let full_image = format!(
            "{repository}:{tag}",
            repository = self.config.repository,
            tag = self.config.tag
        );

        debug!("Ensuring image is available: {full_image}");
        let create_image_options = CreateImageOptionsBuilder::default()
            .from_image(&full_image)
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

        let mut machine = StateMachine::new(self.concurrent_containers.get());
        let mut event = Event::Started;

        let mut containers: HashMap<u32, ContainerCreateResponse> = HashMap::new();

        loop {
            let operation = machine.next(event)?;
            debug!("State machine: {:?} -> {:?}", machine.state(), operation);

            event = match operation {
                Operation::CreateAllContainers => {
                    let mut created = Vec::new();
                    let mut success = true;

                    for idx in 0..self.concurrent_containers.get() {
                        match create_and_start_container(&docker, &self.config, &full_image, idx)
                            .await
                        {
                            Ok(container) => created.push(container),
                            Err(_) => {
                                success = false;
                                break;
                            }
                        }
                    }

                    if success {
                        for (idx, container) in created.into_iter().enumerate() {
                            let idx_u32 = u32::try_from(idx).expect("container index overflow");
                            containers.insert(idx_u32, container);
                        }
                        debug_assert_eq!(
                            containers.len(),
                            self.concurrent_containers.get() as usize,
                            "containers HashMap size mismatch"
                        );
                        Event::AllContainersReady
                    } else {
                        warn!("Failed to create all containers");
                        Event::ShutdownSignaled
                    }
                }
                Operation::RecycleContainer { index } => {
                    debug!("Recycling container {index}");

                    if let Some(old_container) = containers.remove(&index)
                        && let Err(e) = stop_and_remove_container(&docker, &old_container).await
                    {
                        warn!("Failed to stop old container {index}: {e}");
                    }

                    let result = create_and_start_container(&docker, &self.config, &full_image, index)
                        .await;

                    if let Ok(new_container) = result {
                        containers.insert(index, new_container);
                    } else {
                        warn!("Failed to create new container {index}");
                    }

                    debug_assert!(
                        containers.len() <= self.concurrent_containers.get() as usize,
                        "containers HashMap exceeded expected size"
                    );

                    Event::ContainerRecycled { index }
                }
                Operation::Wait => {
                    if let Some(ref mut throttle) = self.throttle {
                        let shutdown_wait = self.shutdown.clone().recv();
                        tokio::select! {
                            result = throttle.wait() => {
                                match result {
                                    Ok(()) => state_machine::Event::RecycleNext,
                                    Err(e) => {
                                        warn!("Throttle error: {e}");
                                        state_machine::Event::RecycleNext
                                    }
                                }
                            }
                            () = shutdown_wait => state_machine::Event::ShutdownSignaled
                        }
                    } else {
                        self.shutdown.clone().recv().await;
                        state_machine::Event::ShutdownSignaled
                    }
                }
                Operation::StopAllContainers => {
                    for container in containers.values() {
                        let _ = stop_and_remove_container(&docker, container).await;
                    }
                    Event::AllContainersStopped
                }
                Operation::Exit => return Ok(()),
            };
        }
    }
}

async fn create_and_start_container(
    docker: &Docker,
    config: &Config,
    full_image: &str,
    index: u32,
) -> Result<ContainerCreateResponse, Error> {
    let container_name = format!("lading_container_{index:05}");
    debug!("Creating container: {container_name}");

    let create_options = CreateContainerOptionsBuilder::default()
        .name(&container_name)
        .build();

    let container = docker
        .create_container(Some(create_options), config.to_container_config(full_image))
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

impl Config {
    /// Convert the `Container` instance to a `ContainerConfig` for the Docker API.
    #[must_use]
    fn to_container_config(&self, full_image: &str) -> ContainerCreateBody {
        // Docker API requires exposed ports as {"<port>/<protocol>": {}}
        // Bollard represents the empty object as HashMap<(), ()>
        #[allow(clippy::zero_sized_map_values)]
        let exposed_ports = if self.exposed_ports.is_empty() {
            None
        } else {
            Some(
                self.exposed_ports
                    .iter()
                    .map(|port| {
                        let port_with_protocol = if port.contains('/') {
                            port.clone()
                        } else {
                            format!("{port}/tcp")
                        };
                        (port_with_protocol, HashMap::<(), ()>::new())
                    })
                    .collect(),
            )
        };

        ContainerCreateBody {
            image: Some(full_image.to_string()),
            tty: Some(true),
            cmd: self.args.clone(),
            env: self.env.clone(),
            labels: self.labels.clone(),
            network_disabled: Some(self.network_disabled),
            exposed_ports,
            ..Default::default()
        }
    }

}
