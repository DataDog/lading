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
use lading_throttle::Throttle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use super::{General, common::BytesThrottleConfig};

mod state_machine;
use state_machine::{Event, Operation, StateMachine};

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
    pub max_lifetime_seconds: Option<NonZeroU64>,
    /// Number of containers to spin up (defaults to 1)
    pub number_of_containers: Option<NonZeroU32>,
    /// Advanced throttle configuration
    pub throttle: Option<BytesThrottleConfig>,
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

#[derive(Debug)]
/// Represents a container that can be spun up from a configured image.
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
    /// Will return an error if config parsing fails or runtime setup fails
    /// in the future. For now, always succeeds.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        _general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let concurrent_containers = config.number_of_containers.unwrap_or(NonZeroU32::MIN);

        // Configure throttle if max_lifetime_seconds is set
        // The throttle controls how often we recycle containers
        let throttle = config.max_lifetime_seconds.map(|lifetime_secs| {
            // We want to recycle all containers over the lifetime period
            // So we need concurrent_containers operations over lifetime_seconds
            // This gives us operations_per_second = concurrent_containers / lifetime_seconds
            let ops_per_sec = if lifetime_secs.get() > u64::from(u32::MAX) {
                // If lifetime is huge, just do 1 op/sec
                NonZeroU32::MIN
            } else {
                #[allow(clippy::cast_possible_truncation)]
                let lifetime_secs_u32 = lifetime_secs.get() as u32;
                let ops = concurrent_containers
                    .get()
                    .saturating_div(lifetime_secs_u32);
                // Ensure at least 1 operation per second
                NonZeroU32::new(ops).unwrap_or(NonZeroU32::MIN)
            };

            let throttle_config = lading_throttle::Config::Stable {
                maximum_capacity: ops_per_sec,
                timeout_micros: 0,
            };
            Throttle::new_with_config(throttle_config)
        });

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
    #[allow(clippy::too_many_lines)]
    pub async fn spin(mut self) -> Result<(), Error> {
        info!(
            "Container generator running: {}:{}",
            self.config.repository, self.config.tag
        );

        let docker = Docker::connect_with_local_defaults()?;
        let full_image = format!("{}:{}", self.config.repository, self.config.tag);

        debug!("Ensuring image is available: {full_image}");
        Self::pull_image(&docker, &full_image).await?;

        let mut machine = StateMachine::new(self.concurrent_containers.get());
        let mut event = Event::Started;

        // Track containers by index
        let mut containers: HashMap<u32, ContainerCreateResponse> = HashMap::new();

        loop {
            let operation = machine.next(event)?;
            debug!("State machine: {:?} -> {:?}", machine.state(), operation);

            event = match operation {
                Operation::CreateAllContainers => {
                    debug!("Creating all {} containers", self.concurrent_containers);
                    let result = self.create_all_containers(&docker, &full_image).await;

                    match result {
                        Ok(created) => {
                            for (idx, container) in created.into_iter().enumerate() {
                                #[allow(clippy::cast_possible_truncation)]
                                let idx_u32 = idx as u32;
                                containers.insert(idx_u32, container);
                            }
                            debug!("Successfully created all containers");
                            Event::AllContainersCreated { success: true }
                        }
                        Err(e) => {
                            warn!("Failed to create all containers: {e}");
                            Event::AllContainersCreated { success: false }
                        }
                    }
                }
                Operation::CreateContainer { index } => {
                    debug!("Creating container {index}");
                    let result = self
                        .create_single_container(&docker, &full_image, index)
                        .await;

                    match result {
                        Ok(container) => {
                            containers.insert(index, container);
                            debug!("Successfully created container {index}");
                            Event::ContainerCreated {
                                index,
                                success: true,
                            }
                        }
                        Err(e) => {
                            warn!("Failed to create container {index}: {e}");
                            Event::ContainerCreated {
                                index,
                                success: false,
                            }
                        }
                    }
                }
                Operation::StopContainer { index } => {
                    debug!("Stopping container {index}");
                    let container = containers.get(&index);

                    let result = if let Some(container) = container {
                        Self::stop_and_remove_container(&docker, container).await
                    } else {
                        Err(Error::Generic(format!("Container {index} not found")))
                    };

                    match result {
                        Ok(()) => {
                            containers.remove(&index);
                            debug!("Successfully stopped container {index}");
                            Event::ContainerStopped {
                                index,
                                success: true,
                            }
                        }
                        Err(e) => {
                            warn!("Failed to stop container {index}: {e}");
                            Event::ContainerStopped {
                                index,
                                success: false,
                            }
                        }
                    }
                }
                Operation::Wait => self.handle_wait(&docker, &containers).await,
                Operation::StopAllContainers => {
                    debug!("Stopping all containers for shutdown");
                    let result = self.stop_all_containers(&docker, containers.values()).await;

                    match result {
                        Ok(()) => {
                            debug!("Successfully stopped all containers");
                            Event::AllContainersStopped { success: true }
                        }
                        Err(e) => {
                            warn!("Failed to stop all containers: {e}");
                            Event::AllContainersStopped { success: false }
                        }
                    }
                }
                Operation::Exit => {
                    debug!("Exiting generator");
                    return Ok(());
                }
            };
        }
    }

    async fn handle_wait(
        &mut self,
        docker: &Docker,
        containers: &HashMap<u32, ContainerCreateResponse>,
    ) -> state_machine::Event {
        // Handle throttle or just wait for shutdown
        if let Some(ref mut throttle) = self.throttle {
            let shutdown_wait = self.shutdown.clone().recv();
            let liveness_check = Self::check_container_liveness(docker, containers);

            tokio::select! {
                result = throttle.wait() => {
                    match result {
                        Ok(()) => state_machine::Event::RecycleNext,
                        Err(e) => {
                            warn!("Throttle error: {e}");
                            // Return RecycleNext anyway to continue
                            state_machine::Event::RecycleNext
                        }
                    }
                }
                result = liveness_check => {
                    if let Err(e) = result {
                        warn!("Container liveness check failed: {e}");
                        state_machine::Event::ShutdownSignaled
                    } else {
                        // Continue waiting
                        Box::pin(async { self.handle_wait(docker, containers).await }).await
                    }
                }
                () = shutdown_wait => {
                    debug!("Shutdown signal received");
                    state_machine::Event::ShutdownSignaled
                }
            }
        } else {
            // No throttle, just wait for shutdown
            self.shutdown.clone().recv().await;
            debug!("Shutdown signal received");
            state_machine::Event::ShutdownSignaled
        }
    }

    async fn check_container_liveness(
        docker: &Docker,
        containers: &HashMap<u32, ContainerCreateResponse>,
    ) -> Result<(), Error> {
        // Check container liveness every 10 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        for (idx, container) in containers {
            let inspect_options = InspectContainerOptionsBuilder::default().build();
            if let Some(state) = docker
                .inspect_container(&container.id, Some(inspect_options))
                .await?
                .state
                && !state.running.unwrap_or(false)
            {
                return Err(Error::Generic(format!(
                    "Container {} (index {}) is not running anymore",
                    container.id, idx
                )));
            }
        }
        Ok(())
    }

    async fn create_all_containers(
        &self,
        docker: &Docker,
        full_image: &str,
    ) -> Result<Vec<ContainerCreateResponse>, Error> {
        let mut set = JoinSet::new();

        for idx in 0..self.concurrent_containers.get() {
            let permit = self
                .operation_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Generic(format!("Semaphore error: {e}")))?;
            let docker = docker.clone();
            let config = self.config.clone();
            let full_image = full_image.to_string();

            set.spawn(async move {
                let result = config
                    .create_and_start_container(&docker, &full_image, idx)
                    .await;
                drop(permit);
                result
            });
        }

        set.join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    }

    async fn create_single_container(
        &self,
        docker: &Docker,
        full_image: &str,
        index: u32,
    ) -> Result<ContainerCreateResponse, Error> {
        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|e| Error::Generic(format!("Semaphore error: {e}")))?;

        self.config
            .create_and_start_container(docker, full_image, index)
            .await
    }

    async fn stop_all_containers<'a, I>(&self, docker: &Docker, containers: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'a ContainerCreateResponse>,
    {
        let mut set = JoinSet::new();

        for container in containers {
            let permit = self
                .operation_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Generic(format!("Semaphore error: {e}")))?;
            let docker = docker.clone();
            let container = container.clone();

            set.spawn(async move {
                let result = Container::stop_and_remove_container(&docker, &container).await;
                drop(permit);
                result
            });
        }

        // Collect all results and check for errors
        let results = set.join_all().await;
        let mut had_error = false;
        for result in results {
            if let Err(e) = result {
                warn!("Failed to stop container: {e}");
                had_error = true;
            }
        }
        
        if had_error {
            Err(Error::Generic("One or more containers failed to stop".to_string()))
        } else {
            Ok(())
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
        index: u32,
    ) -> Result<ContainerCreateResponse, Error> {
        let container_name = format!("lading_container_{index:05}");
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
