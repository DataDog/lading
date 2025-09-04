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
use lading_throttle::{Config as ThrottleConfig, Throttle};
use metrics::counter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use super::{General, common::MetricsBuilder};

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
    metric_labels: Vec<(String, String)>,
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
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let concurrent_containers = config.number_of_containers;
        let metric_labels = MetricsBuilder::new("container").with_id(general.id).build();

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

            Some(Throttle::new_with_config(ThrottleConfig::Stable {
                maximum_capacity: ops_per_sec,
                timeout_micros: 0,
            }))
        };

        Ok(Self {
            config: config.clone(),
            concurrent_containers,
            throttle,
            shutdown,
            metric_labels,
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
        let pull_start = Instant::now();
        let create_image_options = CreateImageOptionsBuilder::default()
            .from_image(&full_image)
            .build();

        let mut pull_stream = docker.create_image(Some(create_image_options), None, None);
        let mut last_progress_log = Instant::now();
        while let Some(item) = pull_stream.next().await {
            match item {
                Ok(status) => {
                    // Only log progress every second to avoid spam
                    if let Some(progress) = status.progress {
                        let now = Instant::now();
                        if now.duration_since(last_progress_log).as_secs() >= 1 {
                            let elapsed = pull_start.elapsed();
                            debug!("Pull progress ({elapsed:?}): {progress}");
                            last_progress_log = now;
                        }
                    }
                }
                Err(e) => {
                    let elapsed = pull_start.elapsed();
                    warn!("Pull failed after {elapsed:?}: {e}");
                    let mut labels = self.metric_labels.clone();
                    labels.push(("kind".to_string(), "pull".to_string()));
                    labels.push(("result".to_string(), "failure".to_string()));
                    counter!("operation", &labels).increment(1);
                    return Err(e.into());
                }
            }
        }
        let pull_elapsed = pull_start.elapsed();
        debug!("Image {full_image} ready after {pull_elapsed:?}");
        let mut labels = self.metric_labels.clone();
        labels.push(("kind".to_string(), "pull".to_string()));
        labels.push(("result".to_string(), "success".to_string()));
        counter!("operation", &labels).increment(1);

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
                        if let Ok(container) = create_and_start_container(
                            &docker,
                            &self.config,
                            &full_image,
                            idx,
                            &self.metric_labels,
                        )
                        .await
                        {
                            created.push(container);
                        } else {
                            success = false;
                            break;
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
                        && let Err(e) =
                            stop_and_remove_container(&docker, &old_container, &self.metric_labels)
                                .await
                    {
                        warn!("Failed to stop old container {index}: {e}");
                    }

                    let result = create_and_start_container(
                        &docker,
                        &self.config,
                        &full_image,
                        index,
                        &self.metric_labels,
                    )
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
                        let _ = stop_and_remove_container(&docker, container, &self.metric_labels)
                            .await;
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
    metric_labels: &[(String, String)],
) -> Result<ContainerCreateResponse, Error> {
    let container_name = format!("lading_container_{index:05}");
    debug!("Creating container: {container_name}");

    let create_options = CreateContainerOptionsBuilder::default()
        .name(&container_name)
        .build();

    let create_start = Instant::now();
    let container = match docker
        .create_container(Some(create_options), config.to_container_config(full_image))
        .await
    {
        Ok(c) => {
            let elapsed = create_start.elapsed();
            debug!(
                "Created container {container_name} with id {id} in {elapsed:?}",
                id = c.id
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "create".to_string()));
            labels.push(("result".to_string(), "success".to_string()));
            counter!("operation", &labels).increment(1);
            c
        }
        Err(e) => {
            let elapsed = create_start.elapsed();
            warn!("Failed to create container {container_name} after {elapsed:?}: {e}");
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "create".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            return Err(e.into());
        }
    };

    for warning in &container.warnings {
        warn!("Container {container_name} warning: {warning}");
    }

    let start_start = Instant::now();
    match docker
        .start_container(&container.id, None::<StartContainerOptions>)
        .await
    {
        Ok(()) => {
            let elapsed = start_start.elapsed();
            debug!(
                "Started container {container_name} (id: {id}) in {elapsed:?}",
                id = container.id
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "start".to_string()));
            labels.push(("result".to_string(), "success".to_string()));
            counter!("operation", &labels).increment(1);
        }
        Err(e) => {
            let elapsed = start_start.elapsed();
            warn!(
                "Failed to start container {container_name} (id: {id}) after {elapsed:?}: {e}",
                id = container.id
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "start".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            // Try to clean up the created but not started container
            let remove_options = RemoveContainerOptionsBuilder::default().force(true).build();
            let _ = docker
                .remove_container(&container.id, Some(remove_options))
                .await;
            return Err(e.into());
        }
    }

    let total_elapsed = create_start.elapsed();
    debug!("Container {container_name} fully operational in {total_elapsed:?}");

    Ok(container)
}

async fn stop_and_remove_container(
    docker: &Docker,
    container: &ContainerCreateResponse,
    metric_labels: &[(String, String)],
) -> Result<(), Error> {
    let total_start = Instant::now();
    debug!("Stopping container: {id}", id = container.id);

    // Stop with 5 second timeout
    let stop_options = StopContainerOptionsBuilder::default().t(5).build();
    let stop_start = Instant::now();
    match docker
        .stop_container(&container.id, Some(stop_options))
        .await
    {
        Ok(()) => {
            let elapsed = stop_start.elapsed();
            debug!("Stopped container {id} in {elapsed:?}", id = container.id);
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "stop".to_string()));
            labels.push(("result".to_string(), "success".to_string()));
            counter!("operation", &labels).increment(1);
        }
        Err(e) => {
            let elapsed = stop_start.elapsed();
            warn!(
                "Failed to stop container {id} after {elapsed:?}: {e} (will force remove)",
                id = container.id
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "stop".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            // Continue to removal even if stop failed - force flag will handle it
        }
    }

    debug!("Removing container: {id}", id = container.id);
    let remove_start = Instant::now();
    let remove_options = RemoveContainerOptionsBuilder::default().force(true).build();
    match docker
        .remove_container(&container.id, Some(remove_options))
        .await
    {
        Ok(()) => {
            let elapsed = remove_start.elapsed();
            let total_elapsed = total_start.elapsed();
            debug!(
                "Removed container {id} in {elapsed:?} (total teardown: {total_elapsed:?})",
                id = container.id
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "remove".to_string()));
            labels.push(("result".to_string(), "success".to_string()));
            counter!("operation", &labels).increment(1);
        }
        Err(e) => {
            let elapsed = remove_start.elapsed();
            let total_elapsed = total_start.elapsed();
            warn!(
                "Failed to remove container {id} after {elapsed:?} (total: {total_elapsed:?}): {e}",
                id = container.id
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "remove".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            return Err(e.into());
        }
    }

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
