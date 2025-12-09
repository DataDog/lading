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
use lading_throttle::Throttle;
use metrics::counter;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::{collections::HashMap, process};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

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
    /// Error pulling container image
    #[error("Failed to pull image {image}: {source}")]
    ImagePull {
        /// Image name that failed to pull
        image: String,
        /// Underlying bollard error
        #[source]
        source: Box<bollard::errors::Error>,
    },
    /// Error creating container
    #[error("Failed to create container {name} (index {index}): {source}")]
    ContainerCreate {
        /// Container name
        name: String,
        /// Container index
        index: u32,
        /// Underlying bollard error
        #[source]
        source: Box<bollard::errors::Error>,
    },
    /// Error starting container
    #[error("Failed to start container {name} (id: {id}, index {index}): {source}")]
    ContainerStart {
        /// Container name
        name: String,
        /// Container ID
        id: String,
        /// Container index
        index: u32,
        /// Underlying bollard error
        #[source]
        source: Box<bollard::errors::Error>,
    },
    /// Error removing container
    #[error("Failed to remove container {name} (id: {id}) during {operation}: {source}")]
    ContainerRemove {
        /// Container name
        name: String,
        /// Container ID
        id: String,
        /// Operation being performed when removal failed
        operation: LifecycleOp,
        /// Underlying bollard error
        #[source]
        source: Box<bollard::errors::Error>,
    },
}

/// Information about a created container
#[derive(Debug, Clone)]
struct ContainerInfo {
    response: ContainerCreateResponse,
    name: String,
}

/// Operation context for container lifecycle operations
#[derive(Debug, Copy, Clone)]
pub enum LifecycleOp {
    /// Container is being recycled due to max lifetime
    Recycle,
    /// Container is being stopped during shutdown
    Shutdown,
}

impl std::fmt::Display for LifecycleOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recycle => write!(f, "recycle"),
            Self::Shutdown => write!(f, "shutdown"),
        }
    }
}

/// Represents a container that can be spun up from a configured image.
#[derive(Debug)]
pub struct Container {
    config: Config,
    concurrent_containers: NonZeroU32,
    throttle: Option<Throttle>,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
    containers: HashMap<u32, ContainerInfo>,
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
            // We want to recycle all containers over the lifetime period, so we
            // need concurrent_containers operations over lifetime_seconds. This
            // gives us operations_per_second = concurrent_containers /
            // lifetime_seconds.
            let ops = concurrent_containers
                .get()
                .saturating_div(config.max_lifetime_seconds.get());
            let ops_per_sec = NonZeroU32::new(ops).unwrap_or(NonZeroU32::MIN);

            Some(Throttle::new_with_config(lading_throttle::Config::Stable {
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
            containers: HashMap::new(),
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
                Err(source) => {
                    let elapsed = pull_start.elapsed();
                    error!("Failed to pull image {full_image} after {elapsed:?}: {source}");
                    let mut labels = self.metric_labels.clone();
                    labels.push(("kind".to_string(), "pull".to_string()));
                    labels.push(("result".to_string(), "failure".to_string()));
                    counter!("operation", &labels).increment(1);
                    return Err(Error::ImagePull {
                        image: full_image,
                        source: Box::new(source),
                    });
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
                        for (idx, container_info) in created.into_iter().enumerate() {
                            let idx_u32 = u32::try_from(idx).expect("container index overflow");
                            self.containers.insert(idx_u32, container_info);
                        }
                        debug_assert_eq!(
                            self.containers.len(),
                            self.concurrent_containers.get() as usize,
                            "containers HashMap size mismatch"
                        );
                        Event::AllContainersReady
                    } else {
                        error!("Failed to create all containers during initialization");
                        Event::ShutdownSignaled
                    }
                }
                Operation::RecycleContainer { index } => {
                    debug!("Recycling container {index}");

                    if let Some(old_container) = self.containers.remove(&index)
                        && let Err(e) = stop_and_remove_container(
                            &docker,
                            &old_container,
                            LifecycleOp::Recycle,
                            &self.metric_labels,
                        )
                        .await
                    {
                        error!("Failed to remove old container during recycle {index}: {e}");
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
                        self.containers.insert(index, new_container);
                    } else {
                        error!("Failed to create new container {index}");
                    }

                    debug_assert!(
                        self.containers.len() <= self.concurrent_containers.get() as usize,
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
                    for container in self.containers.values() {
                        let _ = stop_and_remove_container(
                            &docker,
                            container,
                            LifecycleOp::Shutdown,
                            &self.metric_labels,
                        )
                        .await;
                    }
                    self.containers.clear();
                    Event::AllContainersStopped
                }
                Operation::Exit => return Ok(()),
            };
        }
    }
}

/// Check if a bollard error represents a "not found" error (404). These errors
/// are treated as success for idempotent operations.
fn is_not_found_error(error: &bollard::errors::Error) -> bool {
    matches!(
        error,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

async fn create_and_start_container(
    docker: &Docker,
    config: &Config,
    full_image: &str,
    index: u32,
    metric_labels: &[(String, String)],
) -> Result<ContainerInfo, Error> {
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
        Err(source) => {
            let elapsed = create_start.elapsed();
            error!(
                "Failed to create container {container_name} (index {index}) after {elapsed:?}: {source}"
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "create".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            return Err(Error::ContainerCreate {
                name: container_name,
                index,
                source: Box::new(source),
            });
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
        Err(source) => {
            let elapsed = start_start.elapsed();
            error!(
                "Failed to start container {container_name} (id: {id}, index {index}) after {elapsed:?}: {source}",
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
            return Err(Error::ContainerStart {
                name: container_name,
                id: container.id,
                index,
                source: Box::new(source),
            });
        }
    }

    let total_elapsed = create_start.elapsed();
    debug!("Container {container_name} fully operational in {total_elapsed:?}");

    Ok(ContainerInfo {
        response: container,
        name: container_name,
    })
}

async fn stop_and_remove_container(
    docker: &Docker,
    container: &ContainerInfo,
    operation: LifecycleOp,
    metric_labels: &[(String, String)],
) -> Result<(), Error> {
    let total_start = Instant::now();
    let container_id = &container.response.id;
    let container_name = &container.name;

    debug!("Stopping container {container_name} (id: {container_id}) during {operation}");

    // Stop with 5 second timeout
    let stop_options = StopContainerOptionsBuilder::default().t(5).build();
    let stop_start = Instant::now();
    match docker
        .stop_container(container_id, Some(stop_options))
        .await
    {
        Ok(()) => {
            let elapsed = stop_start.elapsed();
            debug!("Stopped container {container_name} (id: {container_id}) in {elapsed:?}");
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "stop".to_string()));
            labels.push(("result".to_string(), "success".to_string()));
            counter!("operation", &labels).increment(1);
        }
        Err(e) => {
            // Treat 404 as success. The container is already gone, satisfying
            // our goal. Makes stop operations idempotent and handles race
            // conditions where containers disappear between state checks.
            if is_not_found_error(&e) {
                debug!(
                    "Container {container_name} (id: {container_id}) already gone during {operation} - treating as success"
                );
                let mut labels = metric_labels.to_vec();
                labels.push(("kind".to_string(), "stop".to_string()));
                labels.push(("result".to_string(), "not_found".to_string()));
                counter!("operation", &labels).increment(1);
                return Ok(());
            }

            let elapsed = stop_start.elapsed();
            error!(
                "Failed to stop container {container_name} (id: {container_id}) during {operation} after {elapsed:?}: {e} (will force remove)"
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "stop".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            // Continue to removal even if stop failed - force flag will handle it
        }
    }

    debug!("Removing container {container_name} (id: {container_id}) during {operation}");
    let remove_start = Instant::now();
    let remove_options = RemoveContainerOptionsBuilder::default().force(true).build();
    match docker
        .remove_container(container_id, Some(remove_options))
        .await
    {
        Ok(()) => {
            let elapsed = remove_start.elapsed();
            let total_elapsed = total_start.elapsed();
            debug!(
                "Removed container {container_name} (id: {container_id}) in {elapsed:?} (total teardown: {total_elapsed:?})"
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "remove".to_string()));
            labels.push(("result".to_string(), "success".to_string()));
            counter!("operation", &labels).increment(1);
        }
        Err(source) => {
            // Treat 404 as success. The container is already gone, satisfying
            // our goal. Makes stop operations idempotent and handles race
            // conditions where containers disappear between state checks.
            if is_not_found_error(&source) {
                let elapsed = remove_start.elapsed();
                let total_elapsed = total_start.elapsed();
                debug!(
                    "Container {container_name} (id: {container_id}) already removed during {operation} in {elapsed:?} (total: {total_elapsed:?}) - treating as success"
                );
                let mut labels = metric_labels.to_vec();
                labels.push(("kind".to_string(), "remove".to_string()));
                labels.push(("result".to_string(), "not_found".to_string()));
                counter!("operation", &labels).increment(1);
                return Ok(());
            }

            let elapsed = remove_start.elapsed();
            let total_elapsed = total_start.elapsed();
            error!(
                "Failed to remove container {container_name} (id: {container_id}) during {operation} after {elapsed:?} (total: {total_elapsed:?}): {source}"
            );
            let mut labels = metric_labels.to_vec();
            labels.push(("kind".to_string(), "remove".to_string()));
            labels.push(("result".to_string(), "failure".to_string()));
            counter!("operation", &labels).increment(1);
            return Err(Error::ContainerRemove {
                name: container_name.clone(),
                id: container_id.clone(),
                operation,
                source: Box::new(source),
            });
        }
    }

    Ok(())
}

impl Drop for Container {
    fn drop(&mut self) {
        if self.containers.is_empty() {
            return;
        }

        info!(
            "Cleaning up {count} containers on drop",
            count = self.containers.len()
        );

        // Clean up containers using synchronous docker command
        for container_info in self.containers.values() {
            let container_id = &container_info.response.id;
            let container_name = &container_info.name;

            match process::Command::new("docker")
                .arg("rm")
                .arg("--force")
                .arg("--volumes")
                .arg(container_id)
                .output()
            {
                Ok(output) => {
                    if output.status.success() {
                        debug!(
                            "Successfully removed container {container_name} (id: {container_id}) on drop"
                        );
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        if stderr.contains("No such container") {
                            debug!(
                                "Container {container_name} (id: {container_id}) already gone on drop - treating as success"
                            );
                        } else {
                            error!(
                                "Failed to remove container {container_name} (id: {container_id}) on drop: {stderr}"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to execute docker rm for container {container_name} (id: {container_id}) on drop: {e}"
                    );
                }
            }
        }
    }
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
