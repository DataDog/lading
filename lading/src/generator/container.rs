//! The container generator
//!
//! This generator is meant to spin up a container from a configured image. For now,
//! it does not actually do anything beyond logging that it's running and then waiting
//! for a shutdown signal.

use bollard::Docker;
use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, RemoveContainerOptions,
    StartContainerOptions, StopContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::secret::ContainerCreateResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::General;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
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
    /// Number of containers to spin up (defaults to 1)
    pub number_of_containers: Option<u32>,
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
}

#[derive(Debug)]
/// Represents a container that can be spun up from a configured image.
pub struct Container {
    image: String,
    tag: String,
    args: Option<Vec<String>>,
    env: Option<Vec<String>>,
    labels: Option<HashMap<String, String>>,
    network_disabled: Option<bool>,
    exposed_ports: Option<Vec<String>>,
    number_of_containers: usize,
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
            image: config.repository.clone(),
            tag: config.tag.clone(),
            args: config.args.clone(),
            env: config.env.clone(),
            labels: config.labels.clone(),
            network_disabled: config.network_disabled,
            exposed_ports: config.exposed_ports.clone(),
            number_of_containers: config.number_of_containers.unwrap_or(1) as usize,
            shutdown,
        })
    }

    /// Convert the `Container` instance to a `ContainerConfig` for the Docker API.
    #[must_use]
    fn to_container_config<'a>(&'a self, full_image: &'a str) -> ContainerConfig<&'a str> {
        ContainerConfig {
            image: Some(full_image),
            tty: Some(true),
            cmd: self
                .args
                .as_ref()
                .map(|args| args.iter().map(String::as_str).collect()),
            env: self
                .env
                .as_ref()
                .map(|env| env.iter().map(String::as_str).collect()),
            labels: self.labels.as_ref().map(|labels| {
                labels
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect()
            }),
            network_disabled: self.network_disabled,
            #[allow(clippy::zero_sized_map_values)]
            exposed_ports: self.exposed_ports.as_ref().map(|ports| {
                ports
                    .iter()
                    .map(|port| (port.as_str(), HashMap::new()))
                    .collect()
            }),
            ..Default::default()
        }
    }

    /// Run the `Container` generator.
    ///
    /// # Errors
    ///
    /// Will return an error if Docker connection fails, image pulling fails,
    /// container creation fails, container start fails, or container removal fails.
    ///
    /// Steps:
    /// 1. Connect to Docker.
    /// 2. Pull the specified image (if not available).
    /// 3. Create and start the container.
    /// 4. Wait for shutdown signal.
    /// 5. On shutdown, stop and remove the container.
    pub async fn spin(self) -> Result<(), Error> {
        info!("Container generator running: {}:{}", self.image, self.tag);

        let docker = Docker::connect_with_local_defaults()?;

        let full_image = format!("{}:{}", self.image, self.tag);
        debug!("Ensuring image is available: {full_image}");

        // Pull the image
        let mut pull_stream = docker.create_image(
            Some(CreateImageOptions::<String> {
                from_image: full_image.clone(),
                ..Default::default()
            }),
            None,
            None,
        );

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

        let mut containers = Vec::with_capacity(self.number_of_containers);

        for _ in 0..self.number_of_containers {
            let container_name = format!("lading_container_{}", Uuid::new_v4());
            debug!("Creating container: {container_name}");

            let container = docker
                .create_container(
                    Some(CreateContainerOptions {
                        name: &container_name,
                        platform: None,
                    }),
                    self.to_container_config(&full_image),
                )
                .await?;

            debug!("Created container with id: {id}", id = container.id);
            for warning in &container.warnings {
                warn!("Container warning: {warning}");
            }

            containers.push(container);
        }

        for container in &containers {
            docker
                .start_container(&container.id, None::<StartContainerOptions<String>>)
                .await?;

            debug!("Started container: {id}", id = container.id);
        }

        // Wait for shutdown signal
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                // Check that containers are still running every 10 seconds
                _ = interval.tick() => {
                    for container in &containers {
                        if let Some(state) = docker.inspect_container(&container.id, None).await?.state {
                            if !state.running.unwrap_or(false) {
                                return Err(Error::Generic(format!(
                                    "Container {id} is not running anymore",
                                    id = container.id
                                )));
                            }
                        }
                    }
                }
                () = &mut shutdown_wait => {
                    debug!("shutdown signal received");
                    for container in &containers {
                        Self::stop_and_remove_container(&docker, container).await?;
                    }

                    return Ok(())
                }
            }
        }
    }

    async fn stop_and_remove_container(
        docker: &Docker,
        container: &ContainerCreateResponse,
    ) -> Result<(), Error> {
        info!("Stopping container: {id}", id = container.id);
        if let Err(e) = docker
            .stop_container(&container.id, Some(StopContainerOptions { t: 5 }))
            .await
        {
            warn!("Error stopping container {id}: {e}", id = container.id);
        }

        debug!("Removing container: {id}", id = container.id);
        docker
            .remove_container(
                &container.id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await?;

        debug!("Removed container: {id}", id = container.id);
        Ok(())
    }
}
