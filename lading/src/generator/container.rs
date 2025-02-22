//! The container generator
//!
//! This generator is meant to spin up a container from a configured image. For now,
//! it does not actually do anything beyond logging that it's running and then waiting
//! for a shutdown signal.
//!
//! ## Future Work
//! - Pull and run a specified container image
//! - Possibly support metrics about container lifecycle

use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, RemoveContainerOptions,
    StartContainerOptions, StopContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::secret::ContainerCreateResponse;
use bollard::Docker;
use serde::{Deserialize, Serialize};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::{info, warn};
use uuid::Uuid;

use super::General;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of the container generator.
pub struct Config {
    /// The seed for random operations (not currently used, but included for parity)
    //pub seed: [u8; 32],
    /// The container repository (e.g. "library/nginx")
    pub repository: String,
    /// The container image tag (e.g. "latest")
    pub tag: String,
    /// Arguments to provide to the container (docker calls this args, but that's a dumb name)
    pub args: Option<Vec<String>>,
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
    shutdown: lading_signal::Watcher,
}

impl Container {
    /// Create a new `Container` instance
    ///
    /// # Errors
    ///
    /// Will return an error if config parsing fails or runtime setup fails
    /// in the future. For now, always succeeds.
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        Ok(Self {
            image: config.repository.clone(),
            tag: config.tag.clone(),
            args: config.args.clone(),
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
        info!("Ensuring image is available: {}", full_image);

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
                        info!("Pull progress: {}", progress);
                    }
                }
                Err(e) => {
                    warn!("Pull error: {}", e);
                    return Err(e.into());
                }
            }
        }

        let container_name = format!("lading_container_{}", Uuid::new_v4());
        info!("Creating container: {}", container_name);

        let container = docker
            .create_container(
                Some(CreateContainerOptions {
                    name: &container_name,
                    platform: None,
                }),
                ContainerConfig {
                    image: Some(full_image.as_str()),
                    tty: Some(true),
                    cmd: self
                        .args
                        .as_ref()
                        .map(|args| args.iter().map(String::as_str).collect()),
                    ..Default::default()
                },
            )
            .await?;

        info!("Created container with id: {}", container.id);
        for warning in &container.warnings {
            warn!("Container warning: {}", warning);
        }

        docker
            .start_container(&container.id, None::<StartContainerOptions<String>>)
            .await?;

        info!("Started container: {}", container.id);

        // Wait for shutdown signal
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        tokio::select! {
            () = &mut shutdown_wait => {
                info!("shutdown signal received");
                Self::stop_and_remove_container(&docker, &container).await?;

                Ok(())
            }
        }
    }

    async fn stop_and_remove_container(
        docker: &Docker,
        container: &ContainerCreateResponse,
    ) -> Result<(), Error> {
        info!("Stopping container: {}", container.id);
        if let Err(e) = docker
            .stop_container(&container.id, Some(StopContainerOptions { t: 5 }))
            .await
        {
            warn!("Error stopping container {}: {}", container.id, e);
        }

        info!("Removing container: {}", container.id);
        docker
            .remove_container(
                &container.id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await?;

        info!("Removed container: {}", container.id);
        Ok(())
    }
}
