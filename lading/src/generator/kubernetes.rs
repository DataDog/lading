//! The Kubernetes generator
//!
//! This generator is meant to generate Kubernetes objects.

use k8s_openapi::Resource as _k8sResource;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use super::General;

/// Configuration of the Kubernetes generator.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Kubernetes manifest of the resource to create
    pub manifest: Manifest,
    /// Maximum lifetime of the resource before being replaced
    pub max_lifetime_seconds: Option<std::num::NonZeroU64>,
    /// Number of resource exemplars to create (defaults to 1)
    pub number_of_exemplars: Option<std::num::NonZeroU32>,
}

/// Manifest of the Kubernetes resource to create
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Manifest {
    /// Namespace manifest
    Namespace(String),
    /// Pod manifest
    Pod(String),
    /// Deployment manifest
    Deployment(String),
}

/// Errors produced by the `Kubernetes` generator.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Generic error produced by the kubernetes generator.
    #[error("Generic kubernetes error: {0}")]
    Generic(String),
    /// Error produced by the kube client.
    #[error("Kubernetes client error: {0}")]
    Kube(#[from] kube::Error),
    /// Error produced by the YAML parser.
    #[error("YAML parsing error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

/// Represents a Kubernetes resource
#[derive(Debug)]
pub struct Kubernetes {
    resource: Resource,
    max_lifetime_seconds: Option<std::num::NonZeroU64>,
    number_of_exemplars: std::num::NonZeroU32,
    shutdown: lading_signal::Watcher,
}

/// Represents a Kubernetes resource type
#[derive(Debug, Clone)]
pub enum Resource {
    /// Kubernetes Namespace resource
    Namespace(k8s_openapi::api::core::v1::Namespace),
    /// Kubernetes Pod resource
    Pod(k8s_openapi::api::core::v1::Pod),
    /// Kubernetes Deployment resource
    Deployment(k8s_openapi::api::apps::v1::Deployment),
}

impl Kubernetes {
    /// Create a new `Kubernetes` instance
    ///
    /// # Errors
    ///
    /// # Panics
    ///
    /// Will return an error if the config parsing fails or runtime setup fails
    /// in the future. For now, always succeeds.
    pub fn new(
        _general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        Ok(Self {
            resource: match &config.manifest {
                Manifest::Namespace(s) => Resource::Namespace(serde_yaml::from_str(s)?),
                Manifest::Pod(s) => Resource::Pod(serde_yaml::from_str(s)?),
                Manifest::Deployment(s) => Resource::Deployment(serde_yaml::from_str(s)?),
            },
            max_lifetime_seconds: config.max_lifetime_seconds,
            #[allow(clippy::unwrap_used)]
            number_of_exemplars: config
                .number_of_exemplars
                .unwrap_or(std::num::NonZeroU32::new(1).unwrap()),
            shutdown,
        })
    }

    /// Run the Kubernetes generator
    ///
    /// # Errors
    ///
    /// Will return an error if Kubernetes connection fails or if resource creation fails.
    pub async fn spin(self) -> Result<(), Error> {
        let client = kube::Client::try_default().await?;

        macro_rules! spin {
            (namespaced, $type:ty, $object:expr) => {{
                let namespace = $object
                    .metadata
                    .namespace
                    .clone()
                    .unwrap_or_else(|| client.default_namespace().to_string());
                let api: kube::Api<$type> = kube::Api::namespaced(client.clone(), &namespace);
                spin_inner!($type, $object, api);
            }};
            (cluster_level, $type:ty, $object:expr) => {{
                let api: kube::Api<$type> = kube::Api::all(client.clone());
                spin_inner!($type, $object, api);
            }};
        }

        macro_rules! spin_inner {
            ($type:ty, $object:expr, $api:expr) => {
                let mut set = JoinSet::new();

                for i in 0..self.number_of_exemplars.into() {
                    let api = $api.clone();
                    let mut object = $object.clone();
                    if self.number_of_exemplars > std::num::NonZeroU32::new(1).unwrap() {
                        // TODO implement a visitor to patch other fields
                        object.metadata.name = Some(object.metadata.name.map_or_else(
                            || format!("lading-{}-{i:0>5}", <$type>::KIND.to_lowercase()),
                            |name| format!("{name}-{i:0>5}"),
                        ));
                    }

                    set.spawn(async move {
                        match api.create(&kube::api::PostParams::default(), &object).await {
                            Ok(o) => {
                                debug!(
                                    "Created {} {}",
                                    <$type>::KIND.to_lowercase(),
                                    o.metadata.name.unwrap_or_default()
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to create {} {}: {e}",
                                    <$type>::KIND.to_lowercase(),
                                    object.metadata.name.unwrap_or_default()
                                );
                            }
                        }
                    });
                }

                set.join_all().await;
            };
        }

        match self.resource {
            Resource::Namespace(ref ns) => {
                spin!(cluster_level, k8s_openapi::api::core::v1::Namespace, ns);
            }
            Resource::Pod(ref pod) => spin!(namespaced, k8s_openapi::api::core::v1::Pod, pod),
            Resource::Deployment(ref deploy) => {
                spin!(namespaced, k8s_openapi::api::apps::v1::Deployment, deploy);
            }
        }

        // Wait for the shutdown signal
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        let mut recreate_interval = self.max_lifetime_seconds.map(|max_lifetime_seconds| {
            tokio::time::interval(
                tokio::time::Duration::from_secs(max_lifetime_seconds.into())
                    / <std::num::NonZero<u32> as Into<u32>>::into(self.number_of_exemplars),
            )
        });
        let mut i = 0_u32;

        loop {
            tokio::select! {
                // Delete and recreate resources
                _ = if let Some(ref mut interval) = recreate_interval { interval.tick() } else { std::future::pending().await } => {
                    macro_rules! delete_and_recreate {
                        (namespaced, $type:ty, $object:expr) => {{
                            let namespace = $object
                            .metadata
                            .namespace
                            .clone()
                            .unwrap_or_else(|| client.default_namespace().to_string());
                            let api: kube::Api<$type> = kube::Api::namespaced(client.clone(), &namespace);
                            delete_and_recreate_inner!($type, $object, api);
                        }};
                        (cluster_level, $type:ty, $object:expr) => {{
                            let api: kube::Api<$type> = kube::Api::all(client.clone());
                            delete_and_recreate_inner!($type, $object, api);
                        }};
                    }

                    macro_rules! delete_and_recreate_inner {
                        ($type:ty, $object:expr, $api:expr) => {
                            let api = $api.clone();
                            let mut object = $object.clone();
                            if self.number_of_exemplars > std::num::NonZeroU32::new(1).unwrap() {
                                // TODO implement a visitor to patch other fields
                                object.metadata.name = Some(object.metadata.name.map_or_else(
                                    || format!("lading-{}-{i:0>5}", <$type>::KIND.to_lowercase()),
                                    |name| format!("{name}-{i:0>5}"),
                                ));
                            }

                            tokio::spawn(async move {
                                let mut deletion_in_progress = false;

                                match api.delete(&object.metadata.name.clone().unwrap(), &kube::api::DeleteParams::default().grace_period(5)).await {
                                    Ok(either::Either::Left(o)) => {
                                        debug!("Deleting {} {}", <$type>::KIND.to_lowercase(), o.metadata.name.clone().unwrap_or_default());
                                        deletion_in_progress = true;
                                    },
                                    Ok(either::Either::Right(_)) => {
                                        debug!("Deleted {} {}", <$type>::KIND.to_lowercase(), object.metadata.name.clone().unwrap_or_default());
                                    }
                                    Err(e) => {
                                        warn!("Failed to delete {} {}: {e}", <$type>::KIND.to_lowercase(), object.metadata.name.clone().unwrap_or_default());
                                    }
                                }

                                while deletion_in_progress {
                                    match api.get_opt(&object.metadata.name.clone().unwrap()).await {
                                        Ok(Some(o)) => {
                                            debug!("Still deleting {} {}", <$type>::KIND.to_lowercase(), o.metadata.name.clone().unwrap_or_default());
                                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                        },
                                        Ok(None) => {
                                            debug!("Deleted {} {}", <$type>::KIND.to_lowercase(), object.metadata.name.clone().unwrap_or_default());
                                            deletion_in_progress = false
                                        },
                                        Err(e) => {
                                            warn!("Failed to delete {} {}: {e}", <$type>::KIND.to_lowercase(), object.metadata.name.clone().unwrap_or_default());
                                            break;
                                        }
                                    }
                                }

                                match api.create(&kube::api::PostParams::default(), &object).await {
                                    Ok(o) => debug!("Recreated {} {}", <$type>::KIND.to_lowercase(), o.metadata.name.clone().unwrap_or_default()),
                                    Err(e) => warn!("Failed to recreate {} {}: {e}", <$type>::KIND.to_lowercase(), object.metadata.name.clone().unwrap_or_default())
                                }
                            })
                        }
                    }

                    match self.resource {
                        Resource::Namespace(ref ns) => delete_and_recreate!(cluster_level, k8s_openapi::api::core::v1::Namespace, ns),
                        Resource::Pod(ref pod) => delete_and_recreate!(namespaced, k8s_openapi::api::core::v1::Pod, pod),
                        Resource::Deployment(ref deploy) => delete_and_recreate!(namespaced, k8s_openapi::api::apps::v1::Deployment, deploy),
                    }

                    i = (i + 1) % self.number_of_exemplars;
                }

                // Shutdown
                () = &mut shutdown_wait => {
                    debug!("shutdown signal received");

                    return Ok(())
                }
            }
        }
    }
}
