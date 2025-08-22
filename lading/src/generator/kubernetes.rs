//! The Kubernetes generator
//!
//! This generator is meant to generate Kubernetes objects.

use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope, Resource as KubeResource};
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
    /// Number of resource instances to create (defaults to 1)
    pub number_of_instances: Option<std::num::NonZeroU32>,
}

/// Manifest of the Kubernetes resource to create
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Manifest {
    /// Node manifest
    Node(String),
    /// Namespace manifest
    Namespace(String),
    /// Pod manifest
    Pod(String),
    /// Deployment manifest
    Deployment(String),
    /// Service manifest
    Service(String),
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
    number_of_instances: std::num::NonZeroU32,
    shutdown: lading_signal::Watcher,
}

/// Represents a Kubernetes resource type
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Resource {
    /// Kubernetes Node resource
    Node(k8s_openapi::api::core::v1::Node),
    /// Kubernetes Namespace resource
    Namespace(k8s_openapi::api::core::v1::Namespace),
    /// Kubernetes Pod resource
    Pod(k8s_openapi::api::core::v1::Pod),
    /// Kubernetes Deployment resource
    Deployment(k8s_openapi::api::apps::v1::Deployment),
    /// Kubernetes Service resource
    Service(k8s_openapi::api::core::v1::Service),
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
                Manifest::Node(s) => Resource::Node(serde_yaml::from_str(s)?),
                Manifest::Namespace(s) => Resource::Namespace(serde_yaml::from_str(s)?),
                Manifest::Pod(s) => Resource::Pod(serde_yaml::from_str(s)?),
                Manifest::Deployment(s) => Resource::Deployment(serde_yaml::from_str(s)?),
                Manifest::Service(s) => Resource::Service(serde_yaml::from_str(s)?),
            },
            max_lifetime_seconds: config.max_lifetime_seconds,
            #[allow(clippy::unwrap_used)]
            number_of_instances: config
                .number_of_instances
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

        create_resources(
            client.clone(),
            &self.resource,
            self.number_of_instances.get(),
        )
        .await?;

        // Wait for the shutdown signal
        let shutdown_wait = self.shutdown.clone().recv();
        tokio::pin!(shutdown_wait);
        let mut recreate_interval = self.max_lifetime_seconds.map(|max_lifetime_seconds| {
            tokio::time::interval(
                tokio::time::Duration::from_secs(max_lifetime_seconds.into())
                    / self.number_of_instances.get(),
            )
        });

        let mut i = 0_u32;
        loop {
            tokio::select! {
                // Delete and recreate resources
                _ = if let Some(ref mut interval) = recreate_interval { interval.tick() } else { std::future::pending().await } => {
                    delete_and_recreate_resource(client.clone(), &self.resource, self.number_of_instances.get(), i);
                    i = (i + 1) % self.number_of_instances;
                }

                // Shutdown
                () = &mut shutdown_wait => {
                    debug!("shutdown signal received");

                    delete_resources(
                        client.clone(),
                        &self.resource,
                        self.number_of_instances.get(),
                    ).await?;

                    return Ok(())
                }
            }
        }
    }
}

impl Resource {
    fn kind(&self) -> &'static str {
        match self {
            Resource::Node(_) => k8s_openapi::api::core::v1::Node::KIND,
            Resource::Namespace(_) => k8s_openapi::api::core::v1::Namespace::KIND,
            Resource::Pod(_) => k8s_openapi::api::core::v1::Pod::KIND,
            Resource::Deployment(_) => k8s_openapi::api::apps::v1::Deployment::KIND,
            Resource::Service(_) => k8s_openapi::api::core::v1::Service::KIND,
        }
    }

    fn meta(&self) -> &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        match self {
            Resource::Node(node) => &node.metadata,
            Resource::Namespace(ns) => &ns.metadata,
            Resource::Pod(pod) => &pod.metadata,
            Resource::Deployment(deploy) => &deploy.metadata,
            Resource::Service(svc) => &svc.metadata,
        }
    }

    fn meta_mut(&mut self) -> &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        match self {
            Resource::Node(node) => &mut node.metadata,
            Resource::Namespace(ns) => &mut ns.metadata,
            Resource::Pod(pod) => &mut pod.metadata,
            Resource::Deployment(deploy) => &mut deploy.metadata,
            Resource::Service(svc) => &mut svc.metadata,
        }
    }

    fn set_name(&mut self, number_of_instances: u32, instance_index: u32) {
        if number_of_instances > 1 {
            // TODO implement a visitor to patch other fields
            self.meta_mut().name = Some(self.meta().name.as_ref().map_or_else(
                || format!("lading-{}-{instance_index:0>5}", self.kind().to_lowercase()),
                |name| format!("{name}-{instance_index:0>5}"),
            ));
        } else if self.meta().name.is_none() {
            self.meta_mut().name = Some(format!("lading-{}", self.kind().to_lowercase()));
        }
    }

    fn get_name(&self) -> &str {
        self.meta()
            .name
            .as_ref()
            .expect("Do not forget to call `set_name`")
    }

    /// Create a cluster-scoped API for the given resource type
    fn cluster_api<T>(client: kube::Client) -> kube::Api<T>
    where
        T: kube::Resource<Scope = ClusterResourceScope>,
        <T as kube::Resource>::DynamicType: std::default::Default,
    {
        kube::Api::all(client)
    }

    /// Create a namespaced API for the given resource type
    fn namespaced_api<T>(&self, client: kube::Client) -> kube::Api<T>
    where
        T: kube::Resource<Scope = NamespaceResourceScope>,
        <T as kube::Resource>::DynamicType: std::default::Default,
    {
        let namespace = self
            .meta()
            .namespace
            .clone()
            .unwrap_or_else(|| client.default_namespace().to_string());
        kube::Api::namespaced(client, &namespace)
    }

    async fn create(
        &self,
        client: kube::Client,
        pp: &kube::api::PostParams,
    ) -> Result<Self, kube::Error> {
        match self {
            Resource::Node(node) => Self::cluster_api::<k8s_openapi::api::core::v1::Node>(client)
                .create(pp, node)
                .await
                .map(Resource::Node),

            Resource::Namespace(ns) => {
                Self::cluster_api::<k8s_openapi::api::core::v1::Namespace>(client)
                    .create(pp, ns)
                    .await
                    .map(Resource::Namespace)
            }

            Resource::Pod(pod) => self
                .namespaced_api::<k8s_openapi::api::core::v1::Pod>(client)
                .create(pp, pod)
                .await
                .map(Resource::Pod),

            Resource::Deployment(deploy) => self
                .namespaced_api::<k8s_openapi::api::apps::v1::Deployment>(client)
                .create(pp, deploy)
                .await
                .map(Resource::Deployment),

            Resource::Service(svc) => self
                .namespaced_api::<k8s_openapi::api::core::v1::Service>(client)
                .create(pp, svc)
                .await
                .map(Resource::Service),
        }
    }

    async fn delete(
        &self,
        client: kube::Client,
        dp: &kube::api::DeleteParams,
    ) -> Result<either::Either<Self, kube_core::response::Status>, kube::Error> {
        match self {
            Resource::Node(_) => Self::cluster_api::<k8s_openapi::api::core::v1::Node>(client)
                .delete(self.get_name(), dp)
                .await
                .map(|e| e.map_left(Resource::Node)),

            Resource::Namespace(_) => {
                Self::cluster_api::<k8s_openapi::api::core::v1::Namespace>(client)
                    .delete(self.get_name(), dp)
                    .await
                    .map(|e| e.map_left(Resource::Namespace))
            }

            Resource::Pod(_) => self
                .namespaced_api::<k8s_openapi::api::core::v1::Pod>(client)
                .delete(self.get_name(), dp)
                .await
                .map(|e| e.map_left(Resource::Pod)),

            Resource::Deployment(_) => self
                .namespaced_api::<k8s_openapi::api::apps::v1::Deployment>(client)
                .delete(self.get_name(), dp)
                .await
                .map(|e| e.map_left(Resource::Deployment)),

            Resource::Service(_) => self
                .namespaced_api::<k8s_openapi::api::core::v1::Service>(client)
                .delete(self.get_name(), dp)
                .await
                .map(|e| e.map_left(Resource::Service)),
        }
    }

    async fn get_opt(&self, client: kube::Client) -> Result<Option<Self>, kube::Error> {
        match self {
            Resource::Node(_) => Self::cluster_api::<k8s_openapi::api::core::v1::Node>(client)
                .get_opt(self.get_name())
                .await
                .map(|o| o.map(Resource::Node)),

            Resource::Namespace(_) => {
                Self::cluster_api::<k8s_openapi::api::core::v1::Namespace>(client)
                    .get_opt(self.get_name())
                    .await
                    .map(|o| o.map(Resource::Namespace))
            }

            Resource::Pod(_) => self
                .namespaced_api::<k8s_openapi::api::core::v1::Pod>(client)
                .get_opt(self.get_name())
                .await
                .map(|o| o.map(Resource::Pod)),

            Resource::Deployment(_) => self
                .namespaced_api::<k8s_openapi::api::apps::v1::Deployment>(client)
                .get_opt(self.get_name())
                .await
                .map(|o| o.map(Resource::Deployment)),

            Resource::Service(_) => self
                .namespaced_api::<k8s_openapi::api::core::v1::Service>(client)
                .get_opt(self.get_name())
                .await
                .map(|o| o.map(Resource::Service)),
        }
    }
}

async fn create_resources(
    client: kube::Client,
    resource: &Resource,
    number_of_instances: u32,
) -> Result<Vec<Resource>, Error> {
    let mut set = JoinSet::new();

    for instance_index in 0..number_of_instances {
        let client = client.clone();
        let mut object = resource.clone();
        object.set_name(number_of_instances, instance_index);

        set.spawn(async move {
            object
                .create(client, &kube::api::PostParams::default())
                .await
                .inspect(|o| {
                    debug!("Created {} {}", object.kind().to_lowercase(), o.get_name());
                })
                .inspect_err(|e| {
                    warn!(
                        "Failed to create {} {}: {e}",
                        object.kind().to_lowercase(),
                        object.get_name()
                    );
                })
        });
    }

    Ok(set
        .join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<Resource>, kube::Error>>()?)
}

fn delete_and_recreate_resource(
    client: kube::Client,
    resource: &Resource,
    number_of_instances: u32,
    instance_index: u32,
) {
    let mut object = resource.clone();
    object.set_name(number_of_instances, instance_index);

    tokio::spawn(async move {
        let mut deletion_in_progress = false;

        object
            .delete(
                client.clone(),
                &kube::api::DeleteParams::default().grace_period(5),
            )
            .await
            .inspect_err(|e| {
                warn!(
                    "Failed to delete {} {}: {e}",
                    object.kind().to_lowercase(),
                    object.get_name()
                );
            })?
            .map_left(|o| {
                debug!("Deleting {} {}", o.kind().to_lowercase(), o.get_name());
                deletion_in_progress = true;
            })
            .map_right(|_| {
                debug!(
                    "Deleted {} {}",
                    object.kind().to_lowercase(),
                    object.get_name()
                );
            });

        while deletion_in_progress {
            if let Some(o) = object.get_opt(client.clone()).await.inspect_err(|e| {
                warn!(
                    "Failed to delete {} {}: {e}",
                    object.kind().to_lowercase(),
                    object.get_name()
                );
            })? {
                debug!(
                    "Still deleting {} {}",
                    o.kind().to_lowercase(),
                    o.get_name()
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            } else {
                debug!(
                    "Deleted {} {}",
                    object.kind().to_lowercase(),
                    object.get_name()
                );
                deletion_in_progress = false;
            }
        }

        object
            .create(client, &kube::api::PostParams::default())
            .await
            .inspect(|o| {
                debug!("Recreated {} {}", o.kind().to_lowercase(), o.get_name());
            })
            .inspect_err(|e| {
                warn!(
                    "Failed to recreate {} {}: {e}",
                    object.kind().to_lowercase(),
                    object.get_name()
                );
            })?;

        Ok::<(), Error>(())
    });
}

async fn delete_resources(
    client: kube::Client,
    resource: &Resource,
    number_of_instances: u32,
) -> Result<(), Error> {
    let mut set = JoinSet::new();

    for instance_index in 0..number_of_instances {
        let client = client.clone();
        let mut object = resource.clone();
        object.set_name(number_of_instances, instance_index);

        set.spawn(async move {
            object
                .delete(client, &kube::api::DeleteParams::default().grace_period(5))
                .await
                .inspect_err(|e| {
                    warn!(
                        "Failed to delete {} {}: {e}",
                        object.kind().to_lowercase(),
                        object.get_name()
                    );
                })?
                .map_left(|o| {
                    debug!("Deleting {} {}", o.kind().to_lowercase(), o.get_name());
                })
                .map_right(|_| {
                    debug!(
                        "Deleted {} {}",
                        object.kind().to_lowercase(),
                        object.get_name()
                    );
                });

            Ok(())
        });
    }

    Ok(set
        .join_all()
        .await
        .into_iter()
        .collect::<Result<(), kube::Error>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_name() {
        let test_cases = vec![
            // Case 1: Pod without a name, single instance
            (None, 1, 0, "lading-pod"),
            // Case 2: Pod with a name, single instance
            (Some("test-pod".to_string()), 1, 0, "test-pod"),
            // Case 3: Pod without a name, multiple instances
            (None, 3, 1, "lading-pod-00001"),
            // Case 4: Pod with a name, multiple instances
            (Some("test-pod".to_string()), 3, 2, "test-pod-00002"),
        ];

        for (name, num_instances, index, expected_name) in test_cases {
            let mut pod = k8s_openapi::api::core::v1::Pod::default();
            pod.metadata.name = name;
            let mut object = Resource::Pod(pod);

            object.set_name(num_instances, index);
            assert_eq!(object.get_name(), expected_name);
        }
    }
}
