//! The Kubernetes generator
//!
//! This generator is meant to generate Kubernetes objects.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
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
                    delete_and_recreate_resource(client.clone(), &self.resource, self.number_of_instances.get(), i).await?;
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

fn set_object_name<T>(object: &mut T, number_of_instances: u32, instance_index: u32)
where
    T: kube::Resource + k8s_openapi::Resource,
{
    #[allow(clippy::unwrap_used)]
    if number_of_instances > 1 {
        // TODO implement a visitor to patch other fields
        object.meta_mut().name = Some(object.meta().name.as_ref().map_or_else(
            || format!("lading-{}-{instance_index:0>5}", T::KIND.to_lowercase()),
            |name| format!("{name}-{instance_index:0>5}"),
        ));
    } else if object.meta().name.is_none() {
        object.meta_mut().name = Some(format!("lading-{}", T::KIND.to_lowercase()));
    }
}

fn get_object_name<T>(object: &T) -> &str
where
    T: kube::Resource,
{
    object
        .meta()
        .name
        .as_ref()
        .expect("Do not forget to call `set_object_name`")
}

async fn create_resources_for<T>(
    api: kube::Api<T>,
    resource: &T,
    number_of_instances: u32,
) -> Result<Vec<T>, Error>
where
    T: kube::Resource
        + k8s_openapi::Resource
        + Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    <T as kube::Resource>::DynamicType: std::default::Default,
{
    let mut set = JoinSet::new();

    for instance_index in 0..number_of_instances {
        let api = api.clone();
        let mut object = resource.clone();
        set_object_name(&mut object, number_of_instances, instance_index);

        set.spawn(async move {
            api.create(&kube::api::PostParams::default(), &object)
                .await
                .inspect(|o| {
                    debug!("Created {} {}", T::KIND.to_lowercase(), get_object_name(o));
                })
                .inspect_err(|e| {
                    warn!(
                        "Failed to create {} {}: {e}",
                        T::KIND.to_lowercase(),
                        get_object_name(&object)
                    );
                })
        });
    }

    Ok(set
        .join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<T>, kube::Error>>()?)
}

async fn delete_and_recreate_resource_for<T>(
    api: kube::Api<T>,
    resource: &T,
    number_of_instances: u32,
    instance_index: u32,
) -> Result<(), Error>
where
    T: kube::Resource
        + k8s_openapi::Resource
        + Clone
        + Serialize
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let mut object = resource.clone();
    set_object_name(&mut object, number_of_instances, instance_index);

    tokio::spawn(async move {
        let mut deletion_in_progress = false;

        api.delete(
            get_object_name(&object),
            &kube::api::DeleteParams::default().grace_period(5),
        )
        .await
        .inspect_err(|e| {
            warn!(
                "Failed to delete {} {}: {e}",
                T::KIND.to_lowercase(),
                get_object_name(&object)
            );
        })?
        .map_left(|o| {
            debug!(
                "Deleting {} {}",
                T::KIND.to_lowercase(),
                get_object_name(&o)
            );
            deletion_in_progress = true;
        })
        .map_right(|_| {
            debug!(
                "Deleted {} {}",
                T::KIND.to_lowercase(),
                get_object_name(&object)
            );
        });

        while deletion_in_progress {
            if let Some(o) = api
                .get_opt(get_object_name(&object))
                .await
                .inspect_err(|e| {
                    warn!(
                        "Failed to delete {} {}: {e}",
                        T::KIND.to_lowercase(),
                        get_object_name(&object)
                    );
                })?
            {
                debug!(
                    "Still deleting {} {}",
                    T::KIND.to_lowercase(),
                    get_object_name(&o)
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            } else {
                debug!(
                    "Deleted {} {}",
                    T::KIND.to_lowercase(),
                    get_object_name(&object)
                );
                deletion_in_progress = false;
            }
        }

        api.create(&kube::api::PostParams::default(), &object)
            .await
            .inspect(|o| {
                debug!(
                    "Recreated {} {}",
                    T::KIND.to_lowercase(),
                    get_object_name(o)
                );
            })
            .inspect_err(|e| {
                warn!(
                    "Failed to recreate {} {}: {e}",
                    T::KIND.to_lowercase(),
                    get_object_name(&object)
                );
            })?;

        Ok::<(), Error>(())
    });

    Ok(())
}

async fn delete_resources_for<T>(
    api: kube::Api<T>,
    resource: &T,
    number_of_instances: u32,
) -> Result<(), Error>
where
    T: kube::Resource
        + k8s_openapi::Resource
        + Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    <T as kube::Resource>::DynamicType: std::default::Default,
{
    let mut set = JoinSet::new();

    for instance_index in 0..number_of_instances {
        let api = api.clone();
        let mut object = resource.clone();
        set_object_name(&mut object, number_of_instances, instance_index);

        set.spawn(async move {
            api.delete(
                get_object_name(&object),
                &kube::api::DeleteParams::default().grace_period(5),
            )
            .await
            .inspect_err(|e| {
                warn!(
                    "Failed to delete {} {}: {e}",
                    T::KIND.to_lowercase(),
                    get_object_name(&object)
                );
            })?
            .map_left(|o| {
                debug!(
                    "Deleting {} {}",
                    T::KIND.to_lowercase(),
                    get_object_name(&o)
                );
            })
            .map_right(|_| {
                debug!(
                    "Deleted {} {}",
                    T::KIND.to_lowercase(),
                    get_object_name(&object)
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

macro_rules! create_api {
    (namespaced, $client:expr, $type:ty, $object:expr) => {{
        let namespace = $object
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| $client.default_namespace().to_string());
        kube::Api::namespaced($client, &namespace)
    }};
    (cluster_level, $client:expr, $type:ty, $object:expr) => {{ kube::Api::all($client) }};
}

macro_rules! manage_resources {
    ($func_name:ident, $generic_func_name:ident, $($param_name:ident: $param_type:ty),*) => {
        async fn $func_name(
            client: kube::Client,
            resource: &Resource,
            $($param_name: $param_type),*
        ) -> Result<(), Error> {
            match &resource {
                Resource::Node(node) => {
                    let api = create_api!(
                        cluster_level,
                        client,
                        k8s_openapi::api::core::v1::Node,
                        node
                    );
                    $generic_func_name(api, node, $($param_name),*).await?;
                }
                Resource::Namespace(ns) => {
                    let api = create_api!(
                        cluster_level,
                        client,
                        k8s_openapi::api::core::v1::Namespace,
                        ns
                    );
                    $generic_func_name(api, ns, $($param_name),*).await?;
                }
                Resource::Pod(pod) => {
                    let api = create_api!(namespaced, client, k8s_openapi::api::core::v1::Pod, pod);
                    $generic_func_name(api, pod, $($param_name),*).await?;
                }
                Resource::Deployment(deploy) => {
                    let api = create_api!(
                        namespaced,
                        client,
                        k8s_openapi::api::apps::v1::Deployment,
                        deploy
                    );
                    $generic_func_name(api, deploy, $($param_name),*).await?;
                }
                Resource::Service(svc) => {
                    let api =
                        create_api!(namespaced, client, k8s_openapi::api::core::v1::Service, svc);
                    $generic_func_name(api, svc, $($param_name),*).await?;
                }
            };

            Ok(())
        }
    };
}

manage_resources!(create_resources, create_resources_for, number_of_instances: u32);
manage_resources!(delete_and_recreate_resource, delete_and_recreate_resource_for, number_of_instances: u32, instance_index: u32);
manage_resources!(delete_resources, delete_resources_for, number_of_instances: u32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_object_name() {
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

            set_object_name(&mut pod, num_instances, index);
            assert_eq!(get_object_name(&pod), expected_name);
        }
    }
}
