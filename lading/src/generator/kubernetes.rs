//! The Kubernetes generator
//!
//! This generator is meant to generate Kubernetes objects.

use std::num::NonZeroU32;

fn default_concurrent_instances() -> NonZeroU32 {
    NonZeroU32::MIN
}

fn default_max_instance_lifetime_seconds() -> NonZeroU32 {
    NonZeroU32::MAX
}

#[allow(unused_imports)] // Used for Resource enum constructors
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Namespace, Node, Pod, Service},
};
use kube::api::{DeleteParams, PostParams};
use lading_throttle::{self, Throttle};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::generator::General;

mod resource;
mod state_machine;

use self::resource::Resource;

/// Configuration of the Kubernetes generator.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Kubernetes manifest of the resource to create
    pub manifest: Manifest,
    /// Maximum lifetime of the resource before being replaced
    #[serde(default = "default_max_instance_lifetime_seconds")]
    pub max_instance_lifetime_seconds: NonZeroU32,
    /// Number of resource instances to create
    #[serde(default = "default_concurrent_instances")]
    pub concurrent_instances: NonZeroU32,
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
    /// State machine error
    #[error(transparent)]
    StateMachine(#[from] state_machine::Error),
}

/// Represents a Kubernetes resource
#[derive(Debug)]
pub struct Kubernetes {
    resource: Resource,
    concurrent_instances: NonZeroU32,
    throttle: Option<Throttle>,
    shutdown: lading_signal::Watcher,
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
        let concurrent_instances = config.concurrent_instances;

        // Configure throttle for recycling. The throttle controls how often we recreate instances.
        let throttle = if config.max_instance_lifetime_seconds == NonZeroU32::MAX {
            // MAX means no recycling
            None
        } else {
            // We want to recreate all instances over the lifetime period, so we need
            // concurrent_instances operations over lifetime_seconds. This gives us
            // operations_per_second = concurrent_instances / lifetime_seconds.
            let ops = concurrent_instances
                .get()
                .saturating_div(config.max_instance_lifetime_seconds.get());
            let ops_per_sec = NonZeroU32::new(ops).unwrap_or(NonZeroU32::MIN);

            Some(Throttle::new_with_config(lading_throttle::Config::Stable {
                maximum_capacity: ops_per_sec,
                timeout_micros: 0,
            }))
        };

        Ok(Self {
            resource: match &config.manifest {
                Manifest::Node(s) => Resource::Node(serde_yaml::from_str(s)?),
                Manifest::Namespace(s) => Resource::Namespace(serde_yaml::from_str(s)?),
                Manifest::Pod(s) => Resource::Pod(serde_yaml::from_str(s)?),
                Manifest::Deployment(s) => Resource::Deployment(serde_yaml::from_str(s)?),
                Manifest::Service(s) => Resource::Service(serde_yaml::from_str(s)?),
            },
            concurrent_instances,
            throttle,
            shutdown,
        })
    }

    async fn handle_wait(&mut self) -> state_machine::Event {
        // Handle throttle or just wait for shutdown
        if let Some(ref mut throttle) = self.throttle {
            let shutdown_wait = self.shutdown.clone().recv();
            tokio::select! {
                result = throttle.wait() => {
                    match result {
                        Ok(()) => state_machine::Event::RecreateNext,
                        Err(e) => {
                            warn!("Throttle error: {e}");
                            // Return RecreateNext anyway to continue
                            state_machine::Event::RecreateNext
                        }
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

    /// Run the Kubernetes generator
    ///
    /// # Errors
    ///
    /// Will return an error if Kubernetes connection fails or if resource creation fails.
    #[allow(clippy::too_many_lines)]
    pub async fn spin(mut self) -> Result<(), Error> {
        use state_machine::{Event, Operation, StateMachine};

        let client = kube::Client::try_default().await?;
        let mut machine = StateMachine::new(self.concurrent_instances.get());

        let mut event = Event::Started;

        // See `StateMachine::next` for details on transitions
        loop {
            let operation = machine.next(event)?;
            debug!("State machine: {:?} -> {:?}", machine.state(), operation);

            event = match operation {
                Operation::CreateAllInstances => {
                    debug!(
                        "Creating all {count} instances",
                        count = self.concurrent_instances
                    );
                    let result = create_resources(
                        client.clone(),
                        &self.resource,
                        self.concurrent_instances.get(),
                    )
                    .await;

                    match result {
                        Ok(_) => {
                            debug!("Successfully created all instances");
                            Event::AllInstancesCreated { success: true }
                        }
                        Err(e) => {
                            warn!("Failed to create all instances: {e}");
                            Event::AllInstancesCreated { success: false }
                        }
                    }
                }
                Operation::CreateInstance { index } => {
                    debug!("Creating instance {index}");
                    let result = create_single_resource(
                        client.clone(),
                        &self.resource,
                        self.concurrent_instances.get(),
                        index,
                    )
                    .await;

                    match result {
                        Ok(_) => {
                            debug!("Successfully created instance {index}");
                            Event::InstanceCreated {
                                index,
                                success: true,
                            }
                        }
                        Err(e) => {
                            warn!("Failed to create instance {index}: {e}");
                            Event::InstanceCreated {
                                index,
                                success: false,
                            }
                        }
                    }
                }
                Operation::DeleteInstance { index } => {
                    debug!("Deleting instance {index}");
                    let result = delete_single_resource(
                        client.clone(),
                        &self.resource,
                        self.concurrent_instances.get(),
                        index,
                    )
                    .await;

                    match result {
                        Ok(()) => {
                            debug!("Successfully deleted instance {index}");
                            Event::InstanceDeleted {
                                index,
                                success: true,
                            }
                        }
                        Err(e) => {
                            warn!("Failed to delete instance {index}: {e}");
                            Event::InstanceDeleted {
                                index,
                                success: false,
                            }
                        }
                    }
                }
                Operation::Wait => self.handle_wait().await,
                Operation::DeleteAllInstances => {
                    debug!("Deleting all instances for shutdown");
                    let result = delete_resources(
                        client.clone(),
                        &self.resource,
                        self.concurrent_instances.get(),
                    )
                    .await;

                    match result {
                        Ok(()) => {
                            debug!("Successfully deleted all instances");
                            Event::AllInstancesDeleted { success: true }
                        }
                        Err(e) => {
                            warn!("Failed to delete all instances: {e}");
                            Event::AllInstancesDeleted { success: false }
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
}

async fn create_single_resource(
    client: kube::Client,
    resource: &Resource,
    concurrent_instances: u32,
    index: u32,
) -> Result<Resource, Error> {
    let mut object = resource.clone();
    object.set_name(concurrent_instances, index);

    object
        .create(client, &PostParams::default())
        .await
        .inspect(|o| {
            debug!(
                "Created {kind} {name}",
                kind = object.kind().to_lowercase(),
                name = o.get_name()
            );
        })
        .inspect_err(|e| {
            warn!(
                "Failed to create {kind} {name}: {e}",
                kind = object.kind().to_lowercase(),
                name = object.get_name()
            );
        })
        .map_err(Error::from)
}

async fn create_resources(
    client: kube::Client,
    resource: &Resource,
    concurrent_instances: u32,
) -> Result<Vec<Resource>, Error> {
    let mut resources = Vec::new();

    for instance_index in 0..concurrent_instances {
        let created = create_single_resource(
            client.clone(),
            resource,
            concurrent_instances,
            instance_index,
        )
        .await?;
        resources.push(created);
    }

    Ok(resources)
}

async fn delete_single_resource(
    client: kube::Client,
    resource: &Resource,
    concurrent_instances: u32,
    index: u32,
) -> Result<(), Error> {
    let mut object = resource.clone();
    object.set_name(concurrent_instances, index);

    object
        .delete(client, &DeleteParams::default().grace_period(5))
        .await
        .inspect_err(|e| {
            warn!(
                "Failed to delete {kind} {name}: {e}",
                kind = object.kind().to_lowercase(),
                name = object.get_name()
            );
        })?;

    Ok(())
}

async fn delete_resources(
    client: kube::Client,
    resource: &Resource,
    concurrent_instances: u32,
) -> Result<(), Error> {
    for instance_index in 0..concurrent_instances {
        delete_single_resource(
            client.clone(),
            resource,
            concurrent_instances,
            instance_index,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::generator::kubernetes::Resource;
    use http::{Method, Request, Response, StatusCode};
    use http_body_util::BodyExt;
    use k8s_openapi::api::core::v1::{Node, Pod};
    use kube::api::PostParams;
    use kube::{Api, Client, client::Body};
    use rustc_hash::FxHashMap;
    use std::sync::{Arc, Mutex};
    use tower_test::mock;

    /// Kubernetes API server for testing IO behavior
    ///
    /// We avoid testing the state machine as those tests are handled in
    /// state_machine.rs.
    struct ApiMock {
        handle: mock::Handle<Request<Body>, Response<Body>>,
        resources: Arc<Mutex<FxHashMap<String, Resource>>>,
    }

    impl ApiMock {
        fn new() -> (Client, Self) {
            let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
            let client = Client::new(mock_service, "default");
            let resources = Arc::new(Mutex::new(FxHashMap::default()));
            (client, ApiMock { handle, resources })
        }

        async fn run(mut self) {
            while let Some((request, send)) = self.handle.next_request().await {
                let response = self.handle_request(request).await;
                send.send_response(response);
            }
        }

        async fn handle_request(&self, request: Request<Body>) -> Response<Body> {
            let path = request.uri().path();
            let method = request.method();

            match method {
                &Method::POST => self.handle_create(request).await,
                &Method::DELETE => self.handle_delete(path).await,
                _ => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap(),
            }
        }

        async fn handle_create(&self, request: Request<Body>) -> Response<Body> {
            let body_bytes = request
                .into_body()
                .collect()
                .await
                .map(|collected| collected.to_bytes())
                .unwrap_or_default();

            // Try to deserialize as any supported resource type
            if let Ok(pod) = serde_json::from_slice::<Pod>(&body_bytes) {
                self.create_resource(
                    pod.metadata.name.as_deref().unwrap_or("unknown"),
                    Resource::Pod(pod.clone()),
                )
                .await
            } else if let Ok(node) = serde_json::from_slice::<Node>(&body_bytes) {
                self.create_resource(
                    node.metadata.name.as_deref().unwrap_or("unknown"),
                    Resource::Node(node.clone()),
                )
                .await
            } else {
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::empty())
                    .unwrap()
            }
        }

        async fn create_resource(&self, name: &str, resource: Resource) -> Response<Body> {
            let mut resources = self.resources.lock().unwrap();

            if resources.contains_key(name) {
                return Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from(
                        format!(
                            r#"{{"kind":"Status","message":"{name} already exists"}}"#,
                            name = name
                        )
                        .into_bytes(),
                    ))
                    .unwrap();
            }

            resources.insert(name.to_string(), resource.clone());

            let json_bytes = match &resource {
                Resource::Pod(pod) => serde_json::to_vec(pod).unwrap(),
                Resource::Node(node) => serde_json::to_vec(node).unwrap(),
                Resource::Namespace(ns) => serde_json::to_vec(ns).unwrap(),
                Resource::Deployment(deploy) => serde_json::to_vec(deploy).unwrap(),
                Resource::Service(svc) => serde_json::to_vec(svc).unwrap(),
            };

            Response::builder()
                .status(StatusCode::CREATED)
                .body(Body::from(json_bytes))
                .unwrap()
        }

        async fn handle_delete(&self, path: &str) -> Response<Body> {
            let name = path.split('/').last().unwrap_or("");
            let mut resources = self.resources.lock().unwrap();

            if resources.remove(name).is_some() {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(
                        r#"{"kind":"Status","status":"Success"}"#.as_bytes().to_vec(),
                    ))
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from(
                        r#"{"kind":"Status","message":"not found"}"#.as_bytes().to_vec(),
                    ))
                    .unwrap()
            }
        }
    }

    #[tokio::test]
    async fn create_multiple_instances() {
        use crate::generator::kubernetes::create_resources;

        let (client, mock) = ApiMock::new();
        let resources = mock.resources.clone();

        tokio::spawn(async move { mock.run().await });

        let mut pod = Pod::default();
        pod.metadata.name = Some("test-pod".to_string());
        let resource = Resource::Pod(pod);

        let created = create_resources(client, &resource, 3).await.unwrap();
        assert_eq!(created.len(), 3);

        let state = resources.lock().unwrap();
        assert!(state.contains_key("test-pod-00000"));
        assert!(state.contains_key("test-pod-00001"));
        assert!(state.contains_key("test-pod-00002"));
    }

    #[tokio::test]
    async fn delete_multiple_instances() {
        use crate::generator::kubernetes::{create_resources, delete_resources};

        let (client, mock) = ApiMock::new();
        let resources = mock.resources.clone();

        tokio::spawn(async move { mock.run().await });

        let mut pod = Pod::default();
        pod.metadata.name = Some("delete-test".to_string());
        let resource = Resource::Pod(pod);

        create_resources(client.clone(), &resource, 3)
            .await
            .unwrap();
        assert_eq!(resources.lock().unwrap().len(), 3);

        delete_resources(client, &resource, 3).await.unwrap();
        assert_eq!(resources.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn create_duplicate_fails() {
        let (client, mock) = ApiMock::new();
        tokio::spawn(async move { mock.run().await });

        let api: Api<Pod> = Api::default_namespaced(client);
        let mut pod = Pod::default();
        pod.metadata.name = Some("duplicate-pod".to_string());

        api.create(&PostParams::default(), &pod).await.unwrap();
        let result = api.create(&PostParams::default(), &pod).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_with_error_recovery() {
        use crate::generator::kubernetes::create_resources;

        let (client, mock) = ApiMock::new();
        let resources = mock.resources.clone();

        tokio::spawn(async move { mock.run().await });

        // Pre-create a conflicting resource
        let api: Api<Pod> = Api::default_namespaced(client.clone());
        let mut existing_pod = Pod::default();
        existing_pod.metadata.name = Some("test-pod-00001".to_string());
        api.create(&PostParams::default(), &existing_pod)
            .await
            .unwrap();

        // Try to create multiple instances - should fail on conflict
        let mut pod = Pod::default();
        pod.metadata.name = Some("test-pod".to_string());
        let resource = Resource::Pod(pod);

        let result = create_resources(client, &resource, 3).await;
        assert!(result.is_err());

        // Should have the pre-existing resource
        let state = resources.lock().unwrap();
        assert!(state.contains_key("test-pod-00001"));
    }
}
