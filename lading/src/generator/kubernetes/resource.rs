//! Kubernetes resource abstractions

use either::Either;
use k8s_openapi::{
    ClusterResourceScope, NamespaceResourceScope, Resource as KubeResource,
    api::{
        apps::v1::Deployment,
        core::v1::{Namespace, Node, Pod, Service},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::api::{DeleteParams, PostParams};
use kube_core::Status;
use tracing::debug;

/// A Kubernetes resource that can be created, deleted, and managed
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(super) enum Resource {
    /// Kubernetes Node resource (cluster-scoped)
    Node(Node),
    /// Kubernetes Namespace resource (cluster-scoped)
    Namespace(Namespace),
    /// Kubernetes Pod resource
    Pod(Pod),
    /// Kubernetes Deployment resource
    Deployment(Deployment),
    /// Kubernetes Service resource
    Service(Service),
}

impl Resource {
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Resource::Node(_) => <Node as KubeResource>::KIND,
            Resource::Namespace(_) => <Namespace as KubeResource>::KIND,
            Resource::Pod(_) => <Pod as KubeResource>::KIND,
            Resource::Deployment(_) => <Deployment as KubeResource>::KIND,
            Resource::Service(_) => <Service as KubeResource>::KIND,
        }
    }

    pub(super) fn meta(&self) -> &ObjectMeta {
        match self {
            Resource::Node(node) => &node.metadata,
            Resource::Namespace(ns) => &ns.metadata,
            Resource::Pod(pod) => &pod.metadata,
            Resource::Deployment(deploy) => &deploy.metadata,
            Resource::Service(svc) => &svc.metadata,
        }
    }

    pub(super) fn meta_mut(&mut self) -> &mut ObjectMeta {
        match self {
            Resource::Node(node) => &mut node.metadata,
            Resource::Namespace(ns) => &mut ns.metadata,
            Resource::Pod(pod) => &mut pod.metadata,
            Resource::Deployment(deploy) => &mut deploy.metadata,
            Resource::Service(svc) => &mut svc.metadata,
        }
    }

    pub(super) fn set_name(&mut self, concurrent_instances: u32, instance_index: u32) {
        if concurrent_instances > 1 {
            // Currently we only update metadata.name, but other fields may
            // reference this name and need updating too. We defer this until
            // there's demonstrated need.
            let kind = self.kind().to_lowercase();
            self.meta_mut().name = Some(self.meta().name.as_ref().map_or_else(
                || format!("lading-{kind}-{instance_index:0>5}"),
                |name| format!("{name}-{instance_index:0>5}"),
            ));
        } else if self.meta().name.is_none() {
            let kind = self.kind().to_lowercase();
            self.meta_mut().name = Some(format!("lading-{kind}"));
        }
    }

    pub(super) fn get_name(&self) -> &str {
        self.meta()
            .name
            .as_ref()
            .expect("Do not forget to call `set_name`")
    }

    /// Get an API handle for cluster-scoped resources
    fn cluster_api<T>(client: kube::Client) -> kube::Api<T>
    where
        T: kube::Resource<Scope = ClusterResourceScope>,
        <T as kube::Resource>::DynamicType: std::default::Default,
    {
        kube::Api::all(client)
    }

    /// Get an API handle for namespace-scoped resources
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

    pub(super) async fn create(
        &self,
        client: kube::Client,
        pp: &PostParams,
    ) -> Result<Self, kube::Error> {
        match self {
            Resource::Node(node) => Self::cluster_api::<Node>(client)
                .create(pp, node)
                .await
                .map(Resource::Node),
            Resource::Namespace(ns) => Self::cluster_api::<Namespace>(client)
                .create(pp, ns)
                .await
                .map(Resource::Namespace),
            Resource::Pod(pod) => self
                .namespaced_api::<Pod>(client)
                .create(pp, pod)
                .await
                .map(Resource::Pod),
            Resource::Deployment(deploy) => self
                .namespaced_api::<Deployment>(client)
                .create(pp, deploy)
                .await
                .map(Resource::Deployment),
            Resource::Service(svc) => self
                .namespaced_api::<Service>(client)
                .create(pp, svc)
                .await
                .map(Resource::Service),
        }
    }

    pub(super) async fn delete(
        &self,
        client: kube::Client,
        dp: &DeleteParams,
    ) -> Result<(), kube::Error> {
        let name = self.get_name();
        let kind = self.kind().to_lowercase();

        let handle_status = |status: Status| -> Result<(), kube::Error> {
            if status.is_failure() {
                let message = if status.message.is_empty() {
                    "unknown error"
                } else {
                    &status.message
                };
                let reason = if status.reason.is_empty() {
                    "unknown reason"
                } else {
                    &status.reason
                };
                return Err(kube::Error::Api(kube::core::ErrorResponse {
                    status: "Failure".to_string(),
                    message: format!("Failed to delete {kind} {name}: {message} ({reason})"),
                    reason: reason.to_string(),
                    code: status.code,
                }));
            }

            debug!("Confirmed deletion of {kind} {name}");
            Ok(())
        };

        match self {
            Resource::Node(_) => {
                let result = Self::cluster_api::<Node>(client).delete(name, dp).await?;
                match result {
                    Either::Left(_) => {
                        debug!("Initiated deletion of {kind} {name}");
                        Ok(())
                    }
                    Either::Right(status) => handle_status(status),
                }
            }
            Resource::Namespace(_) => {
                let result = Self::cluster_api::<Namespace>(client)
                    .delete(name, dp)
                    .await?;
                match result {
                    Either::Left(_) => {
                        debug!("Initiated deletion of {kind} {name}");
                        Ok(())
                    }
                    Either::Right(status) => handle_status(status),
                }
            }
            Resource::Pod(_) => {
                let result = self.namespaced_api::<Pod>(client).delete(name, dp).await?;
                match result {
                    Either::Left(_) => {
                        debug!("Initiated deletion of {kind} {name}");
                        Ok(())
                    }
                    Either::Right(status) => handle_status(status),
                }
            }
            Resource::Deployment(_) => {
                let result = self
                    .namespaced_api::<Deployment>(client)
                    .delete(name, dp)
                    .await?;
                match result {
                    Either::Left(_) => {
                        debug!("Initiated deletion of {kind} {name}");
                        Ok(())
                    }
                    Either::Right(status) => handle_status(status),
                }
            }
            Resource::Service(_) => {
                let result = self
                    .namespaced_api::<Service>(client)
                    .delete(name, dp)
                    .await?;
                match result {
                    Either::Left(_) => {
                        debug!("Initiated deletion of {kind} {name}");
                        Ok(())
                    }
                    Either::Right(status) => handle_status(status),
                }
            }
        }
    }
}
