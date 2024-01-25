use core::slice;
use std::time::Duration;

use futures::Future;
use http::{header::AUTHORIZATION, Method, Request, StatusCode, Uri};
use hyper::{client::HttpConnector, Body, Client};
use metrics::counter;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::timeout,
};
use tracing::{debug, info};

use super::{AckSettings, SPLUNK_HEC_CHANNEL_HEADER};

type AckId = u64;

#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {}

#[derive(Debug, Clone)]
pub(crate) enum Channel {
    /// Variant that communicates acks to underlying AckService.
    Ack { id: String, tx: Sender<AckId> },
    /// Variant that does no ack'ing.
    NoAck { id: String },
}

impl Channel {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Ack { ref id, .. } | Self::NoAck { ref id, .. } => id,
        }
    }

    pub(crate) async fn send<Fut>(&self, msg: Fut)
    where
        Fut: Future<Output = AckId>,
    {
        match self {
            Self::NoAck { .. } => (),
            Self::Ack { tx, .. } => tx
                .send(msg.await)
                .await
                .expect("receiver dropped unexpectedly"),
        }
    }
}

/// Splunk HEC channels
#[derive(Debug)]
pub(crate) struct Channels {
    channels: Vec<Channel>,
}

impl Channels {
    pub(crate) fn new(num_channels: u16) -> Self {
        let channels = (0..num_channels)
            .map(|i| Channel::NoAck {
                id: format!(
                    "{}-1111-1111-1111-111111111111",
                    10_000_000_u32 + u32::from(i)
                ),
            })
            .collect::<Vec<Channel>>();
        Self { channels }
    }

    pub(crate) fn iter(&self) -> Iter<'_, Channel> {
        Iter(self.channels.iter())
    }

    pub(crate) fn enable_acknowledgements(
        &mut self,
        ack_uri: Uri,
        token: String,
        ack_settings: AckSettings,
    ) {
        let client: Client<HttpConnector, Body> = Client::builder()
            .retry_canceled_requests(false)
            .set_host(false)
            .build_http();

        let ack_service = AckService {
            ack_uri,
            token,
            client,
            ack_settings,
        };

        for channel in &mut self.channels {
            match channel {
                Channel::Ack { .. } => unreachable!(),
                Channel::NoAck { id } => {
                    let (tx, rx) = mpsc::channel::<AckId>(1_000_000);
                    tokio::spawn(ack_service.clone().spin(id.clone(), rx));
                    *channel = Channel::Ack { id: id.clone(), tx };
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct Iter<'a, V>(slice::Iter<'a, V>);

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[derive(Debug, Clone)]
/// Responsible for querying /services/collector/ack
/// periodically. [`AckService`] is bounded to a single Channel -- via the
/// `channel_id` parameter on [`AckService::spin`] -- and is created by
/// Channel. That is, this service is purely an implementation detail.
struct AckService {
    pub(crate) ack_uri: Uri,
    pub(crate) token: String,
    pub(crate) client: Client<HttpConnector, Body>,
    pub(crate) ack_settings: AckSettings,
}

impl AckService {
    /// Spawn a tokio task that will continuously query
    /// to check on a particular Splunk channel's ack id statuses. The task
    /// receives new ack ids from [`super::worker::Worker`]
    pub(crate) async fn spin<'a>(self, channel_id: String, mut ack_rx: Receiver<AckId>) {
        let mut ack_ids: FxHashMap<AckId, u64> = FxHashMap::default();
        let mut interval = tokio::time::interval(Duration::from_secs(
            self.ack_settings.ack_query_interval_seconds,
        ));
        let retries =
            self.ack_settings.ack_timeout_seconds / self.ack_settings.ack_query_interval_seconds;

        loop {
            tokio::select! {
                resp = ack_rx.recv() => {
                    match resp {
                        None => {
                            info!("AckService receiver closed, shutting down");
                            return;
                        }
                        Some(ack_id) => {
                            ack_ids.insert(ack_id, retries);
                        }
                    }
                }
                _ = interval.tick(), if !ack_ids.is_empty() => {
                    if ack_ids.is_empty() {
                        debug!("tick expired with no acks");
                    } else {
                        let body = Body::from(
                            serde_json::json!({ "acks": ack_ids.keys().collect::<Vec<&u64>>() })
                                .to_string(),
                        );
                        let request: Request<Body> = Request::builder()
                            .method(Method::POST)
                            .uri(self.ack_uri.clone())
                            .header(AUTHORIZATION, format!("Splunk {}", self.token))
                            .header(SPLUNK_HEC_CHANNEL_HEADER, channel_id.clone())
                            .body(body)
                            .expect("Error: failed to build ack request");
                        let work = ack_request(self.client.clone(), request, channel_id.clone(), &mut ack_ids);

                        if let Err(_err) = timeout(Duration::from_secs(1), work).await {
                            counter!("ack_request_timeout", 1);
                        }

                    }
                }
            }
        }
    }
}

async fn ack_request(
    client: Client<HttpConnector>,
    request: Request<Body>,
    channel_id: String,
    ack_ids: &mut FxHashMap<AckId, u64>,
) {
    match client.request(request).await {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            let status = parts.status;
            counter!("ack_status_request_ok", 1, "channel_id" => channel_id.clone(), "status" => status.to_string());
            if status == StatusCode::OK {
                let body = hyper::body::to_bytes(body)
                    .await
                    .expect("Error: failed to convert response body to bytes");
                let ack_status = serde_json::from_slice::<HecAckStatusResponse>(&body)
                    .expect("Error: failed to deserialize ack status response");

                let mut ack_ids_acked: u32 = 0;
                // Remove successfully acked ack ids
                for acked_ack_id in
                    ack_status
                        .acks
                        .into_iter()
                        .filter_map(|(ack_id, acked)| if acked { Some(ack_id) } else { None })
                {
                    ack_ids.remove(&acked_ack_id);
                    ack_ids_acked += 1;
                }
                counter!("ack_ids_acked", u64::from(ack_ids_acked), "channel_id" => channel_id.clone());

                // For all remaining ack ids, decrement the retries count,
                // removing ack ids with no retries left
                let mut timed_out_ack_ids = Vec::new();
                for (ack_id, retries) in ack_ids.iter_mut() {
                    match retries.checked_sub(1) {
                        None => timed_out_ack_ids.push(*ack_id),
                        Some(r) => *retries = r,
                    }
                }
                counter!("ack_ids_dropped", timed_out_ack_ids.len() as u64, "channel_id" => channel_id.clone());
                for timed_out_ack_id in timed_out_ack_ids {
                    ack_ids.remove(&timed_out_ack_id);
                }
            }
        }
        Err(err) => {
            counter!("ack_status_request_failure", 1, "channel_id" => channel_id.clone(), "error" => err.to_string());
        }
    }
}

#[derive(Deserialize, Debug)]
struct HecAckStatusResponse {
    acks: FxHashMap<AckId, bool>,
}
