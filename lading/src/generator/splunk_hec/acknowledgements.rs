use core::slice;
use std::time::Duration;

use bytes::Bytes;
use futures::Future;
use http::{Method, Request, StatusCode, Uri, header::AUTHORIZATION};
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use metrics::counter;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::timeout,
};
use tracing::{debug, info, warn};

use super::{AckSettings, SPLUNK_HEC_CHANNEL_HEADER};
type AckId = u64;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper around [`hyper::http::Error`].
    #[error("HTTP error: {0}")]
    Http(#[from] hyper::http::Error),
    /// receiver dropped unexpectedly
    #[error("receiver dropped unexpectedly: {0}")]
    Send(#[from] mpsc::error::SendError<AckId>),
    /// Wrapper around [`hyper::Error`].
    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),
    /// Wrapper around [`serde_json::Error`].
    #[error("Failed to deserialize: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub(crate) enum Channel {
    /// Variant that communicates acks to underlying `AckService`.
    Ack { id: String, tx: Sender<AckId> },
    /// Variant that does no ack'ing.
    NoAck { id: String },
}

impl Channel {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Ack { id, .. } | Self::NoAck { id, .. } => id,
        }
    }

    pub(crate) async fn send<Fut>(&self, msg: Fut) -> Result<(), Error>
    where
        Fut: Future<Output = Option<AckId>>,
    {
        match self {
            Self::NoAck { .. } => Ok(()),
            Self::Ack { tx, id } => {
                if let Some(ack_id) = msg.await {
                    Ok(tx.send(ack_id).await?)
                } else {
                    // A target can return a 2xx body with no ackId even when
                    // acknowledgements are enabled: a misbehaving or non-acking
                    // endpoint, and the body is target-controlled. There is no
                    // id to track, so skip it rather than abort the whole run
                    // under panic="abort" (ADR-004). The block is simply not
                    // ack-verified. The label is the bounded channel id
                    // (ADR-005).
                    counter!("ack_id_missing", "channel_id" => id.clone()).increment(1);
                    warn!(channel_id = %id, "HEC response carried no ackId; skipping ack tracking");
                    Ok(())
                }
            }
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
        let client = Client::builder(TokioExecutor::new())
            .retry_canceled_requests(false)
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
    pub(crate) client: Client<HttpConnector, BoxBody<Bytes, hyper::Error>>,
    pub(crate) ack_settings: AckSettings,
}

impl AckService {
    /// Spawn a tokio task that will continuously query
    /// to check on a particular Splunk channel's ack id statuses. The task
    /// receives new ack ids from [`super::worker::Worker`]
    pub(crate) async fn spin(
        self,
        channel_id: String,
        mut ack_rx: Receiver<AckId>,
    ) -> Result<(), Error> {
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
                            return Ok(());
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
                        let body = crate::full(
                            serde_json::json!({ "acks": ack_ids.keys().collect::<Vec<&u64>>() })
                                .to_string(),
                        );
                        let request = Request::builder()
                            .method(Method::POST)
                            .uri(self.ack_uri.clone())
                            .header(AUTHORIZATION, format!("Splunk {}", self.token))
                            .header(SPLUNK_HEC_CHANNEL_HEADER, channel_id.clone())
                            .body(body)?;
                        let work = ack_request(self.client.clone(), request, channel_id.clone(), &mut ack_ids);

                        if let Err(_err) = timeout(Duration::from_secs(1), work).await {
                            counter!("ack_request_timeout").increment(1);
                        }

                    }
                }
            }
        }
    }
}

async fn ack_request(
    client: Client<HttpConnector, BoxBody<Bytes, hyper::Error>>,
    request: Request<BoxBody<Bytes, hyper::Error>>,
    channel_id: String,
    ack_ids: &mut FxHashMap<AckId, u64>,
) -> Result<(), Error> {
    match client.request(request).await {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            let status = parts.status;
            counter!("ack_status_request_ok", "channel_id" => channel_id.clone(), "status" => status.to_string()).increment(1);
            if status == StatusCode::OK {
                let body = body.boxed().collect().await?.to_bytes();
                let ack_status = serde_json::from_slice::<HecAckStatusResponse>(&body)?;

                let mut ack_ids_acked: u32 = 0;
                // Remove successfully acked ack ids
                for acked_ack_id in ack_status
                    .acks
                    .into_iter()
                    .filter_map(|(ack_id, acked)| if acked { Some(ack_id) } else { None })
                {
                    ack_ids.remove(&acked_ack_id);
                    ack_ids_acked += 1;
                }
                counter!("ack_ids_acked", "channel_id" => channel_id.clone())
                    .increment(u64::from(ack_ids_acked));

                // For all remaining ack ids, decrement the retries count,
                // removing ack ids with no retries left
                let mut timed_out_ack_ids = Vec::new();
                for (ack_id, retries) in ack_ids.iter_mut() {
                    match retries.checked_sub(1) {
                        None => timed_out_ack_ids.push(*ack_id),
                        Some(r) => *retries = r,
                    }
                }
                counter!("ack_ids_dropped", "channel_id" => channel_id.clone())
                    .increment(timed_out_ack_ids.len() as u64);
                for timed_out_ack_id in timed_out_ack_ids {
                    ack_ids.remove(&timed_out_ack_id);
                }
            }
        }
        Err(err) => {
            counter!("ack_status_request_failure", "channel_id" => channel_id.clone(), "error" => err.to_string()).increment(1);
        }
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
struct HecAckStatusResponse {
    acks: FxHashMap<AckId, bool>,
}

#[cfg(test)]
mod tests {
    use super::Channel;
    use std::future::ready;
    use tokio::sync::mpsc;

    /// An `Ack` channel handed a `None` ackId -- the target returned a 2xx body
    /// with no ackId while acknowledgements were enabled -- must skip the ack
    /// and return Ok, never panic. The previous `.expect()` aborted the whole
    /// run under panic="abort" (ADR-004) on a target-controlled response.
    #[tokio::test]
    async fn ack_channel_send_none_id_is_skipped_not_panicking() {
        let (tx, _rx) = mpsc::channel(4);
        let channel = Channel::Ack {
            id: "test-channel".to_string(),
            tx,
        };
        channel
            .send(ready(None::<u64>))
            .await
            .expect("a None ackId must be skipped, not panic");
    }

    /// A present ackId is still forwarded to the receiver.
    #[tokio::test]
    async fn ack_channel_send_some_id_forwards() {
        let (tx, mut rx) = mpsc::channel(4);
        let channel = Channel::Ack {
            id: "test-channel".to_string(),
            tx,
        };
        channel
            .send(ready(Some(42u64)))
            .await
            .expect("a present ackId must forward");
        assert_eq!(rx.recv().await, Some(42u64));
    }
}
