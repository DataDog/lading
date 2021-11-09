use std::{collections::HashMap, time::Duration};

use futures::{
    channel::mpsc::{self, Receiver, Sender},
    StreamExt,
};
use http::{header::AUTHORIZATION, Method, Request, StatusCode, Uri};
use hyper::{client::HttpConnector, Body, Client};
use metrics::counter;
use serde::Deserialize;

use crate::splunk_hec_gen::SPLUNK_HEC_CHANNEL_HEADER;

use super::{config::AckSettings, worker::Error};

type AckId = u64;

/// Splunk HEC channels
pub struct Channels {
    /// If acknowledgements are enabled, the channel id maps to Some(Sender) used
    /// to send ack ids to a separate task that will query for ack status
    ids: HashMap<String, Option<Sender<AckId>>>,
    acks_enabled: bool,
}

impl Channels {
    pub fn new(num_channels: u16) -> Self {
        let ids = (0..num_channels)
            .map(|i| {
                (
                    format!(
                        "{}-1111-1111-1111-111111111111",
                        10_000_000_u32 + u32::from(i)
                    ),
                    None,
                )
            })
            .collect::<HashMap<_, _>>();
        Self {
            ids,
            acks_enabled: false,
        }
    }

    pub fn get_channel_info(&self) -> Vec<(String, Option<Sender<AckId>>)> {
        self.ids
            .iter()
            .map(|(channel_id, ack_id_tx)| (channel_id.clone(), ack_id_tx.clone()))
            .collect()
    }

    pub fn enable_acknowledgements(
        &mut self,
        ack_uri: Uri,
        token: String,
        ack_settings: AckSettings,
    ) -> Result<(), Error> {
        if self.acks_enabled {
            Err(Error::AcksAlreadyEnabled)
        } else {
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

            let channel_id_to_ack_id_tx = self
                .ids
                .keys()
                .map(|channel_id| {
                    let (tx, rx) = mpsc::channel::<AckId>(1_000_000);
                    ack_service.spawn_ack_task(channel_id.clone(), rx);
                    (channel_id.clone(), tx)
                })
                .collect::<Vec<(String, Sender<AckId>)>>();
            for (id, tx) in channel_id_to_ack_id_tx {
                self.ids.insert(id, Some(tx));
            }

            self.acks_enabled = true;
            Ok(())
        }
    }
}

struct AckService {
    pub ack_uri: Uri,
    pub token: String,
    pub client: Client<HttpConnector, Body>,
    pub ack_settings: AckSettings,
}

impl AckService {
    /// Spawn a tokio task that will continuously query /services/collector/ack
    /// to check on a particular Splunk channel's ack id statuses. The task
    /// receives new ack ids from [`super::worker::Worker`]
    pub fn spawn_ack_task(&self, channel_id: String, mut ack_rx: Receiver<AckId>) {
        let mut ack_ids = HashMap::new();
        let mut interval = tokio::time::interval(Duration::from_secs(
            self.ack_settings.ack_query_interval_seconds,
        ));
        let retries =
            self.ack_settings.ack_timeout_seconds / self.ack_settings.ack_query_interval_seconds;
        let client = self.client.clone();
        let token = self.token.clone();
        let ack_uri = self.ack_uri.clone();
        tokio::spawn(async move {
            loop {
                let new_ack_ids = ack_rx
                    .by_ref()
                    .take_until(interval.tick())
                    .map(|ack_id| (ack_id, retries))
                    .collect::<Vec<_>>()
                    .await;
                ack_ids.extend(new_ack_ids);

                if !ack_ids.is_empty() {
                    let body = Body::from(
                        serde_json::json!({ "acks": ack_ids.keys().collect::<Vec<&u64>>() })
                            .to_string(),
                    );
                    let request: Request<Body> = Request::builder()
                        .method(Method::POST)
                        .uri(ack_uri.clone())
                        .header(AUTHORIZATION, format!("Splunk {}", token))
                        .header(SPLUNK_HEC_CHANNEL_HEADER, channel_id.clone())
                        .body(body)
                        .unwrap();

                    match client.request(request).await {
                        Ok(response) => {
                            let (parts, body) = response.into_parts();
                            let status = parts.status;
                            counter!("ack_status_request_ok", 1, "channel_id" => channel_id.clone(), "status" => status.to_string());
                            if status == StatusCode::OK {
                                let body = hyper::body::to_bytes(body).await.unwrap();
                                let ack_status =
                                    serde_json::from_slice::<HecAckStatusResponse>(&body).unwrap();

                                let acked_ack_ids = ack_status
                                    .acks
                                    .into_iter()
                                    .filter_map(
                                        |(ack_id, acked)| if acked { Some(ack_id) } else { None },
                                    )
                                    .collect::<Vec<_>>();

                                // Remove successfully acked ack ids
                                for acked_ack_id in acked_ack_ids {
                                    ack_ids.remove(&acked_ack_id);
                                }
                                // For all remaining ack ids, decrement the retries count,
                                // removing ack ids with no retries left
                                let mut timed_out_ack_ids = Vec::new();
                                for (ack_id, retries) in &mut ack_ids {
                                    *retries -= 1;
                                    if retries <= &mut 0 {
                                        timed_out_ack_ids.push(*ack_id);
                                    }
                                }
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
            }
        });
    }
}

#[derive(Deserialize, Debug)]
struct HecAckStatusResponse {
    acks: HashMap<AckId, bool>,
}
