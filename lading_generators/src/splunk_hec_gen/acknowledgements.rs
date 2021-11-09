use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::{
    channel::mpsc::{self, Receiver, Sender},
    StreamExt,
};
use http::{header::AUTHORIZATION, Method, Request, Uri};
use hyper::{client::HttpConnector, Body, Client};
use serde::Deserialize;

use super::config::AckConfig;

type AckId = u64;

pub struct Channels {
    ids: HashMap<String, Option<Sender<AckId>>>,
    acks_enabled: bool,
}

impl Channels {
    pub fn new(num_channels: u16) -> Self {
        let ids = (0..num_channels)
            .map(|i| {
                (
                    format!("{}-1111-1111-1111-111111111111", 10000000 as u32 + i as u32),
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
            .map(|(channel_id, ack_id_tx)| (channel_id.to_owned(), ack_id_tx.to_owned()))
            .collect()
    }

    pub fn enable_acknowledgements(
        &mut self,
        ack_uri: Uri,
        token: String,
        ack_config: AckConfig,
    ) -> Result<(), ()> {
        if !self.acks_enabled {
            let client: Client<HttpConnector, Body> = Client::builder()
                .retry_canceled_requests(false)
                .set_host(false)
                .build_http();
            let ack_service = AckService {
                ack_uri,
                token,
                client,
                ack_config,
            };

            let channel_id_to_ack_id_tx = self
                .ids
                .keys()
                .map(|channel_id| {
                    let (tx, rx) = mpsc::channel::<u64>(10000);
                    ack_service.spawn_task(channel_id.clone(), rx);
                    (channel_id.clone(), tx)
                })
                .collect::<Vec<(String, Sender<AckId>)>>();
            for (id, tx) in channel_id_to_ack_id_tx {
                self.ids.insert(id, Some(tx));
            }

            self.acks_enabled = true;
            Ok(())
        } else {
            Err(())
        }
    }
}

struct AckService {
    pub ack_uri: Uri,
    pub token: String,
    pub client: Client<HttpConnector, Body>,
    pub ack_config: AckConfig,
}

impl AckService {
    pub fn spawn_task(&self, channel_id: String, mut ack_rx: Receiver<u64>) {
        let mut ack_ids = HashSet::new();
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.ack_config.ack_query_interval));
        // todo: use retries to expire ack ids
        // let retries = self.ack_config.ack_timeout / self.ack_config.ack_query_interval;
        let client = self.client.clone();
        let token = self.token.clone();
        let ack_uri = self.ack_uri.clone();
        tokio::spawn(async move {
            loop {
                let new_ack_ids = ack_rx
                    .by_ref()
                    .take_until(interval.tick())
                    .collect::<Vec<_>>()
                    .await;
                ack_ids.extend(new_ack_ids.into_iter());

                if ack_ids.len() > 0 {
                    let body = Body::from(serde_json::json!({ "acks": ack_ids }).to_string());
                    let request: Request<Body> = Request::builder()
                        .method(Method::POST)
                        .uri(ack_uri.clone())
                        .header(AUTHORIZATION, format!("Splunk {}", token))
                        .header("x-splunk-request-channel", channel_id.clone())
                        .body(body)
                        .unwrap();
                    match client.request(request).await {
                        Ok(response) => {
                            // todo: check response code
                            let body = hyper::body::to_bytes(response.into_body())
                                .await
                                .unwrap()
                                .to_vec();
                            let ack_status =
                                serde_json::from_slice::<HecAckStatusResponse>(body.as_slice())
                                    .unwrap();

                            let acked_ack_ids = ack_status
                                .acks
                                .into_iter()
                                .filter_map(
                                    |(ack_id, acked)| if acked { Some(ack_id) } else { None },
                                )
                                .collect::<HashSet<_>>();
                            ack_ids = ack_ids
                                .difference(&acked_ack_ids)
                                .map(|x| *x)
                                .collect::<HashSet<_>>();
                        }
                        Err(_err) => {
                            // todo: handle error sending request
                            println!("ack query failed");
                        }
                    }
                }
            }
        });
    }
}

#[derive(Deserialize, Debug)]
struct HecAckStatusResponse {
    acks: HashMap<u64, bool>,
}
