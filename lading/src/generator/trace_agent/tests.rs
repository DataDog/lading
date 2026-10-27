use std::{convert::Infallible, error, sync::Mutex, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Response,
    header::{CONNECTION, CONTENT_TYPE, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use lading_payload::{
    block::CacheMethod,
    common::config::ConfRange,
    trace_agent::{self, agent_payload, v04},
};
use prost::Message;
use tokio::{net::TcpListener, time::timeout};

use super::{BackoffBehavior, Config, General, TraceAgent};

type TestError = Box<dyn error::Error + Send + Sync>;

// Decode only the structural fields here, independently of the payload serializer's types.
#[derive(Clone, PartialEq, Message)]
struct IntakePayload {
    #[prost(message, repeated, tag = "5")]
    tracer_payloads: Vec<TracerPayload>,
}

#[derive(Clone, PartialEq, Message)]
struct TracerPayload {
    #[prost(message, repeated, tag = "6")]
    chunks: Vec<TraceChunk>,
}

#[derive(Clone, PartialEq, Message)]
struct TraceChunk {
    #[prost(message, repeated, tag = "3")]
    spans: Vec<Span>,
}

#[derive(Clone, PartialEq, Message)]
struct Span {
    #[prost(uint64, tag = "4")]
    trace_id: u64,
    #[prost(uint64, tag = "5")]
    span_id: u64,
}

async fn serve_intake(
    listener: TcpListener,
    broadcaster: lading_signal::Broadcaster,
) -> Result<(), TestError> {
    let broadcaster = Mutex::new(Some(broadcaster));
    loop {
        let (socket, _) = listener.accept().await?;
        let broadcaster = &broadcaster;
        http1::Builder::new()
            .serve_connection(
                TokioIo::new(socket),
                service_fn(
                    |request: hyper::Request<hyper::body::Incoming>| async move {
                        assert_eq!(request.method(), hyper::Method::POST);
                        assert_eq!(request.uri().path(), "/api/v0.2/traces");
                        assert_eq!(request.headers()[CONTENT_TYPE], "application/x-protobuf");
                        let body = request
                            .into_body()
                            .collect()
                            .await
                            .expect("read request")
                            .to_bytes();
                        let payload = IntakePayload::decode(body).expect("valid protobuf");
                        // One host owning two tracers: each request carries one or both.
                        assert!((1..=2).contains(&payload.tracer_payloads.len()));
                        for tracer in &payload.tracer_payloads {
                            assert!(!tracer.chunks.is_empty());
                        }
                        for chunk in payload
                            .tracer_payloads
                            .iter()
                            .flat_map(|tracer| &tracer.chunks)
                        {
                            assert_eq!(chunk.spans.len(), 2);
                            assert_eq!(chunk.spans[0].trace_id, chunk.spans[1].trace_id);
                            assert_ne!(chunk.spans[0].span_id, chunk.spans[1].span_id);
                        }
                        if let Some(signal) = broadcaster.lock().expect("lock signal").take() {
                            signal.signal();
                        }
                        let mut response = Response::new(Full::new(Bytes::new()));
                        response
                            .headers_mut()
                            .insert(CONNECTION, HeaderValue::from_static("close"));
                        Ok::<_, Infallible>(response)
                    },
                ),
            )
            .await?;
    }
}

/// The configured intake variant sends decodable trace chunks to the protobuf route/media type.
#[tokio::test]
async fn agent_payload_requests_use_the_intake_protocol() -> Result<(), TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = Config {
        seed: [0; 32],
        target_uri: format!("http://{address}").parse()?,
        backoff_behavior: BackoffBehavior::Ignore,
        variant: trace_agent::Config::AgentPayload(agent_payload::Config {
            hosts: ConfRange::Constant(1),
            tracers: ConfRange::Constant(2),
            traces: v04::Config {
                contexts: ConfRange::Constant(1),
                spans_per_trace: ConfRange::Constant(2),
                tags_per_span: ConfRange::Constant(1),
                metrics_per_span: ConfRange::Constant(0),
                ..v04::Config::default()
            },
        }),
        bytes_per_second: Some(byte_unit::Byte::from_u64(4096)),
        maximum_block_size: byte_unit::Byte::from_u64(4096),
        maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64(8192),
        block_cache_method: CacheMethod::Fixed,
        parallel_connections: 1,
        throttle: None,
    };
    let (watcher, broadcaster) = lading_signal::signal();
    let generator = TraceAgent::new(General { id: None }, &config, watcher)?;
    let serve = serve_intake(listener, broadcaster);
    let generate = async {
        generator.spin().await?;
        Ok::<_, TestError>(())
    };
    timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = serve => result,
            result = generate => result,
        }
    })
    .await??;
    Ok(())
}
