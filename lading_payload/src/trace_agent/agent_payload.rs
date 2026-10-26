//! Protobuf `AgentPayload` generation for the agent-to-intake `/api/v0.2/traces` endpoint.
//!
//! This is distinct from the tracer-to-agent `/v0.2/traces` `MessagePack` protocol.
//! Reuse the v0.4 trace generator so both wire formats share the same trace-shape controls.

use std::io::Write;

use prost::Message;
use rand::Rng;

use super::v04;
use crate::{Error, Generator};

#[cfg(test)]
mod tests;
mod wire;

/// Generate protobuf payloads containing complete trace chunks.
#[derive(Debug, Clone)]
pub struct AgentPayload {
    traces: v04::V04,
    last_span_count: u64,
}

impl AgentPayload {
    /// Create a generator using the existing v0.4 trace-shape configuration.
    ///
    /// # Errors
    ///
    /// Return an error if configuration validation or string-pool construction fails.
    pub fn with_config<R: Rng>(config: v04::Config, rng: &mut R) -> Result<Self, Error> {
        config.valid().map_err(|error| {
            Error::Validation(format!("invalid agent payload configuration: {error:?}"))
        })?;
        Ok(Self {
            traces: v04::V04::with_config(config, rng)?,
            last_span_count: 0,
        })
    }
}

// Both containing message fields (AgentPayload.tracerPayloads = 5 and
// TracerPayload.chunks = 6) have a one-byte key. Include each length prefix,
// including changes in its width, without rescanning previously generated chunks.
fn message_size(bytes: usize) -> usize {
    1 + prost::length_delimiter_len(bytes) + bytes
}

impl crate::Serialize for AgentPayload {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        self.last_span_count = 0;
        if max_bytes == 0 {
            return Ok(());
        }

        let mut payload = wire::AgentPayload {
            host_name: "lading".to_owned(),
            env: "benchmark".to_owned(),
            agent_version: "lading".to_owned(),
            tracer_payloads: Vec::new(),
        };
        let mut tracer = wire::TracerPayload {
            language_name: "rust".to_owned(),
            tracer_version: "lading".to_owned(),
            chunks: Vec::new(),
        };
        let payload_bytes = payload.encoded_len();
        let mut tracer_bytes = tracer.encoded_len();
        loop {
            let trace = self.traces.generate(&mut rng)?;
            let chunk = wire::TraceChunk::from(trace);
            let next_tracer_bytes = tracer_bytes + message_size(chunk.encoded_len());
            if payload_bytes + message_size(next_tracer_bytes) > max_bytes {
                break;
            }
            self.last_span_count += chunk.spans.len() as u64;
            tracer.chunks.push(chunk);
            tracer_bytes = next_tracer_bytes;
        }

        // A budget smaller than one complete trace produces no block, not an
        // empty protobuf message that could be mistaken for useful traffic.
        if tracer.chunks.is_empty() {
            return Ok(());
        }
        payload.tracer_payloads.push(tracer);
        let mut buffer = Vec::with_capacity(payload.encoded_len());
        payload.encode(&mut buffer)?;
        writer.write_all(&buffer)?;
        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_span_count)
    }
}

impl From<v04::Trace<'_>> for wire::TraceChunk {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "priority metrics originate from i32 weights"
    )]
    fn from(trace: v04::Trace<'_>) -> Self {
        let priority = trace
            .spans
            .first()
            .and_then(|span| span.metrics.get(v04::SAMPLING_PRIORITY_KEY))
            .map_or(1, |priority| *priority as i32);
        Self {
            priority,
            spans: trace.spans.into_iter().map(wire::Span::from).collect(),
        }
    }
}

impl From<v04::Span<'_>> for wire::Span {
    fn from(span: v04::Span<'_>) -> Self {
        Self {
            service: span.service.to_owned(),
            name: span.name.to_owned(),
            resource: span.resource.to_owned(),
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_id: span.parent_id,
            start: span.start,
            duration: span.duration,
            error: span.error,
            meta: span
                .meta
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            metrics: span
                .metrics
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
            r#type: span.kind.to_owned(),
            meta_struct: span
                .meta_struct
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        }
    }
}
