use std::{collections::BTreeMap, num::NonZeroU32};

use proptest::prelude::*;
use prost::Message;
use rand::{SeedableRng, rngs::StdRng};

use super::{AgentPayload, wire};
use crate::{
    Serialize,
    block::Cache,
    common::config::ConfRange,
    trace_agent::{self, v1, v04},
};

fn config(spans: u8) -> v04::Config {
    v04::Config {
        contexts: ConfRange::Constant(2),
        spans_per_trace: ConfRange::Constant(spans),
        tags_per_span: ConfRange::Constant(2),
        metrics_per_span: ConfRange::Constant(1),
        service_name_length: ConfRange::Constant(8),
        operation_name_length: ConfRange::Constant(12),
        resource_name_length: ConfRange::Constant(16),
        span_type_length: ConfRange::Constant(3),
        tag_key_length: ConfRange::Constant(5),
        tag_value_length: ConfRange::Constant(8),
        metric_key_length: ConfRange::Constant(5),
        ..v04::Config::default()
    }
}

proptest! {
    /// For all seeds and budgets: identical configuration and RNG state produce identical bytes.
    #[test]
    fn payloads_are_deterministic(seed: u64, spans in 1u8..8, budget in 0usize..32768) {
        let mut first_rng = StdRng::seed_from_u64(seed);
        let mut second_rng = StdRng::seed_from_u64(seed);
        let mut first = AgentPayload::with_config(config(spans), &mut first_rng)?;
        let mut second = AgentPayload::with_config(config(spans), &mut second_rng)?;
        let mut first_bytes = Vec::new();
        let mut second_bytes = Vec::new();
        first.to_bytes(&mut first_rng, budget, &mut first_bytes)?;
        second.to_bytes(&mut second_rng, budget, &mut second_bytes)?;
        prop_assert_eq!(first_bytes, second_bytes);
        prop_assert_eq!(first.data_points_generated(), second.data_points_generated());
    }

    /// For all seeds and budgets: `encoded_len <= budget`, every emitted chunk is complete,
    /// parent IDs refer to spans in that chunk, and reported data points equal emitted spans.
    #[test]
    fn payloads_preserve_trace_shape_within_the_budget(
        seed: u64,
        spans in 1u8..8,
        budget in 0usize..65536,
        errors: bool,
        priority in -1i32..3,
    ) {
        let mut configuration = config(spans);
        configuration.error_rate = if errors { 1.0 } else { 0.0 };
        configuration.priority_weights = Some(BTreeMap::from([(priority, 1)]));
        let mut rng = StdRng::seed_from_u64(seed);
        let mut generator = AgentPayload::with_config(configuration, &mut rng)?;
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, budget, &mut bytes)?;
        prop_assert!(bytes.len() <= budget);
        if bytes.is_empty() {
            prop_assert_eq!(generator.data_points_generated(), Some(0));
        } else {
            let payload = wire::AgentPayload::decode(bytes.as_slice())?;
            prop_assert_eq!(payload.tracer_payloads.len(), 1);
            let chunks = &payload.tracer_payloads[0].chunks;
            prop_assert!(!chunks.is_empty());
            let mut count = 0;
            for chunk in chunks {
                prop_assert_eq!(chunk.priority, priority);
                prop_assert_eq!(chunk.spans.len(), usize::from(spans));
                prop_assert_eq!(chunk.spans.iter().filter(|span| span.parent_id == 0).count(), 1);
                let trace_id = chunk.spans[0].trace_id;
                for span in &chunk.spans {
                    prop_assert_eq!(span.trace_id, trace_id);
                    prop_assert_eq!(span.error, i32::from(errors));
                    prop_assert_eq!(span.service.len(), 8);
                    prop_assert_eq!(span.name.len(), 12);
                    prop_assert_eq!(span.resource.len(), 16);
                    prop_assert_eq!(span.r#type.len(), 3);
                    prop_assert!(!span.meta.is_empty());
                    prop_assert!(!span.metrics.is_empty());
                    prop_assert!(span.start >= 946_684_800_000_000_000);
                    prop_assert!(span.duration >= 0);
                    prop_assert!(span.start.checked_add(span.duration).is_some());
                    prop_assert!(span.parent_id == 0 || chunk.spans.iter().any(|parent| parent.span_id == span.parent_id));
                    count += 1;
                }
            }
            prop_assert_eq!(generator.data_points_generated(), Some(count));
        }
    }

    /// For all seeds: a rejected block clears the span count from the previous successful block.
    #[test]
    fn rejected_blocks_reset_the_span_count(seed: u64, budget in 0usize..32) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut generator = AgentPayload::with_config(config(1), &mut rng)?;
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, 4096, &mut bytes)?;
        prop_assert!(!bytes.is_empty());
        bytes.clear();
        generator.to_bytes(&mut rng, budget, &mut bytes)?;
        prop_assert!(bytes.is_empty());
        prop_assert_eq!(generator.data_points_generated(), Some(0));
    }

    /// For all seeds: cached block metadata equals the number of spans in its protobuf body.
    #[test]
    fn cache_metadata_counts_encoded_spans(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let cache = Cache::fixed_with_max_overhead(
            &mut rng,
            NonZeroU32::new(8192).expect("nonzero"),
            4096,
            &crate::Config::TraceAgent(trace_agent::Config::AgentPayload(config(2))),
            8192,
        )?;
        let mut handle = cache.handle();
        for _ in 0..8 {
            let block = cache.advance(&mut handle);
            let payload = wire::AgentPayload::decode(block.bytes.clone())?;
            let spans: usize = payload.tracer_payloads.iter()
                .flat_map(|tracer| &tracer.chunks)
                .map(|chunk| chunk.spans.len())
                .sum();
            prop_assert!(spans > 0);
            prop_assert_eq!(block.metadata.data_points, Some(spans as u64));
        }
    }
}

/// Reuse the v0.4 validator rather than admitting zero-span traces or invalid error rates.
#[test]
fn invalid_trace_configuration_is_rejected() {
    let mut configuration = config(0);
    let mut rng = StdRng::seed_from_u64(0);
    assert!(AgentPayload::with_config(configuration.clone(), &mut rng).is_err());
    configuration.spans_per_trace = ConfRange::Constant(1);
    configuration.error_rate = f32::NAN;
    assert!(AgentPayload::with_config(configuration, &mut rng).is_err());
}

/// The intake protocol is not the tracer-facing v0.2 `MessagePack` endpoint.
#[test]
fn intake_and_tracer_protocols_have_distinct_routes_and_media_types() {
    let agent = trace_agent::Config::AgentPayload(config(1));
    assert_eq!(agent.endpoint_path(), "/api/v0.2/traces");
    assert_eq!(agent.content_type(), "application/x-protobuf");
    assert_eq!(agent.version(), "agent_payload");
    assert_eq!(
        trace_agent::Config::default().endpoint_path(),
        "/v0.4/traces"
    );
    assert_eq!(
        trace_agent::Config::default().content_type(),
        "application/msgpack"
    );
    let v1 = trace_agent::Config::V1(v1::Config::default());
    assert_eq!(v1.endpoint_path(), "/v1.0/traces");
    assert_eq!(v1.content_type(), "application/msgpack");
}
