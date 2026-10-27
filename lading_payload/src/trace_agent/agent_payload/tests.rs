use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
};

use proptest::prelude::*;
use prost::Message;
use rand::{SeedableRng, rngs::StdRng};

use super::{AgentPayload, Config, ConfigError, PRIORITY_NONE, wire};
use crate::{
    Serialize,
    block::Cache,
    common::config::ConfRange,
    trace_agent::{self, v1, v04},
};

fn config(spans: u8, hosts: u32, tracers: u32) -> Config {
    Config {
        hosts: ConfRange::Constant(hosts),
        tracers: ConfRange::Constant(tracers),
        traces: v04::Config {
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
        },
    }
}

/// Produce `blocks` payloads of at most `budget` bytes, decoding each non-empty one.
fn payloads(
    configuration: Config,
    seed: u64,
    blocks: usize,
    budget: usize,
) -> Result<Vec<wire::AgentPayload>, TestCaseError> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut generator = AgentPayload::with_config(configuration, &mut rng)?;
    let mut decoded = Vec::with_capacity(blocks);
    for _ in 0..blocks {
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, budget, &mut bytes)?;
        prop_assert!(bytes.len() <= budget);
        if !bytes.is_empty() {
            decoded.push(wire::AgentPayload::decode(bytes.as_slice())?);
        }
    }
    Ok(decoded)
}

proptest! {
    /// For all seeds and budgets: identical configuration and RNG state produce identical bytes.
    #[test]
    fn payloads_are_deterministic(seed: u64, spans in 1u8..8, budget in 0usize..32768) {
        let mut first_rng = StdRng::seed_from_u64(seed);
        let mut second_rng = StdRng::seed_from_u64(seed);
        let mut first = AgentPayload::with_config(config(spans, 2, 4), &mut first_rng)?;
        let mut second = AgentPayload::with_config(config(spans, 2, 4), &mut second_rng)?;
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
        let mut configuration = config(spans, 2, 4);
        configuration.traces.error_rate = if errors { 1.0 } else { 0.0 };
        configuration.traces.priority_weights = Some(BTreeMap::from([(priority, 1)]));
        let mut rng = StdRng::seed_from_u64(seed);
        let mut generator = AgentPayload::with_config(configuration, &mut rng)?;
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, budget, &mut bytes)?;
        prop_assert!(bytes.len() <= budget);
        if bytes.is_empty() {
            prop_assert_eq!(generator.data_points_generated(), Some(0));
        } else {
            let payload = wire::AgentPayload::decode(bytes.as_slice())?;
            prop_assert!(!payload.tracer_payloads.is_empty());
            let mut count = 0;
            for tracer in &payload.tracer_payloads {
                prop_assert!(!tracer.chunks.is_empty());
                for chunk in &tracer.chunks {
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
            }
            prop_assert_eq!(generator.data_points_generated(), Some(count));
        }
    }

    /// For all seeds and `1 <= hosts <= tracers`: payloads use at most `hosts` Agent
    /// identities and `tracers` tracer identities; each tracer identity always carries the
    /// same fields and belongs to exactly one host; and every identity field is non-empty.
    #[test]
    fn identities_are_bounded_and_stable(seed: u64, hosts in 1u32..5, extra in 0u32..6) {
        let tracers = hosts + extra;
        let decoded = payloads(config(1, hosts, tracers), seed, 64, 4096)?;
        prop_assert!(!decoded.is_empty());
        let mut agents = BTreeSet::new();
        // container_id -> (host identity, full tracer identity)
        let mut owners = BTreeMap::new();
        for payload in &decoded {
            let agent = (payload.host_name.clone(), payload.env.clone(), payload.agent_version.clone());
            prop_assert!(!agent.0.is_empty() && !agent.1.is_empty() && !agent.2.is_empty());
            agents.insert(agent.clone());
            for tracer in &payload.tracer_payloads {
                let identity = (
                    tracer.language_name.clone(),
                    tracer.language_version.clone(),
                    tracer.tracer_version.clone(),
                    tracer.runtime_id.clone(),
                    tracer.app_version.clone(),
                );
                prop_assert!(!tracer.container_id.is_empty());
                prop_assert!(!identity.0.is_empty() && !identity.3.is_empty());
                let owner = owners
                    .entry(tracer.container_id.clone())
                    .or_insert_with(|| (agent.clone(), identity.clone()));
                prop_assert_eq!(&owner.0, &agent);
                prop_assert_eq!(&owner.1, &identity);
            }
        }
        prop_assert!(agents.len() <= hosts as usize);
        prop_assert!(owners.len() <= tracers as usize);
    }

    /// For all seeds and `1 <= hosts <= tracers`: the generator holds exactly `hosts`
    /// distinct host names and exactly `tracers` distinct container and runtime IDs, so the
    /// configured cardinality is met rather than reduced by colliding random strings.
    #[test]
    fn identities_are_distinct(seed: u64, hosts in 1u32..64, extra in 0u32..256) {
        let tracers = hosts + extra;
        let mut rng = StdRng::seed_from_u64(seed);
        let generator = AgentPayload::with_config(config(1, hosts, tracers), &mut rng)?;
        let host_names: BTreeSet<_> = generator.hosts.iter().map(|host| &host.envelope.host_name).collect();
        let container_ids: BTreeSet<_> = generator.tracers.iter().map(|tracer| &tracer.container_id).collect();
        let runtime_ids: BTreeSet<_> = generator.tracers.iter().map(|tracer| &tracer.runtime_id).collect();
        prop_assert_eq!(host_names.len(), hosts as usize);
        prop_assert_eq!(container_ids.len(), tracers as usize);
        prop_assert_eq!(runtime_ids.len(), tracers as usize);
        prop_assert!(generator.hosts.iter().all(|host| !host.tracers.is_empty()));
    }

    /// For all seeds: without `priority_weights`, every chunk carries the Agent's
    /// `PriorityNone`, so receivers see no sampling decision.
    #[test]
    fn unset_priority_is_priority_none(seed: u64) {
        let mut configuration = config(2, 1, 2);
        configuration.traces.priority_weights = None;
        for payload in payloads(configuration, seed, 8, 4096)? {
            for chunk in payload.tracer_payloads.iter().flat_map(|tracer| &tracer.chunks) {
                prop_assert_eq!(chunk.priority, PRIORITY_NONE);
            }
        }
    }

    /// For all seeds: a rejected block clears the span count from the previous successful block.
    #[test]
    fn rejected_blocks_reset_the_span_count(seed: u64, budget in 0usize..32) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut generator = AgentPayload::with_config(config(1, 1, 1), &mut rng)?;
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
            &crate::Config::TraceAgent(trace_agent::Config::AgentPayload(config(2, 2, 4))),
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
    let mut configuration = config(0, 1, 1);
    let mut rng = StdRng::seed_from_u64(0);
    assert!(AgentPayload::with_config(configuration.clone(), &mut rng).is_err());
    configuration.traces.spans_per_trace = ConfRange::Constant(1);
    configuration.traces.error_rate = f32::NAN;
    assert!(AgentPayload::with_config(configuration, &mut rng).is_err());
}

/// Every host must own a tracer, and identity counts must be non-zero, ordered, and bounded.
#[test]
fn invalid_identity_bounds_are_rejected() {
    let cases = [
        (
            ConfRange::Constant(0),
            ConfRange::Constant(1),
            ConfigError::ZeroValue,
        ),
        (
            ConfRange::Constant(1),
            ConfRange::Constant(0),
            ConfigError::ZeroValue,
        ),
        (
            ConfRange::Inclusive { min: 3, max: 2 },
            ConfRange::Constant(4),
            ConfigError::InvalidRange,
        ),
        (
            ConfRange::Constant(1),
            ConfRange::Constant(100_001),
            ConfigError::TooManyIdentities,
        ),
        (
            ConfRange::Constant(3),
            ConfRange::Constant(2),
            ConfigError::FewerTracersThanHosts,
        ),
        // A sampled tracer count could fall below a sampled host count.
        (
            ConfRange::Inclusive { min: 1, max: 4 },
            ConfRange::Inclusive { min: 3, max: 8 },
            ConfigError::FewerTracersThanHosts,
        ),
    ];
    for (hosts, tracers, expected) in cases {
        let configuration = Config {
            hosts,
            tracers,
            ..config(1, 1, 1)
        };
        assert_eq!(configuration.valid(), Err(expected));
    }
    assert_eq!(Config::default().valid(), Ok(()));
}

/// The intake protocol is not the tracer-facing v0.2 `MessagePack` endpoint.
#[test]
fn intake_and_tracer_protocols_have_distinct_routes_and_media_types() {
    let agent = trace_agent::Config::AgentPayload(config(1, 1, 1));
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

/// A host owning the maximum number of tracers still produces small blocks, carrying
/// a tracer payload only for tracers that received chunks.
#[test]
fn many_tracers_on_one_host_emit_only_used_tracers() {
    let mut rng = StdRng::seed_from_u64(7);
    let mut generator =
        AgentPayload::with_config(config(1, 1, 100_000), &mut rng).expect("valid configuration");
    for _ in 0..64 {
        let mut bytes = Vec::new();
        generator
            .to_bytes(&mut rng, 2048, &mut bytes)
            .expect("serialize");
        let payload = wire::AgentPayload::decode(bytes.as_slice()).expect("decode");
        let chunks: usize = payload
            .tracer_payloads
            .iter()
            .map(|tracer| tracer.chunks.len())
            .sum();
        assert!(chunks > 0);
        assert!(payload.tracer_payloads.len() <= chunks);
        assert!(
            payload
                .tracer_payloads
                .iter()
                .all(|tracer| !tracer.chunks.is_empty())
        );
    }
}
