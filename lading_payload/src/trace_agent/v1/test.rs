//! Invariants for configuration, generation, and serialization.
use std::num::NonZeroU32;

use rand::{SeedableRng, rngs::SmallRng};
use rustc_hash::FxHashMap;

use super::golden::decode;
use super::{
    AttributeValue, Config, Operation, Service, Span, SubOperation, TraceChunk, TracerPayload, V1,
};
use crate::Serialize;
use crate::block::Cache;
use crate::trace_agent;

fn service_graph() -> Config {
    Config {
        error_rate: 0.0,
        link_rate: 0.0,
        event_rate: 0.0,
        chunks_per_payload: 1,
        language_name: "go".to_string(),
        language_version: "1.24.2".to_string(),
        tracer_version: "2.5.0".to_string(),
        env: "production".to_string(),
        app_version: "1.0.0".to_string(),
        priority: 1,
        services: vec![
            Service {
                name: "api-gateway".to_string(),
                operations: vec![Operation {
                    id: "get-product".to_string(),
                    name: "http.request".to_string(),
                    resource: "GET /api/v1/products/{id}".to_string(),
                    span_type: "web".to_string(),
                    kind: 2,
                    component: "net/http".to_string(),
                    attributes: FxHashMap::from_iter([(
                        "http.method".to_string(),
                        super::ConfigAttributeValue::String("GET".to_string()),
                    )]),
                    suboperations: vec![SubOperation {
                        to: "product-service/get-product".to_string(),
                        rate: 1.0,
                    }],
                }],
            },
            Service {
                name: "product-service".to_string(),
                operations: vec![Operation {
                    id: "get-product".to_string(),
                    name: "grpc.server.request".to_string(),
                    resource: "/ProductService/GetProduct".to_string(),
                    span_type: "web".to_string(),
                    kind: 2,
                    component: "grpc".to_string(),
                    attributes: FxHashMap::default(),
                    suboperations: vec![
                        SubOperation {
                            to: "product-cache/get-product-by-id".to_string(),
                            rate: 1.0,
                        },
                        SubOperation {
                            to: "product-db/select-product-by-id".to_string(),
                            rate: 0.1,
                        },
                    ],
                }],
            },
            Service {
                name: "product-cache".to_string(),
                operations: vec![Operation {
                    id: "get-product-by-id".to_string(),
                    name: "redis.command".to_string(),
                    resource: "GET products:by_id:$1".to_string(),
                    span_type: "redis".to_string(),
                    kind: 3,
                    component: "redis".to_string(),
                    attributes: FxHashMap::default(),
                    suboperations: Vec::new(),
                }],
            },
            Service {
                name: "product-db".to_string(),
                operations: vec![Operation {
                    id: "select-product-by-id".to_string(),
                    name: "postgres.query".to_string(),
                    resource: "SELECT * FROM products WHERE id = $1".to_string(),
                    span_type: "sql".to_string(),
                    kind: 3,
                    component: "postgresql".to_string(),
                    attributes: FxHashMap::default(),
                    suboperations: Vec::new(),
                }],
            },
        ],
    }
}

proptest::proptest! {
    /// For every seed and batch size, successful serialization preserves the exact chunk count,
    /// reports the decoded span count, and emits enabled errors, links, and events.
    #[test]
    fn serialized_batches_preserve_configured_workload(
        seed: u64,
        chunks in 1usize..9,
        inject_errors: bool,
        inject_links: bool,
        inject_events: bool,
    ) {
        let mut config = service_graph();
        config.chunks_per_payload = chunks;
        config.error_rate = f64::from(inject_errors);
        config.link_rate = f64::from(inject_links);
        config.event_rate = f64::from(inject_events);
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut generator = V1::with_config(config.clone(), &mut rng).expect("valid graph");
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, 1_048_576, &mut bytes).expect("serialize");
        let payload = decode(&bytes).expect("generated payload decodes");
        proptest::prop_assert_eq!(payload.chunks.len(), chunks);
        let spans: Vec<_> = payload.chunks.iter().flat_map(|chunk| &chunk.spans).collect();
        proptest::prop_assert_eq!(generator.data_points_generated(), Some(spans.len() as u64));
        for span in spans {
            proptest::prop_assert_eq!(span.error, inject_errors);
            proptest::prop_assert_eq!(span.links.len(), usize::from(inject_links));
            proptest::prop_assert_eq!(span.events.len(), usize::from(inject_events));
            for key in ["error.type", "error.message"] {
                proptest::prop_assert_eq!(
                    span.attributes.iter().filter(|(name, _)| name == key).count(),
                    usize::from(inject_errors),
                );
            }
        }
        // Increasing an already sufficient byte budget must not change the payload or RNG use.
        let mut repeated_rng = SmallRng::seed_from_u64(seed);
        let mut repeated = V1::with_config(config, &mut repeated_rng).expect("valid graph");
        let mut repeated_bytes = Vec::new();
        repeated.to_bytes(&mut repeated_rng, 2_097_152, &mut repeated_bytes).expect("serialize");
        proptest::prop_assert_eq!(bytes, repeated_bytes);
    }

    /// For every seed, configured error attributes survive injection once each on the wire.
    #[test]
    fn error_injection_preserves_configured_attributes(seed: u64, message in "[a-z]{1,32}") {
        let mut config = service_graph();
        config.error_rate = 1.0;
        let operation = &mut config.services[0].operations[0];
        for (key, value) in [("error.type", "CustomError"), ("error.message", message.as_str())] {
            operation.attributes.insert(key.to_string(), super::ConfigAttributeValue::String(value.to_string()));
        }
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut generator = V1::with_config(config, &mut rng).expect("valid graph");
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, 65_536, &mut bytes).expect("serialize");
        let payload = decode(&bytes).expect("generated payload decodes");
        let root = &payload.chunks[0].spans[0];
        proptest::prop_assert!(root.error);
        for (key, value) in [("error.type", "CustomError"), ("error.message", message.as_str())] {
            let matching: Vec<_> = root.attributes.iter().filter(|(name, _)| name == key).collect();
            proptest::prop_assert_eq!(matching.len(), 1);
            proptest::prop_assert_eq!(&matching[0].1, &AttributeValue::String(value.to_string()));
        }
    }

    /// For every seed, rejecting a batch writes no bytes and clears the previous span count.
    #[test]
    fn rejected_batches_clear_span_count(seed: u64, budget in 0usize..32) {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut generator = V1::with_config(service_graph(), &mut rng).expect("valid graph");
        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, 65_536, &mut bytes).expect("serialize");
        proptest::prop_assert!(generator.data_points_generated().expect("count") > 0);
        bytes.clear();
        generator.to_bytes(&mut rng, budget, &mut bytes).expect("reject");
        proptest::prop_assert!(bytes.is_empty());
        proptest::prop_assert_eq!(generator.data_points_generated(), Some(0));
    }
}

/// A batch larger than every allowed block must return an actionable error.
#[test]
fn impossible_batch_fails_cache_construction() {
    let mut config = service_graph();
    config.chunks_per_payload = 100;
    let mut rng = SmallRng::seed_from_u64(0);
    let error = Cache::fixed_with_max_overhead(
        &mut rng,
        NonZeroU32::new(4096).expect("nonzero"),
        1024,
        &crate::Config::TraceAgent(trace_agent::Config::V1(config)),
        0,
    )
    .expect_err("100 chunks cannot fit in 1 KiB");
    assert!(error.to_string().contains("maximum_block_size"));
    assert!(error.to_string().contains("chunks_per_payload"));
}

proptest::proptest! {
    #[test]
    fn the_same_seed_produces_byte_identical_payloads(seed: u64) {
        let config = service_graph();
        let mut first = V1::with_config(config.clone(), &mut SmallRng::seed_from_u64(0))
            .expect("config should be valid");
        let mut second = V1::with_config(config, &mut SmallRng::seed_from_u64(0))
            .expect("config should be valid");

        let mut rng_one = SmallRng::seed_from_u64(seed);
        let mut rng_two = SmallRng::seed_from_u64(seed);

        let (payload_one, _) = first
            .generate_payload(&mut rng_one, 1)
            .expect("should not fail to generate");
        let (payload_two, _) = second
            .generate_payload(&mut rng_two, 1)
            .expect("should not fail to generate");

        proptest::prop_assert_eq!(payload_one, payload_two);
    }
}

proptest::proptest! {
    #[test]
    fn child_spans_are_nested_inside_their_parents_window(seed: u64) {
        let config = service_graph();
        let generator =
            V1::with_config(config, &mut SmallRng::seed_from_u64(0))
                .expect("config should be valid");
        let mut rng = SmallRng::seed_from_u64(seed);

        for _ in 0..16 {
            let chunk = generator
                .generate_chunk(&mut rng)
                .expect("should not fail to generate");
            let parents: FxHashMap<u64, &Span> =
                chunk.spans.iter().map(|span| (span.span_id, span)).collect();
            for span in &chunk.spans {
                if span.parent_id == 0 {
                    continue;
                }
                // A parent is always generated into the chunk before its children, so the
                // lookup cannot miss.
                let parent = parents
                    .get(&span.parent_id)
                    .expect("parent span exists in chunk");
                proptest::prop_assert!(span.start >= parent.start);
                proptest::prop_assert!(
                    span.start + span.duration <= parent.start + parent.duration
                );
            }
        }
    }
}

#[test]
fn validation_rejects_a_rate_outside_the_unit_interval() {
    let mut config = service_graph();
    config.error_rate = 1.5;
    assert!(config.valid().is_err());

    config.error_rate = -0.1;
    assert!(config.valid().is_err());
}

#[test]
fn validation_rejects_a_duplicated_operation() {
    let mut config = service_graph();
    let duplicated = config.services[0].operations[0].clone();
    config.services[0].operations.push(duplicated);
    assert!(config.valid().is_err());
}

#[test]
fn validation_rejects_an_unknown_suboperation() {
    let mut config = service_graph();
    config.services[0].operations[0].suboperations[0].to = "nowhere/nothing".to_string();
    assert!(config.valid().is_err());
}

#[test]
fn validation_rejects_an_operation_the_normalizer_would_rewrite() {
    let mut config = service_graph();
    config.services[0].operations[0].resource = String::new();
    assert!(config.valid().is_err());
}

#[test]
fn error_injection_marks_spans_the_way_real_tracers_do() {
    let mut config = service_graph();
    config.error_rate = 1.0;
    let generator =
        V1::with_config(config, &mut SmallRng::seed_from_u64(0)).expect("config should be valid");
    let mut rng = SmallRng::seed_from_u64(11);

    let chunk = generator
        .generate_chunk(&mut rng)
        .expect("should not fail to generate");
    assert!(!chunk.spans.is_empty());
    for span in &chunk.spans {
        assert!(span.error, "every span should carry the error bit");
        let find = |key: &str| {
            span.attributes
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, value)| value)
                .expect("attribute is present")
        };
        // The error bit plus error attributes is what real tracers emit and what the
        // receiving agent's error tracking reads; never a transport status code.
        assert_eq!(
            find("error.type"),
            &AttributeValue::String("RuntimeError".to_string())
        );
        assert_eq!(
            find("error.message"),
            &AttributeValue::String("synthetic error".to_string())
        );
        assert!(!span.attributes.iter().any(|(k, _)| k == "http.status_code"));
    }
}

#[test]
fn a_suboperation_rate_of_zero_never_produces_a_child_span() {
    let mut config = service_graph();
    for service in &mut config.services {
        for operation in &mut service.operations {
            for suboperation in &mut operation.suboperations {
                suboperation.rate = 0.0;
            }
        }
    }

    let mut generator =
        V1::with_config(config, &mut SmallRng::seed_from_u64(0)).expect("config should be valid");
    let mut rng = SmallRng::seed_from_u64(7);

    // With every suboperation rate at zero, each payload's single chunk holds exactly one
    // span: the entry point.
    let (payload, span_count) = generator
        .generate_payload(&mut rng, 1)
        .expect("should not fail to generate");
    assert_eq!(span_count, 1);
    assert!(!payload.is_empty());

    // With one span per chunk, the count equals the exact configured chunk count.
    let mut bytes = Vec::new();
    generator
        .to_bytes(&mut rng, 65_536, &mut bytes)
        .expect("should not fail to generate");
    assert_eq!(generator.last_span_count, 1);
    assert!(!bytes.is_empty());
}

#[test]
fn a_repeated_string_is_written_as_an_index() {
    // Both fields carry `go`, so the first writes it inline and takes table index 1, and the
    // second writes the index. Field 3 is the language name and field 7 is the environment.
    let payload = TracerPayload {
        language_name: "go".to_string(),
        env: "go".to_string(),
        ..Default::default()
    };

    let encoded = payload
        .encode()
        .expect("encoding into a buffer cannot fail");

    assert_eq!(encoded, vec![0x82, 0x03, 0xa2, b'g', b'o', 0x07, 0x01]);
}

#[test]
fn the_empty_string_is_written_as_index_zero() {
    // An inline empty literal would take a table slot on the reading side and shift every
    // later index by one, so the empty string is always written as its pre-seeded index
    // instead.
    let payload = TracerPayload {
        attributes: vec![("k".to_string(), AttributeValue::String(String::new()))],
        ..Default::default()
    };

    let encoded = payload
        .encode()
        .expect("encoding into a buffer cannot fail");

    // One field (10, attributes), holding a three-slot array: key `k` inline, type 1 (string),
    // then index 0 for the value.
    assert_eq!(encoded, vec![0x81, 0x0a, 0x93, 0xa1, b'k', 0x01, 0x00]);
}

#[test]
fn fields_holding_their_zero_value_are_omitted() {
    let encoded = TracerPayload::default()
        .encode()
        .expect("encoding into a buffer cannot fail");

    // A map header declaring no fields at all, and nothing after it.
    assert_eq!(encoded, vec![0x80]);
}

#[test]
fn an_attribute_array_declares_three_slots_per_entry() {
    let payload = TracerPayload {
        attributes: vec![
            ("a".to_string(), AttributeValue::Bool(true)),
            ("b".to_string(), AttributeValue::Int(-1)),
        ],
        ..Default::default()
    };

    let encoded = payload
        .encode()
        .expect("encoding into a buffer cannot fail");

    assert_eq!(encoded[0..3], [0x81, 0x0a, 0x96]);
}

#[test]
fn string_indices_are_assigned_in_write_order_across_nesting_levels() {
    // `svc` first appears as the payload hostname and again as the span service, so the span
    // must reference it by the index the hostname established rather than repeating it
    // inline.
    let payload = TracerPayload {
        hostname: "svc".to_string(),
        chunks: vec![TraceChunk {
            spans: vec![Span {
                service: "svc".into(),
                span_id: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let encoded = payload
        .encode()
        .expect("encoding into a buffer cannot fail");

    assert_eq!(
        encoded,
        vec![
            0x82, // payload map, two fields
            0x08, 0xa3, b's', b'v', b'c', // field 8 (hostname), inline `svc`, taking index 1
            0x0b, 0x91, // field 11 (chunks), one chunk
            0x81, 0x04, 0x91, // chunk map with one field: 4 (spans), one span
            0x82, // span map, two fields
            0x01, 0x01, // field 1 (service), string index 1
            0x04, 0x01, // field 4 (spanID), 1
        ]
    );
}

proptest::proptest! {
    #[test]
    fn payload_not_exceed_max_bytes(seed: u64, max_bytes in 0usize..=262_144) {
        let config = service_graph();
        let mut generator = V1::with_config(config, &mut SmallRng::seed_from_u64(0)).expect("config should be valid");
        let mut rng = SmallRng::seed_from_u64(seed);

        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, max_bytes, &mut bytes).expect("should not fail to generate");

        // A request too small to carry the configured batch of chunks writes nothing,
        // which the block cache reads as a rejected block.
        proptest::prop_assert!(bytes.is_empty() || bytes.len() <= max_bytes);
    }
}
