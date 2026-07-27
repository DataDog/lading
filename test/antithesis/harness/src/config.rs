//! Per-timeline lading config sampling.
//!
//! The sampler builds lading's real [`tcp::Config`] so the value menu is exactly
//! the config schema, then serializes it under the `generator: [{ tcp: … }]`
//! shape lading parses. The transport is fixed to TCP against the sink oracle,
//! which counts bytes and so catches any payload variant; only the free axes
//! (payload variant, rate, connections, block sizes, seed) vary. Transport
//! variation waits on a multi-protocol sink.

use lading::generator::tcp;
use rand::seq::IndexedRandom;
use rand::{Rng, RngExt};

/// Fixed address of the sink oracle every sampled config targets.
const SINK_ADDR: &str = "sink:9000";

/// Payload variants the TCP sink catches with no decoding. All are fieldless, so
/// they need no extra configuration to sample.
fn variant_menu() -> [lading_payload::Config; 6] {
    [
        lading_payload::Config::Ascii,
        lading_payload::Config::Syslog5424,
        lading_payload::Config::Json,
        lading_payload::Config::Fluent,
        lading_payload::Config::ApacheCommon,
        lading_payload::Config::DatadogLog,
    ]
}

/// Sample a lading TCP generator config for one timeline.
///
/// Draws the payload variant, throughput, parallel connections, and seed from
/// `rng`; the transport and target stay fixed to the sink.
#[must_use]
pub fn sample<R: Rng>(rng: &mut R) -> tcp::Config {
    // Structured choices come from the caller's rng -- AntithesisRng in
    // production -- so Antithesis branches each pick and sweeps the menu.
    let variant = variant_menu()
        .choose(rng)
        .cloned()
        .unwrap_or(lading_payload::Config::Ascii);

    let bps_mib = [1_u64, 5, 10, 50, 100].choose(rng).copied().unwrap_or(10);
    let bytes_per_second_bytes = bps_mib * 1024 * 1024;
    let bytes_per_second = Some(byte_unit::Byte::from_u64(bytes_per_second_bytes));

    let parallel_connections = rng.random_range(1..=8_u16);

    // Cap the block size at the smallest per-connection throttle capacity. lading
    // divides `bytes_per_second` evenly across `parallel_connections`; a block
    // larger than a worker's divided capacity is rejected by the throttle, and
    // the TCP worker then busy-spins discarding it with no backoff. Keeping
    // `maximum_block_size <= bytes_per_second / parallel_connections` guarantees
    // every block fits, so no timeline degrades into a discard spin.
    let per_connection_capacity = bytes_per_second_bytes / u64::from(parallel_connections);
    let maximum_block_size =
        byte_unit::Byte::from_u64(per_connection_capacity.clamp(1, 1024 * 1024));

    // lading seeds its own payload PRNG from `seed`. The Antithesis docs warn
    // against seeding your own RNG from SDK randomness, so draw the seed from
    // system entropy rather than the (Antithesis) `rng`. This makes payload byte
    // content opaque to Antithesis and effectively fixed across timelines, which
    // is fine: the sink asserts on bytes received, not on content.
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);

    tcp::Config {
        seed,
        addr: SINK_ADDR.to_string(),
        variant,
        bytes_per_second,
        maximum_block_size,
        maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64(8 * 1024 * 1024),
        parallel_connections,
        throttle: None,
    }
}

/// Serialize a sampled `tcp::Config` into the top-level `generator: [{ tcp: … }]`
/// YAML that lading consumes.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn to_yaml(cfg: &tcp::Config) -> Result<String, serde_yaml::Error> {
    let mut tcp_item = serde_yaml::Mapping::new();
    tcp_item.insert(serde_yaml::Value::from("tcp"), serde_yaml::to_value(cfg)?);
    let generators = serde_yaml::Value::Sequence(vec![serde_yaml::Value::Mapping(tcp_item)]);
    let mut top = serde_yaml::Mapping::new();
    top.insert(serde_yaml::Value::from("generator"), generators);
    serde_yaml::to_string(&serde_yaml::Value::Mapping(top))
}

/// Short, stable label for a payload variant, for tagging the Antithesis sample.
#[must_use]
pub fn variant_label(variant: &lading_payload::Config) -> &'static str {
    match variant {
        lading_payload::Config::Ascii => "ascii",
        lading_payload::Config::Syslog5424 => "syslog5424",
        lading_payload::Config::Json => "json",
        lading_payload::Config::Fluent => "fluent",
        lading_payload::Config::ApacheCommon => "apache_common",
        lading_payload::Config::DatadogLog => "datadog_log",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::{sample, to_yaml};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn sampled_config_deserializes_as_valid_lading_config() {
        // The load-bearing invariant: whatever we sample must be a config lading
        // actually accepts. Sweep many seeds so the whole menu is exercised.
        for s in 0..256_u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let cfg = sample(&mut rng);
            let yaml = to_yaml(&cfg).expect("serialize sampled config");
            let parsed: Result<lading::config::Config, _> = serde_yaml::from_str(&yaml);
            assert!(
                parsed.is_ok(),
                "seed {s} produced a config lading rejects: {err:?}\n{yaml}",
                err = parsed.err()
            );
        }
    }

    #[test]
    fn sampled_config_holds_invariants() {
        for s in 0..256_u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let cfg = sample(&mut rng);
            assert_eq!(cfg.addr, "sink:9000");
            assert!((1..=8).contains(&cfg.parallel_connections));
            assert!(cfg.bytes_per_second.is_some());
        }
    }

    #[test]
    fn block_size_never_exceeds_per_connection_capacity() {
        // Regression: lading divides bytes_per_second across parallel_connections,
        // and a block larger than a worker's divided capacity busy-spins on
        // discard. maximum_block_size must fit the smallest per-connection share.
        for s in 0..256_u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let cfg = sample(&mut rng);
            let bps = cfg.bytes_per_second.expect("bytes_per_second set").as_u64();
            let per_connection = bps / u64::from(cfg.parallel_connections);
            assert!(
                cfg.maximum_block_size.as_u64() <= per_connection,
                "seed {s}: block {block} exceeds per-connection capacity {per_connection}",
                block = cfg.maximum_block_size.as_u64()
            );
        }
    }
}
