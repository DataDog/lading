//! Payload generation tool for lading configurations.

#![allow(clippy::print_stdout)]

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tokio::fs;
use tokio::runtime::Builder;

use anyhow::{Context, Result, anyhow};
use byte_unit::{Byte, Unit, UnitType};
use clap::Parser;
use lading::generator::{self, http::Method};
use lading_payload::block;
use rand::{SeedableRng, rngs::StdRng};
use sha2::{Digest, Sha256};

/// Fingerprint result containing both hash and entropy metrics.
#[derive(Debug)]
struct Fingerprint {
    /// SHA256 hash of the payload bytes
    hash: String,
    /// Shannon entropy in bits per byte
    entropy: f64,
}

impl Fingerprint {
    /// Parse a fingerprint from a string in the format: "<hash> entropy=<value>"
    fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return None;
        }
        let hash = parts[0].to_string();
        let entropy_part = parts[1].strip_prefix("entropy=")?;
        let entropy: f64 = entropy_part.parse().ok()?;
        Some(Self { hash, entropy })
    }

    /// Compare with another fingerprint. Hash must match exactly, entropy
    /// must be within tolerance (0.01 bits).
    fn matches(&self, other: &Fingerprint) -> bool {
        self.hash == other.hash && (self.entropy - other.entropy).abs() < 0.01
    }

    /// Compare with an expected string, parsing it first.
    fn matches_str(&self, expected: &str) -> bool {
        if let Some(expected_fp) = Self::parse(expected) {
            self.matches(&expected_fp)
        } else {
            // Fall back to hash-only comparison for backward compatibility
            self.hash == expected
        }
    }
}

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} entropy={:.4}", self.hash, self.entropy)
    }
}

/// Compute Shannon entropy (bits per byte) of a byte sequence.
#[allow(clippy::cast_precision_loss)]
fn shannon_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut freq = [0u64; 256];
    for &b in data {
        freq[b as usize] += 1;
    }
    let len = data.len() as f64;
    let mut entropy = 0.0;
    for &count in &freq {
        if count > 0 {
            let p = count as f64 / len;
            entropy -= p * p.log2();
        }
    }
    entropy
}

use tracing::{error, info, trace, warn};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};

const UDP_PACKET_LIMIT_BYTES: Byte =
    Byte::from_u64_with_unit(65_507, Unit::B).expect("valid bytes");

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to standard lading config file
    config_path: String,
    /// Optionally only run a single generator's payload
    #[clap(short, long)]
    generator_id: Option<String>,
    /// Generate and print fingerprints
    #[clap(short, long)]
    fingerprint: bool,
    /// Path to file containing expected fingerprints for verification
    #[clap(short, long)]
    verify: Option<PathBuf>,
}

fn generate_and_check(
    config: &lading_payload::Config,
    seed: [u8; 32],
    total_bytes: NonZeroU32,
    max_block_size: Byte,
    compute_fingerprint: bool,
) -> Result<Option<Fingerprint>> {
    let mut rng = StdRng::from_seed(seed);
    let start = Instant::now();
    let blocks = match block::Cache::fixed_with_max_overhead(
        &mut rng,
        total_bytes,
        max_block_size.as_u128(),
        config,
        // NOTE we bound payload generation to have overhead only equivalent to
        // the prebuild cache size, `total_bytes`. This means on systems with
        // plentiful memory we're under generating entropy, on systems with
        // minimal memory we're over-generating.
        //
        // `lading::get_available_memory` suggests we can learn to divvy this up
        // in the future.
        total_bytes.get() as usize,
    )? {
        block::Cache::Fixed { blocks, .. } => blocks,
    };
    info!("Payload generation took {:?}", start.elapsed());
    trace!("Payload: {:#?}", blocks);

    // Compute fingerprint if requested: SHA256 hash and Shannon entropy.
    let fingerprint = if compute_fingerprint {
        let mut hasher = Sha256::new();
        let mut all_bytes = Vec::new();
        for block in &blocks {
            hasher.update(&block.bytes);
            all_bytes.extend_from_slice(&block.bytes);
        }
        let result = hasher.finalize();
        let hash = format!("{result:x}");
        let entropy = shannon_entropy(&all_bytes);
        Some(Fingerprint { hash, entropy })
    } else {
        None
    };

    let mut total_generated_bytes: u32 = 0;
    for block in &blocks {
        total_generated_bytes += block.total_bytes.get();
    }
    let total_requested_bytes =
        Byte::from_u128(total_bytes.get().into()).expect("total_bytes must be non-zero");
    let total_requested_bytes_str = total_requested_bytes
        .get_appropriate_unit(UnitType::Binary)
        .to_string();
    if total_bytes.get().abs_diff(total_generated_bytes) > 1_000_000 {
        let total_generated_bytes = Byte::from_u128(total_generated_bytes.into())
            .expect("total_generated_bytes must be non-zero");
        let total_generated_bytes_str = total_generated_bytes
            .get_appropriate_unit(UnitType::Binary)
            .to_string();
        warn!(
            "Generator failed to generate {total_requested_bytes_str}, producing {total_generated_bytes_str} of data"
        );
    } else {
        info!("Generator succeeded in generating {total_requested_bytes_str} of data");
    }

    Ok(fingerprint)
}

#[allow(clippy::too_many_lines)]
fn check_generator(
    config: &generator::Config,
    compute_fingerprint: bool,
) -> Result<Option<Fingerprint>> {
    match &config.inner {
        generator::Inner::FileGen(_) => {
            if compute_fingerprint {
                warn!("FileGen not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("FileGen not supported")
        }
        generator::Inner::UnixDatagram(g) => {
            let max_block_size = UDP_PACKET_LIMIT_BYTES;
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(
                &g.variant,
                g.seed,
                total_bytes,
                max_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::Tcp(g) => {
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(
                &g.variant,
                g.seed,
                total_bytes,
                g.maximum_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::Udp(g) => {
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            let max_block_size = UDP_PACKET_LIMIT_BYTES;
            generate_and_check(
                &g.variant,
                g.seed,
                total_bytes,
                max_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::Http(g) => {
            let (variant, max_prebuild_cache_size_bytes) = match &g.method {
                Method::Post {
                    variant,
                    maximum_prebuild_cache_size_bytes,
                    block_cache_method: _,
                } => (variant, maximum_prebuild_cache_size_bytes),
            };
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(max_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(
                variant,
                g.seed,
                total_bytes,
                g.maximum_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::SplunkHec(_) => {
            if compute_fingerprint {
                warn!("SplunkHec not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("SplunkHec not supported")
        }
        generator::Inner::FileTree(_) => {
            if compute_fingerprint {
                warn!("FileTree not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("FileTree not supported")
        }
        generator::Inner::Grpc(g) => {
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(
                &g.variant,
                g.seed,
                total_bytes,
                g.maximum_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::UnixStream(g) => {
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(
                &g.variant,
                g.seed,
                total_bytes,
                g.maximum_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::PassthruFile(g) => {
            #[allow(clippy::cast_possible_truncation)]
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(
                &g.variant,
                g.seed,
                total_bytes,
                g.maximum_block_size,
                compute_fingerprint,
            )
        }
        generator::Inner::ProcessTree(_) => {
            if compute_fingerprint {
                warn!("ProcessTree not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("ProcessTree not supported")
        }
        generator::Inner::ProcFs(_) => {
            if compute_fingerprint {
                warn!("ProcFs not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("ProcFs not supported")
        }
        generator::Inner::Container(_) => {
            if compute_fingerprint {
                warn!("Container not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("Container not supported")
        }
        generator::Inner::Kubernetes(_) => {
            if compute_fingerprint {
                warn!("Kubernetes not supported for fingerprinting");
                return Ok(None);
            }
            unimplemented!("Kubernetes not supported")
        }
        generator::Inner::TraceAgent(g) => {
            let total_bytes =
                generator::trace_agent::validate_cache_size(g.maximum_prebuild_cache_size_bytes)
                    .map_err(|e| anyhow::anyhow!("Cache size validation failed: {e}"))?;
            let conf = lading_payload::Config::TraceAgent(g.variant);
            generate_and_check(
                &conf,
                g.seed,
                total_bytes,
                g.maximum_block_size,
                compute_fingerprint,
            )
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn inner_main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .finish()
        .init();

    info!("Welcome to payloadtool");
    let args = Args::parse();

    let config_path = Path::new(&args.config_path);
    let mut file: File = OpenOptions::new()
        .read(true)
        .open(config_path)
        .with_context(|| {
            format!(
                "Could not open configuration file at: {}",
                config_path.display()
            )
        })?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).with_context(|| {
        format!(
            "Failed to read configuration file at: {}",
            config_path.display()
        )
    })?;

    let config: lading::config::Config =
        serde_yaml::from_str(&contents).with_context(|| "Failed to deserialize configuration")?;
    info!(
        "Loaded configuration, found {} generators",
        config.generator.len()
    );

    if let Some(generator_id) = args.generator_id {
        let generator = config
            .generator
            .iter()
            .find(|g| {
                let Some(ref id) = g.general.id else {
                    return false;
                };
                *id == generator_id
            })
            .ok_or_else(|| anyhow!("No generator found with id: {generator_id}"))?;
        let fingerprint = check_generator(generator, args.fingerprint)?;
        if args.fingerprint
            && let Some(fp) = fingerprint
        {
            if let Some(verify_path) = args.verify {
                let expected_content =
                    fs::read_to_string(&verify_path).await.with_context(|| {
                        format!("Could not read verify file {}", verify_path.display())
                    })?;

                // Look for the specific generator ID in the file
                let expected = expected_content
                    .lines()
                    .find(|line| line.starts_with(&format!("{generator_id}: ")))
                    .and_then(|line| line.split(": ").nth(1))
                    .ok_or_else(|| {
                        anyhow!(
                            "No fingerprint found for {} in {}",
                            generator_id,
                            verify_path.display()
                        )
                    })?;

                if fp.matches_str(expected) {
                    info!("✓ Fingerprint matches expected value");
                } else {
                    error!("✗ Fingerprint mismatch!");
                    error!("  Expected: {expected}");
                    error!("  Got:      {fp}");
                    return Err(anyhow!("Fingerprint verification failed"));
                }
            } else {
                println!("{fp}");
            }
        }
    } else {
        let mut all_fingerprints = Vec::new();
        for generator in config.generator {
            let fingerprint = check_generator(&generator, args.fingerprint)?;
            if args.fingerprint
                && let Some(fp) = fingerprint
            {
                let gen_id = generator.general.id.as_deref().unwrap_or("<unnamed>");
                all_fingerprints.push((gen_id.to_string(), fp));
            }
        }
        if args.fingerprint && !all_fingerprints.is_empty() {
            if let Some(verify_path) = args.verify {
                // Read expected fingerprints from file
                let expected_content =
                    fs::read_to_string(&verify_path).await.with_context(|| {
                        format!("Could not read verify file {}", verify_path.display())
                    })?;

                let mut all_passed = true;
                for (id, fp) in &all_fingerprints {
                    let expected = expected_content
                        .lines()
                        .find(|line| line.starts_with(&format!("{id}: ")))
                        .and_then(|line| line.split(": ").nth(1));

                    if let Some(expected) = expected {
                        if fp.matches_str(expected) {
                            info!("✓ {id} fingerprint matches");
                        } else {
                            error!("✗ {id} fingerprint mismatch!");
                            error!("  Expected: {expected}");
                            error!("  Got:      {fp}");
                            all_passed = false;
                        }
                    } else {
                        warn!("No expected fingerprint found for {id}");
                    }
                }

                if !all_passed {
                    return Err(anyhow!("Fingerprint verification failed"));
                }
            } else {
                for (id, fp) in all_fingerprints {
                    println!("{id}: {fp}");
                }
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let runtime = Builder::new_multi_thread().enable_io().build()?;
    runtime.block_on(inner_main())
}
