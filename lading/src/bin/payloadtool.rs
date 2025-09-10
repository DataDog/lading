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
) -> Result<Option<String>> {
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

    // Compute fingerprint if requested, done by iterating the generated blocks
    // to the end and sha256'ing the blocks.
    let fingerprint = if compute_fingerprint {
        let mut hasher = Sha256::new();
        for block in blocks.iter() {
            hasher.update(&block.bytes);
        }
        let result = hasher.finalize();
        Some(format!("{result:x}"))
    } else {
        None
    };

    let mut total_generated_bytes: u32 = 0;
    for block in blocks.iter() {
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
        )
    } else {
        info!("Generator succeeded in generating {total_requested_bytes_str} of data")
    }

    Ok(fingerprint)
}

fn check_generator(
    config: &generator::Config,
    compute_fingerprint: bool,
) -> Result<Option<String>> {
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
    }
}

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
            .ok_or_else(|| anyhow!("No generator found with id: {}", generator_id))?;
        let fingerprint = check_generator(generator, args.fingerprint)?;
        if args.fingerprint
            && let Some(fp) = fingerprint {
                if let Some(verify_path) = args.verify {
                    let expected_content =
                        fs::read_to_string(&verify_path).await.with_context(|| {
                            format!("Could not read verify file {}", verify_path.display())
                        })?;

                    // Look for the specific generator ID in the file
                    let expected = expected_content
                        .lines()
                        .find(|line| line.starts_with(&format!("{}: ", generator_id)))
                        .and_then(|line| line.split(": ").nth(1))
                        .ok_or_else(|| {
                            anyhow!(
                                "No fingerprint found for {} in {}",
                                generator_id,
                                verify_path.display()
                            )
                        })?;

                    if fp == expected {
                        info!("✓ Fingerprint matches expected value");
                    } else {
                        error!("✗ Fingerprint mismatch!");
                        error!("  Expected: {}", expected);
                        error!("  Got:      {}", fp);
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
                && let Some(fp) = fingerprint {
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
                        .find(|line| line.starts_with(&format!("{}: ", id)))
                        .and_then(|line| line.split(": ").nth(1));

                    if let Some(expected) = expected {
                        if fp == expected {
                            info!("✓ {} fingerprint matches", id);
                        } else {
                            error!("✗ {} fingerprint mismatch!", id);
                            error!("  Expected: {}", expected);
                            error!("  Got:      {}", fp);
                            all_passed = false;
                        }
                    } else {
                        warn!("No expected fingerprint found for {}", id);
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
