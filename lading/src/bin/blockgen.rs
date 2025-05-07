use std::{fs, io::Read, num::NonZeroU32, path, process::exit, time::Instant};

use byte_unit::{Unit, UnitType};
use clap::Parser;
use lading::generator;
use lading_payload::block;
use rand::{SeedableRng, rngs::StdRng};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};

const UDP_PACKET_LIMIT_BYTES: byte_unit::Byte =
    byte_unit::Byte::from_u64_with_unit(65_507, Unit::B).expect("valid bytes");

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to standard lading config file
    config_path: String,
}

fn generate_blocks(
    config: &lading_payload::Config,
    seed: [u8; 32],
    total_bytes: NonZeroU32,
    max_block_size: byte_unit::Byte,
) -> Result<(), lading_payload::block::Error> {
    let mut rng = StdRng::from_seed(seed);
    let start = Instant::now();
    let blocks = match block::Cache::fixed(&mut rng, total_bytes, max_block_size.as_u128(), config)?
    {
        block::Cache::Fixed { blocks, .. } => blocks,
    };
    info!("Payload generation took {:?}", start.elapsed());
    let mut total_generated_bytes: u32 = 0;
    for block in blocks.iter() {
        total_generated_bytes += block.total_bytes.get();
    }
    if total_bytes.get() != total_generated_bytes {
        let total_requested_bytes = byte_unit::Byte::from_u128(total_bytes.get().into())
            .expect("total_bytes must be non-zero");
        let total_requested_bytes_str = total_requested_bytes
            .get_appropriate_unit(UnitType::Binary)
            .to_string();
        let total_generated_bytes = byte_unit::Byte::from_u128(total_generated_bytes.into())
            .expect("total_generated_bytes must be non-zero");
        let total_generated_bytes_str = total_generated_bytes
            .get_appropriate_unit(UnitType::Binary)
            .to_string();
        warn!(
            "Generator failed to generate {total_requested_bytes_str}, instead only found {total_generated_bytes_str} of data"
        )
    }
    Ok(())
}

fn main() {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .finish()
        .init();

    let args = Args::parse();
    let config_path = path::Path::new(&args.config_path);
    let mut file: fs::File = match fs::OpenOptions::new().read(true).open(config_path) {
        Ok(f) => f,
        Err(e) => {
            error!(
                "Could not open configuration file at: {}: {e}",
                config_path.display()
            );
            exit(1);
        }
    };
    let mut contents = String::new();
    if let Err(e) = file.read_to_string(&mut contents) {
        error!("Could not read configuration file: {e}");
        exit(1);
    }
    let config: lading::config::Config = match serde_yaml::from_str(&contents) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to parse configuration: {e}");
            exit(1);
        }
    };
    info!(
        "Loaded configuration, found {} generators",
        config.generator.len()
    );
    for generator in config.generator {
        match &generator.inner {
            generator::Inner::FileGen(_)
            | generator::Inner::SplunkHec(_)
            | generator::Inner::FileTree(_)
            | generator::Inner::ProcessTree(_)
            | generator::Inner::ProcFs(_)
            | generator::Inner::Container(_) => {
                warn!("Generator type not supported in blockgen, skipping");
                continue;
            }
            generator::Inner::UnixDatagram(g) => {
                let max_block_size = UDP_PACKET_LIMIT_BYTES;
                let total_bytes =
                    match NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                if let Err(e) = generate_blocks(&g.variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
            generator::Inner::Udp(g) => {
                let max_block_size = UDP_PACKET_LIMIT_BYTES;
                let total_bytes =
                    match NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                if let Err(e) = generate_blocks(&g.variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
            generator::Inner::Tcp(g) => {
                let total_bytes =
                    match NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                let max_block_size = g.maximum_block_size;
                if let Err(e) = generate_blocks(&g.variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
            generator::Inner::Http(g) => {
                let (variant, max_prebuild_cache_size_bytes) = match &g.method {
                    generator::http::Method::Post {
                        variant,
                        maximum_prebuild_cache_size_bytes,
                        ..
                    } => (variant, maximum_prebuild_cache_size_bytes),
                };
                let total_bytes =
                    match NonZeroU32::new(max_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                let max_block_size = g.maximum_block_size;
                if let Err(e) = generate_blocks(variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
            generator::Inner::Grpc(g) => {
                let total_bytes =
                    match NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                let max_block_size = g.maximum_block_size;
                if let Err(e) = generate_blocks(&g.variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
            generator::Inner::UnixStream(g) => {
                let total_bytes =
                    match NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                let max_block_size = g.maximum_block_size;
                if let Err(e) = generate_blocks(&g.variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
            generator::Inner::PassthruFile(g) => {
                let total_bytes =
                    match NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32) {
                        Some(nz) => nz,
                        None => {
                            warn!("Non-zero max prebuild cache size required, skipping");
                            continue;
                        }
                    };
                let max_block_size = g.maximum_block_size;
                if let Err(e) = generate_blocks(&g.variant, g.seed, total_bytes, max_block_size) {
                    error!("Block generation failed: {e}");
                    exit(1);
                }
            }
        }
    }
}
