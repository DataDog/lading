use std::io::Read;
use std::time::Instant;
use std::{io, num::NonZeroU32};

use byte_unit::{Unit, UnitType};
use clap::Parser;
use jemallocator::Jemalloc;
use lading::generator::http::Method;
use lading_payload::block;
use rand::{SeedableRng, rngs::StdRng};
use tracing::{error, info, trace, warn};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const UDP_PACKET_LIMIT_BYTES: byte_unit::Byte =
    byte_unit::Byte::from_u64_with_unit(65_507, Unit::B).expect("valid bytes");

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to standard lading config file
    config_path: String,

    /// Optionally only run a single generator's payload
    #[clap(short, long)]
    generator_id: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid arguments specified")]
    InvalidArgs,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Block(#[from] lading_payload::block::Error),
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    #[error(transparent)]
    Deserialize(#[from] serde_yaml::Error),
}

fn generate_and_check(
    config: &lading_payload::Config,
    seed: [u8; 32],
    total_bytes: NonZeroU32,
    max_block_size: byte_unit::Byte,
) -> Result<(), Error> {
    let mut rng = StdRng::from_seed(seed);
    let start = Instant::now();
    let blocks = match block::Cache::fixed(&mut rng, total_bytes, max_block_size.as_u128(), config)?
    {
        block::Cache::Fixed { blocks, .. } => blocks,
    };
    info!("Payload generation took {:?}", start.elapsed());
    trace!("Payload: {:#?}", blocks);

    let mut total_generated_bytes: u32 = 0;
    for block in blocks.iter() {
        total_generated_bytes += block.total_bytes.get();
    }
    let total_requested_bytes =
        byte_unit::Byte::from_u128(total_bytes.get().into()).expect("total_bytes must be non-zero");
    let total_requested_bytes_str = total_requested_bytes
        .get_appropriate_unit(UnitType::Binary)
        .to_string();
    if total_bytes.get().abs_diff(total_generated_bytes) > 1_000_000 {
        let total_generated_bytes = byte_unit::Byte::from_u128(total_generated_bytes.into())
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

    Ok(())
}

fn check_generator(config: &lading::generator::Config) -> Result<(), Error> {
    match &config.inner {
        lading::generator::Inner::FileGen(_) => unimplemented!("FileGen not supported"),
        lading::generator::Inner::UnixDatagram(g) => {
            let max_block_size = UDP_PACKET_LIMIT_BYTES;
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, total_bytes, max_block_size)?;
        }
        lading::generator::Inner::Tcp(g) => {
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, total_bytes, g.maximum_block_size)?;
        }
        lading::generator::Inner::Udp(g) => {
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            let max_block_size = UDP_PACKET_LIMIT_BYTES;
            generate_and_check(&g.variant, g.seed, total_bytes, max_block_size)?;
        }
        lading::generator::Inner::Http(g) => {
            let (variant, max_prebuild_cache_size_bytes) = match &g.method {
                Method::Post {
                    variant,
                    maximum_prebuild_cache_size_bytes,
                    block_cache_method: _,
                } => (variant, maximum_prebuild_cache_size_bytes),
            };
            let total_bytes = NonZeroU32::new(max_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(variant, g.seed, total_bytes, g.maximum_block_size)?;
        }
        lading::generator::Inner::SplunkHec(_) => unimplemented!("SplunkHec not supported"),
        lading::generator::Inner::FileTree(_) => unimplemented!("FileTree not supported"),
        lading::generator::Inner::Grpc(g) => {
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, total_bytes, g.maximum_block_size)?;
        }
        lading::generator::Inner::UnixStream(g) => {
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, total_bytes, g.maximum_block_size)?;
        }
        lading::generator::Inner::PassthruFile(g) => {
            let total_bytes = NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, total_bytes, g.maximum_block_size)?;
        }
        lading::generator::Inner::ProcessTree(_) => unimplemented!("ProcessTree not supported"),
        lading::generator::Inner::ProcFs(_) => unimplemented!("ProcFs not supported"),
        lading::generator::Inner::Container(_) => unimplemented!("Container not supported"),
    };

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .finish()
        .init();

    info!("Welcome to payloadtool");
    let args = Args::parse();

    let config_path = std::path::Path::new(&args.config_path);
    let mut file: std::fs::File = std::fs::OpenOptions::new()
        .read(true)
        .open(config_path)
        .unwrap_or_else(|_| {
            panic!(
                "Could not open configuration file at: {}",
                config_path.display()
            )
        });
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let config: lading::config::Config = serde_yaml::from_str(&contents)?;
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
            .ok_or_else(|| {
                error!("No generator found with id: {}", generator_id);
                Error::InvalidArgs
            })?;
        check_generator(generator)?;
    } else {
        for generator in config.generator {
            check_generator(&generator)?;
        }
    }

    Ok(())
}
