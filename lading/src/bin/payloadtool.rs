use std::io::Read;
use std::time::Instant;
use std::{io, num::NonZeroU32};

use byte_unit::{Byte, ByteUnit};
use clap::Parser;
use lading_payload::block;
use rand::{rngs::StdRng, SeedableRng};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};

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
    #[error(transparent)]
    Byte(#[from] byte_unit::ByteError),
    #[error(transparent)]
    Deserialize(#[from] serde_yaml::Error),
}

fn powers_of_two_strategy(start_size: u32) -> Vec<NonZeroU32> {
    (0..8)
        .map(|i| NonZeroU32::new(start_size * 2_u32.pow(i)).unwrap())
        .collect()
}

fn fibonacci_strategy(start_size: u32) -> Vec<NonZeroU32> {
    let mut vec = Vec::new();
    let (mut a, mut b) = (start_size, start_size);
    vec.push(NonZeroU32::new(a).unwrap());
    while b < 50_000 {
        // Adjust this limit as needed
        vec.push(NonZeroU32::new(b).unwrap());
        let next = a + b;
        a = b;
        b = next;
    }
    vec
}

fn default_blocks() -> Vec<NonZeroU32> {
    [
        Byte::from_unit(1.0 / 64.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 32.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 16.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 8.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 4.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 2.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
        Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
        Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
    ]
    .iter()
    .map(|sz| NonZeroU32::new(sz.get_bytes() as u32).expect("bytes must be non-zero"))
    .collect()
}

fn udp_friendly_default_blocks() -> Vec<NonZeroU32> {
    [
        Byte::from_unit(1.0 / 64.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 32.0, ByteUnit::MB).unwrap(),
        Byte::from_unit(1.0 / 16.0, ByteUnit::MB).unwrap(),
    ]
    .iter()
    .map(|sz| NonZeroU32::new(sz.get_bytes() as u32).expect("bytes must be non-zero"))
    .collect()
}

fn generate_and_check(
    config: &lading_payload::Config,
    seed: [u8; 32],
    block_sizes: Option<Vec<byte_unit::Byte>>,
    total_bytes: NonZeroU32,
) -> Result<(), Error> {
    let mut rng = StdRng::from_seed(seed);
    let block_sizes: Vec<NonZeroU32> = match block_sizes {
        Some(sizes) => {
            info!("Using custom block sizes: {:#?}", sizes);
            sizes
                .iter()
                .map(|sz| NonZeroU32::new(sz.get_bytes() as u32).expect("bytes must be non-zero"))
                .collect()
        }
        None => {
            info!("Using default block sizes");
            //fibonacci_strategy(100)
            udp_friendly_default_blocks()
            //powers_of_two_strategy(500)
        }
    };
    info!(
        "Generating {} bytes with block sizes: {:#?}",
        total_bytes, block_sizes
    );
    let start = Instant::now();
    let blocks = match block::Cache::fixed(&mut rng, total_bytes, &block_sizes, config)? {
        block::Cache::Fixed { blocks, idx: _ } => blocks,
        _ => unreachable!(),
    };
    info!("Payload generation took {:?}", start.elapsed());
    debug!("Payload: {:#?}", blocks);

    let mut total_generated_bytes: u32 = 0;
    for block in blocks.iter() {
        total_generated_bytes += block.total_bytes.get();
    }
    if total_bytes.get() != total_generated_bytes {
        let total_requested_bytes = byte_unit::Byte::from_bytes(total_bytes.get().into());
        let total_requested_bytes_str = total_requested_bytes
            .get_appropriate_unit(false)
            .to_string();
        let total_generated_bytes = byte_unit::Byte::from_bytes(total_generated_bytes.into());
        let total_generated_bytes_str = total_generated_bytes
            .get_appropriate_unit(false)
            .to_string();
        warn!("Generator failed to generate {total_requested_bytes_str}, instead only found {total_generated_bytes_str} of data")
    }

    Ok(())
}

fn check_generator(config: &lading::generator::Config) -> Result<(), Error> {
    match &config.inner {
        lading::generator::Inner::FileGen(_) => unimplemented!("FileGen not supported"),
        lading::generator::Inner::UnixDatagram(g) => {
            let total_bytes =
                NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, g.block_sizes.clone(), total_bytes)?;
        }
        lading::generator::Inner::Tcp(g) => {
            let total_bytes =
                NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, g.block_sizes.clone(), total_bytes)?;
        }
        lading::generator::Inner::Udp(g) => {
            let total_bytes =
                NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, g.block_sizes.clone(), total_bytes)?;
        }
        lading::generator::Inner::Http(_) => unimplemented!("Http not supported"),
        lading::generator::Inner::SplunkHec(_) => unimplemented!("SplunkHec not supported"),
        lading::generator::Inner::FileTree(_) => unimplemented!("FileTree not supported"),
        lading::generator::Inner::Grpc(g) => {
            let total_bytes =
                NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, g.block_sizes.clone(), total_bytes)?;
        }
        lading::generator::Inner::UnixStream(g) => {
            let total_bytes =
                NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, g.block_sizes.clone(), total_bytes)?;
        }
        lading::generator::Inner::PassthruFile(g) => {
            let total_bytes =
                NonZeroU32::new(g.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("Non-zero max prebuild cache size");
            generate_and_check(&g.variant, g.seed, g.block_sizes.clone(), total_bytes)?;
        }
        lading::generator::Inner::ProcessTree(_) => unimplemented!("ProcessTree not supported"),
        lading::generator::Inner::ProcFs(_) => unimplemented!("ProcFs not supported"),
    };

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
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
