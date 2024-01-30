use std::collections::{HashMap, HashSet};

use average::{concatenate, Estimate, Quantile, Variance};
use clap::Parser;
use futures::io;
use lading_capture::json::Line;
use rustc_hash::FxHashMap;
use tokio::io::AsyncReadExt;
use tracing::{error, info};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// list metric names
    #[clap(short, long)]
    list_names: bool,

    /// show details about a metric
    #[clap(short, long)]
    metric: Option<String>,

    /// Path to line-delimited capture file
    capture_path: String,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid arguments specified")]
    InvalidArgs,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::FULL)
        .with_ansi(false)
        .finish()
        .init();

    info!("Welcome to captool");
    let args = Args::parse();

    let capture_path = std::path::Path::new(&args.capture_path);
    if !capture_path.exists() {
        error!("Capture file {} does not exist", &args.capture_path);
        return Err(Error::InvalidArgs);
    }

    let mut file = tokio::fs::File::open(capture_path).await?;

    let mut contents = String::with_capacity(file.metadata().await?.len() as usize);

    file.read_to_string(&mut contents).await?;

    let lines: Vec<Line> = serde_json::Deserializer::from_str(&contents)
        .into_iter::<Line>()
        .map(|line| line.unwrap())
        .collect();

    if args.list_names {
        let mut names: Vec<String> = lines.iter().map(|line| line.metric_name.clone()).collect();
        names.sort();
        names.dedup();
        for name in names {
            println!("{}", name);
        }
        return Ok(());
    }
    if let Some(metric) = args.metric {
        concatenate!(
            Estimator,
            [Variance, variance, mean, error],
            [Quantile, quantile, quantile]
        );

        let s: Estimator = lines
            .iter()
            .filter(|line| line.metric_name == metric)
            .map(|line| line.value.as_f64())
            .collect();

        info!(
            "For metric: {}, mean: {} (+- {}), median: {}",
            metric,
            s.mean(),
            s.error(),
            s.quantile()
        );

        let mut key_and_values: HashMap<String, HashSet<String>> = HashMap::new();
        // TODO consolidate these loops into two, one to compute, one to print
        let labels: Vec<FxHashMap<String, String>> = lines
            .iter()
            .filter(|line| line.metric_name == metric)
            .map(|line| line.labels.clone())
            .collect();
        for label in labels.iter() {
            for (key, value) in label.iter() {
                key_and_values
                    .entry(key.clone())
                    .or_default()
                    .insert(value.clone());
            }
        }
        info!("Labels for metric: {}", metric);
        for (key, values) in key_and_values.iter_mut() {
            println!("{}: {:?}", key, values);
        }
    }

    info!("Bye. :)");
    Ok(())
}
