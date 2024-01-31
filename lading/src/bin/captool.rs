use std::collections::{HashMap, HashSet};

use average::{concatenate, Estimate, Variance};
use clap::Parser;
use futures::io;
use lading_capture::json::Line;
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
        info!("Metric: {metric}");

        let mut key_and_values: HashMap<String, HashSet<String>> = HashMap::new();
        // TODO consolidate these loops into two, one to compute, one to print
        let labels = lines
            .iter()
            .filter(|line| line.metric_name == metric)
            .map(|line| line.labels.clone());

        for label in labels {
            for (key, value) in label.iter() {
                key_and_values
                    .entry(key.clone())
                    .or_default()
                    .insert(value.clone());
            }
        }
        info!("Labels:");
        for (key, values) in key_and_values.iter_mut() {
            println!("{}: {:?}", key, values);
        }

        concatenate!(Estimator, [Variance, variance, mean]);

        // Note this does not correctly expand all timeseries present for this metric
        // It only filters by one key, not multiple keys.
        for (key, labels) in key_and_values.iter_mut() {
            for label in labels.iter() {
                let s: Estimator = lines
                    .iter()
                    .filter(|line| {
                        line.metric_name == metric
                            && line.labels.contains_key(key)
                            && line.labels.get(key).unwrap() == label
                    })
                    .map(|line| line.value.as_f64())
                    .collect();
                info!("Stats for {metric}[{key}:{label}]: mean: {}", s.mean(),);
            }
        }
    }

    info!("Bye. :)");
    Ok(())
}
