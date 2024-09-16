use std::collections::{hash_map::RandomState, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::hash::BuildHasher;
use std::hash::Hasher;

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
    list_metrics: bool,

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

    let is_zstd = capture_path
        .extension()
        .is_some_and(|ext| ext == OsStr::new("zstd"));

    let mut file = tokio::fs::File::open(capture_path).await?;

    let contents = if !is_zstd {
        let mut contents = String::with_capacity(file.metadata().await?.len() as usize);
        file.read_to_string(&mut contents).await?;

        contents
    } else {
        let stdfile = file.try_into_std().expect("file can be std");
        let contents = zstd::stream::decode_all(stdfile).expect("invalid zstd");

        String::from_utf8(contents).expect("Valid utf8")
    };

    let lines: Vec<Line> = serde_json::Deserializer::from_str(&contents)
        .into_iter::<Line>()
        .map(|line| line.expect("failed to deserialize line"))
        .collect();

    // Print out available metrics if user asked for it
    // or if they didn't specify a specific metric
    if args.list_metrics || args.metric.is_none() {
        let mut names: Vec<String> = lines.iter().map(|line| line.metric_name.clone()).collect();
        names.sort();
        names.dedup();
        for name in names {
            println!("{}", name);
        }
        return Ok(());
    }

    if let Some(metric) = args.metric {
        // key is hash of the metric name and all the sorted, concatenated labels
        // value is a tuple of
        // - hashset of "key:value" strings (aka, concatenated labels)
        // - vec of data points
        let mut context_map: HashMap<u64, (BTreeSet<String>, Vec<f64>)> = HashMap::new();
        let hash_builder = RandomState::new();
        info!("Metric: {metric}");

        // Use a BTreeSet to ensure that the tags are sorted
        lines
            .iter()
            .filter(|line| line.metric_name == metric)
            .for_each(|line| {
                let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
                for (key, value) in line.labels.iter() {
                    let tag = format!("{}:{}", key, value);
                    sorted_labels.insert(tag);
                }
                let mut context_key = hash_builder.build_hasher();
                context_key.write_usize(metric.len());
                context_key.write(metric.as_bytes());
                for label in sorted_labels.iter() {
                    context_key.write_usize(label.len());
                    context_key.write(label.as_bytes());
                }
                let entry = context_map.entry(context_key.finish()).or_default();
                entry.0 = sorted_labels;
                entry.1.push(line.value.as_f64());
            });

        concatenate!(Estimator, [Variance, variance, mean]);

        for (_, (labels, values)) in context_map.iter() {
            let s: Estimator = values.iter().copied().collect();
            info!(
                "{metric}[{labels}]: mean: {}",
                s.mean(),
                labels = labels.iter().cloned().collect::<Vec<String>>().join(",")
            );
        }
    }

    info!("Bye. :)");
    Ok(())
}
