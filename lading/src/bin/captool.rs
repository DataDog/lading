use std::collections::{hash_map::RandomState, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::hash::BuildHasher;
use std::hash::Hasher;

use async_compression::tokio::bufread::ZstdDecoder;
use average::{concatenate, Estimate, Max, Min, Variance};
use clap::Parser;
use futures::io;
use lading_capture::json::{Line, MetricKind};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;
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

    /// dump a metric's values. Requires --metric.
    #[clap(short, long)]
    dump_values: bool,

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

    let file = tokio::fs::File::open(capture_path).await?;

    let lines = if !is_zstd {
        let reader = BufReader::new(file);
        tokio_util::either::Either::Left(LinesStream::new(reader.lines()))
    } else {
        let reader = BufReader::new(file);
        let decoder = ZstdDecoder::new(reader);
        let reader = BufReader::new(decoder);
        tokio_util::either::Either::Right(LinesStream::new(reader.lines()))
    };

    let lines = lines.map(|l| {
        let line_str = l.expect("failed to read line");
        let line: Line = serde_json::from_str(&line_str).expect("failed to deserialize line");
        line
    });

    // Print out available metrics if user asked for it
    // or if they didn't specify a specific metric
    if args.list_metrics || args.metric.is_none() {
        let mut names: Vec<(String, String)> = lines.map(|line| (line.metric_name.clone(), match line.metric_kind { MetricKind::Counter => "c".to_owned(), MetricKind::Gauge => "g".to_owned() } )).collect().await;
        names.sort();
        names.dedup();
        for (name, kind) in names {
            println!("{kind}: {name}");
        }
        return Ok(());
    }

    if let Some(metric) = &args.metric {
        // key is hash of the metric name and all the sorted, concatenated labels
        // value is a tuple of
        // - hashset of "key:value" strings (aka, concatenated labels)
        // - vec of data points
        let mut context_map: HashMap<u64, (BTreeSet<String>, Vec<f64>)> = HashMap::new();
        let hash_builder = RandomState::new();
        info!("Metric: {metric}");

        // Use a BTreeSet to ensure that the tags are sorted
        let filtered: Vec<_> = lines
            .filter(|line| &line.metric_name == metric)
            .collect()
            .await;

        if let Some(point) = filtered.first() {
            info!("Metric kind: {:?}", point.metric_kind);
        } else {
            error!("No data found for metric {}", metric);
        }

        for line in &filtered {
            let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
            for (key, value) in line.labels.iter() {
                let tag = format!("{key}:{value}");
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
        }

        concatenate!(Estimator, [Variance, mean], [Max, max], [Min, min]);
        let is_monotonic = |v: &Vec<_>| v.windows(2).all(|w| w[0] <= w[1]);

        for (_, (labels, values)) in context_map.iter() {
            let s: Estimator = values.iter().copied().collect();
            info!(
                "{metric}[{labels}]: min: {}, mean: {}, max: {}, is_monotonic: {}",
                s.min(),
                s.mean(),
                s.max(),
                is_monotonic(values),
                labels = labels.iter().cloned().collect::<Vec<String>>().join(",")
            );
        }

        if args.dump_values {
            for line in &filtered {
                println!("{}: {}", line.fetch_index, line.value);
            }
        }
    }

    info!("Bye. :)");
    Ok(())
}
