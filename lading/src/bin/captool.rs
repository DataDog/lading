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

    /// path to line-delimited capture file
    capture_path: String,

    /// path to a second capture file for comparison
    #[clap(short, long)]
    comparison_capture_path: Option<String>,

    /// warmup period in seconds - data points before this time will be ignored
    #[clap(long)]
    warmup: Option<u64>,
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

concatenate!(Estimator, [Variance, mean], [Max, max], [Min, min]);

async fn process_capture_path(
    args: &Args,
    path: &str,
) -> Result<Option<Vec<(String, Estimator)>>, Error> {
    let capture_path = std::path::Path::new(&path);
    if !capture_path.exists() {
        error!("Capture file {path} does not exist");
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
        let mut names: Vec<(String, String)> = lines
            .map(|line| {
                (
                    line.metric_name.clone(),
                    match line.metric_kind {
                        MetricKind::Counter => "c".to_owned(),
                        MetricKind::Gauge => "g".to_owned(),
                    },
                )
            })
            .collect()
            .await;
        names.sort();
        names.dedup();
        for (name, kind) in names {
            println!("{kind}: {name}");
        }
        return Ok(None);
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
            .filter(|line| {
                if let Some(warmup) = args.warmup {
                    line.fetch_index >= warmup
                } else {
                    true
                }
            })
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

        // let is_monotonic = |v: &Vec<_>| v.windows(2).all(|w| w[0] <= w[1]);

        let mut estimators = vec![];

        for (_, (labels, values)) in context_map.iter() {
            let s: Estimator = values.iter().copied().collect();
            // println!(
            //     "{metric}[{labels}]: min: {}, mean: {}, max: {}, is_monotonic: {}",
            //     s.min(),
            //     s.mean(),
            //     s.max(),
            //     is_monotonic(values),
            //     labels = labels.iter().cloned().collect::<Vec<String>>().join(",")
            // );

            let mut context = labels
                .iter()
                .filter(|v| !v.starts_with("replicate:") && !v.starts_with("pid:"))
                .cloned()
                .collect::<Vec<_>>();
            context.sort();
            let context = context.join(",");

            estimators.push((context, s));
        }

        if args.dump_values {
            for line in &filtered {
                println!("{}: {}", line.fetch_index, line.value);
            }
        }

        return Ok(Some(estimators));
    }

    Ok(None)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::FULL)
        .with_ansi(false)
        .finish()
        .init();

    info!("Welcome to captool");
    let args = Args::parse();

    let cap1 = process_capture_path(&args, &args.capture_path).await?;

    if let Some(comparison_capture_path) = &args.comparison_capture_path {
        let metric = args.metric.clone().unwrap();

        let cap1 = cap1.unwrap();
        let cap2 = process_capture_path(&args, comparison_capture_path)
            .await?
            .unwrap();

        let all_contexts = cap1
            .iter()
            .map(|(labels, _)| labels.clone())
            .chain(cap2.iter().map(|(labels, _)| labels.clone()))
            .collect::<BTreeSet<_>>();

        for label in &all_contexts {
            let cap1_lookup = &cap1.iter().find(|(labels, _)| labels.contains(label));
            let cap2_lookup = &cap2.iter().find(|(labels, _)| labels.contains(label));

            if cap1_lookup.is_none() {
                println!("{} only present in cap2", label);
                continue;
            }
            if cap2_lookup.is_none() {
                println!("{} only present in cap1", label);
                continue;
            }

            let cap1_est = &cap1_lookup.unwrap().1;
            let cap2_est = &cap2_lookup.unwrap().1;

            let min = cap1_est.min() - cap2_est.min();
            let mean = cap1_est.mean() - cap2_est.mean();
            let max = cap1_est.max() - cap2_est.max();

            if min.abs() > 0.01 || mean.abs() > 0.01 || max.abs() > 0.01 {
                println!("{metric}[{label}]");
                println!(
                    "\tA: min: {}, mean: {}, max: {}",
                    cap1_est.min(),
                    cap1_est.mean(),
                    cap1_est.max(),
                );
                println!(
                    "\tB: min: {}, mean: {}, max: {}",
                    cap2_est.min(),
                    cap2_est.mean(),
                    cap2_est.max(),
                );
                println!("\tDiff min: {min}, mean: {mean}, max: {max}");
            }
        }
    }

    info!("Bye. :)");
    Ok(())
}
