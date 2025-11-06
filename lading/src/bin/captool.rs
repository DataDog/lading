//! Capture analysis tool for lading capture files.

#![allow(clippy::print_stdout)]
use std::collections::{BTreeSet, HashMap, hash_map::RandomState};
use std::ffi::OsStr;
use std::hash::{BuildHasher, Hasher};
use std::path;

use async_compression::tokio::bufread::ZstdDecoder;
use average::{Estimate, Max, Min, Variance, concatenate};
use clap::Parser;
use futures::io;
use lading_capture::line::{Line, MetricKind};
use tokio::{
    fs,
    io::{AsyncBufReadExt, BufReader},
};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::LinesStream;
use tokio_util::either;
use tracing::{error, info};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Option<Command>,

    /// list metric names
    #[clap(short, long)]
    list_metrics: bool,

    /// show details about a metric
    #[clap(short, long)]
    metric: Option<String>,

    /// dump a metric's values. Requires --metric.
    #[clap(short, long)]
    dump_values: bool,

    /// Path to line-delimited capture file (not used with subcommands)
    capture_path: Option<String>,
}

#[derive(Parser, Debug)]
enum Command {
    /// Validate that a capture file is well-formed
    Validate {
        /// Path to line-delimited capture file
        capture_path: String,
        /// Minimum number of seconds (unique timestamps) that must be present in the capture file
        #[clap(long)]
        min_seconds: Option<u64>,
    },
}

/// Errors that can occur while running captool.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Invalid command line arguments provided.
    #[error("Invalid arguments specified")]
    InvalidArgs,
    /// I/O operation failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// JSON deserialization failed.
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
}

#[tokio::main(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::FULL)
        .with_ansi(false)
        .finish()
        .init();

    info!("Welcome to captool");
    let args = Args::parse();

    // Handle validate subcommand
    if let Some(Command::Validate {
        capture_path,
        min_seconds,
    }) = args.command
    {
        return validate_capture(&capture_path, min_seconds).await;
    }

    // For non-subcommand usage, capture_path is required
    let Some(capture_path_str) = &args.capture_path else {
        error!("capture_path is required (or use 'validate' subcommand)");
        return Err(Error::InvalidArgs);
    };
    let capture_path = path::Path::new(capture_path_str);
    if !capture_path.exists() {
        error!("Capture file {capture_path_str} does not exist");
        return Err(Error::InvalidArgs);
    }

    let is_zstd = capture_path
        .extension()
        .is_some_and(|ext| ext == OsStr::new("zstd"));

    let file = fs::File::open(capture_path).await?;

    let lines = if is_zstd {
        let reader = BufReader::new(file);
        let decoder = ZstdDecoder::new(reader);
        let reader = BufReader::new(decoder);
        either::Either::Right(LinesStream::new(reader.lines()))
    } else {
        let reader = BufReader::new(file);
        either::Either::Left(LinesStream::new(reader.lines()))
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
            for (key, value) in &line.labels {
                let tag = format!("{key}:{value}");
                sorted_labels.insert(tag);
            }
            let mut context_key = hash_builder.build_hasher();
            context_key.write_usize(metric.len());
            context_key.write(metric.as_bytes());
            for label in &sorted_labels {
                context_key.write_usize(label.len());
                context_key.write(label.as_bytes());
            }
            let entry = context_map.entry(context_key.finish()).or_default();
            entry.0 = sorted_labels;
            entry.1.push(line.value.as_f64());
        }

        concatenate!(Estimator, [Variance, mean], [Max, max], [Min, min]);
        let is_monotonic = |v: &Vec<_>| v.windows(2).all(|w| w[0] <= w[1]);

        for (labels, values) in context_map.values() {
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

async fn validate_capture(capture_path_str: &str, min_seconds: Option<u64>) -> Result<(), Error> {
    let capture_path = path::Path::new(capture_path_str);
    if !capture_path.exists() {
        error!("Capture file {capture_path_str} does not exist");
        return Err(Error::InvalidArgs);
    }

    let is_zstd = capture_path
        .extension()
        .is_some_and(|ext| ext == OsStr::new("zstd"));

    let file = fs::File::open(capture_path).await?;

    let lines = if is_zstd {
        let reader = BufReader::new(file);
        let decoder = ZstdDecoder::new(reader);
        let reader = BufReader::new(decoder);
        either::Either::Right(LinesStream::new(reader.lines()))
    } else {
        let reader = BufReader::new(file);
        either::Either::Left(LinesStream::new(reader.lines()))
    };

    let mut lines_vec = Vec::new();
    let mut lines_stream = lines.map(|l| {
        let line_str = l.expect("failed to read line");
        let line: Line = serde_json::from_str(&line_str).expect("failed to deserialize line");
        line
    });

    while let Some(line) = lines_stream.next().await {
        lines_vec.push(line);
    }

    // Use the canonical validation logic from lading_capture::validate
    let result = lading_capture::validate::validate_lines(&lines_vec, min_seconds);

    info!(
        "Validated {line_count} lines",
        line_count = result.line_count
    );
    info!(
        "  Unique series: {series_count}",
        series_count = result.unique_series
    );
    info!(
        "  Unique fetch_index values: {fetch_count}",
        fetch_count = result.unique_fetch_indices
    );

    if !result.is_valid() {
        let (line, category, msg) = result
            .first_error
            .as_ref()
            .expect("first_error must be set when invalid");

        if result.fetch_index_errors > 0 {
            error!(
                "Found {errors} fetch_index/time mapping violations",
                errors = result.fetch_index_errors
            );
            error!("First violation at line {line}: {category}");
            error!("  {msg}");
            error!("Each fetch_index MUST map to exactly one time value");
        } else if result.per_series_errors > 0 {
            error!(
                "Found {errors} per-series violations",
                errors = result.per_series_errors
            );
            error!("First violation at line {line}: {category}");
            error!("  {msg}");
            error!("Each (metric+labels) MUST have strictly increasing time and fetch_index");
        } else if result.min_seconds_errors > 0 {
            error!("Minimum seconds requirement failed");
            error!("  {msg}");
        }

        return Err(Error::InvalidArgs);
    }

    info!("All invariants satisfied:");
    info!("  - Each fetch_index maps to exactly one time");
    info!("  - Each series has strictly increasing time");
    info!("  - Each series has strictly increasing fetch_index");
    if let Some(min_secs) = min_seconds {
        info!("  - Contains at least {min_secs} unique timestamps");
    }
    info!("Capture file is valid");
    Ok(())
}
