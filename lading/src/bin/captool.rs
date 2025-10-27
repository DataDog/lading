//! Capture analysis tool for lading capture files.

#![allow(clippy::print_stdout)]

use std::collections::{BTreeSet, HashMap, hash_map::RandomState};
use std::ffi::OsStr;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::path;

use async_compression::tokio::bufread::ZstdDecoder;
use average::{Estimate, Max, Min, Variance, concatenate};
use clap::Parser;
use futures::io;
use lading_capture::json::{Line, MetricKind};
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
    if let Some(Command::Validate { capture_path }) = args.command {
        return validate_capture(&capture_path).await;
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

async fn validate_capture(capture_path_str: &str) -> Result<(), Error> {
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

    let mut lines = lines.map(|l| {
        let line_str = l.expect("failed to read line");
        let line: Line = serde_json::from_str(&line_str).expect("failed to deserialize line");
        line
    });

    // Track fetch_index -> time mapping (global invariant)
    let mut fetch_index_to_time: HashMap<u64, u128> = HashMap::new();

    // Track last seen (time, fetch_index) for each (metric_name, labels) combination
    let mut series_last_state: HashMap<u64, (u128, u64, String)> = HashMap::new();
    let hash_builder = std::collections::hash_map::RandomState::new();
    let mut line_num = 0u128;
    let mut error_count = 0u128;
    let mut fetch_index_errors = 0u128;
    let mut first_error: Option<(u128, String, String)> = None;

    while let Some(line) = lines.next().await {
        line_num += 1;
        let time = line.time;
        let fetch_index = line.fetch_index;

        // Check global invariant: each fetch_index maps to exactly one time
        if let Some(&existing_time) = fetch_index_to_time.get(&fetch_index) {
            if existing_time != time {
                if fetch_index_errors == 0 {
                    let msg = format!(
                        "fetch_index {fetch_index} appears with multiple times: {existing_time} and {time}"
                    );
                    error!("INVARIANT VIOLATION at line {line_num}");
                    error!("  {msg}");
                    first_error = Some((line_num, "fetch_index/time mismatch".to_string(), msg));
                }
                fetch_index_errors += 1;
            }
        } else {
            fetch_index_to_time.insert(fetch_index, time);
        }

        // Build a unique key for (metric_name, labels)
        let mut sorted_labels: BTreeSet<String> = BTreeSet::new();
        for (key, value) in &line.labels {
            sorted_labels.insert(format!("{key}:{value}"));
        }

        let mut hasher = hash_builder.build_hasher();
        hasher.write_usize(line.metric_name.len());
        hasher.write(line.metric_name.as_bytes());
        for label in &sorted_labels {
            hasher.write_usize(label.len());
            hasher.write(label.as_bytes());
        }
        let series_key = hasher.finish();

        let series_id = format!(
            "{metric}[{labels}]",
            metric = line.metric_name,
            labels = sorted_labels.iter().cloned().collect::<Vec<String>>().join(",")
        );

        // Check per-series invariants
        if let Some((prev_time, prev_fetch_index, _)) = series_last_state.get(&series_key) {
            if time <= *prev_time {
                if error_count == 0 {
                    let msg = format!(
                        "time not strictly increasing: prev={prev_time}, curr={time}"
                    );
                    error!("Series {series_id} at line {line_num}");
                    error!("  {msg}");
                    first_error = Some((line_num, series_id.clone(), msg));
                }
                error_count += 1;
            }
            if fetch_index <= *prev_fetch_index {
                if error_count == 0 {
                    let msg = format!(
                        "fetch_index not strictly increasing: prev={prev_fetch_index}, curr={fetch_index}"
                    );
                    error!("Series {series_id} at line {line_num}");
                    error!("  {msg}");
                    first_error = Some((line_num, series_id.clone(), msg));
                }
                error_count += 1;
            }
        }

        series_last_state.insert(series_key, (time, fetch_index, series_id));
    }

    info!("Validated {line_num} lines");
    info!("  Unique series: {series_count}", series_count = series_last_state.len());
    info!("  Unique fetch_index values: {fetch_count}", fetch_count = fetch_index_to_time.len());

    if fetch_index_errors > 0 {
        let (line, category, msg) = first_error.as_ref().unwrap();
        error!("Found {fetch_index_errors} fetch_index/time mapping violations");
        error!("First violation at line {line}: {category}");
        error!("  {msg}");
        error!("Each fetch_index MUST map to exactly one time value");
        return Err(Error::InvalidArgs);
    }

    if error_count > 0 {
        let (line, series, msg) = first_error.as_ref().unwrap();
        error!("Found {error_count} per-series violations");
        error!("First violation at line {line}: {series}");
        error!("  {msg}");
        error!("Each (metric+labels) MUST have strictly increasing time and fetch_index");
        return Err(Error::InvalidArgs);
    }

    info!("✓ All invariants satisfied:");
    info!("  - Each fetch_index maps to exactly one time");
    info!("  - Each series has strictly increasing time");
    info!("  - Each series has strictly increasing fetch_index");
    info!("Capture file is valid");
    Ok(())
}
