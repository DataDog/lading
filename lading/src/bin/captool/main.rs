//! Capture analysis tool for lading capture files.

#![allow(clippy::print_stdout)]

mod analyze;

use std::path;

use async_compression::tokio::bufread::ZstdDecoder;
use clap::Parser;
use futures::io;
use lading_capture::format::{CaptureFormat, detect_format};
use lading_capture::line::Line;
use lading_capture::validate::{jsonl, parquet};
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

    let format = detect_format(capture_path).map_err(|e| {
        error!("Failed to detect capture format: {e}");
        Error::InvalidArgs
    })?;

    // List metrics
    if args.list_metrics || args.metric.is_none() {
        let metrics = match format {
            CaptureFormat::Parquet => {
                let path_str = capture_path_str.to_owned();
                tokio::task::spawn_blocking(move || analyze::parquet::list_metrics(&path_str))
                    .await
                    .expect("list_metrics task panicked")
                    .map_err(|e| {
                        error!("Failed to list metrics: {e}");
                        Error::InvalidArgs
                    })?
            }
            CaptureFormat::Jsonl { compressed } => {
                let file = fs::File::open(capture_path).await?;
                let lines = if compressed {
                    let reader = BufReader::new(file);
                    let decoder = ZstdDecoder::new(reader);
                    let reader = BufReader::new(decoder);
                    either::Either::Right(LinesStream::new(reader.lines()))
                } else {
                    let reader = BufReader::new(file);
                    either::Either::Left(LinesStream::new(reader.lines()))
                };

                let lines_vec: Vec<Line> = lines
                    .map(|l| {
                        let line_str = l.expect("failed to read line");
                        serde_json::from_str(&line_str).expect("failed to deserialize line")
                    })
                    .collect()
                    .await;

                analyze::jsonl::list_metrics(&lines_vec)
            }
        };

        for metric in metrics {
            let kind_char = match metric.kind.as_str() {
                "counter" => "c",
                "gauge" => "g",
                _ => "?",
            };
            println!("{kind_char}: {}", metric.name);
        }
        return Ok(());
    }

    // Analyze specific metric
    if let Some(metric_name) = &args.metric {
        info!("Metric: {metric_name}");

        let series_stats = match format {
            CaptureFormat::Parquet => {
                let path_str = capture_path_str.to_owned();
                let metric_clone = metric_name.clone();
                tokio::task::spawn_blocking(move || {
                    analyze::parquet::analyze_metric(&path_str, &metric_clone)
                })
                .await
                .expect("analyze_metric task panicked")
                .map_err(|e| {
                    error!("Failed to analyze metric: {e}");
                    Error::InvalidArgs
                })?
            }
            CaptureFormat::Jsonl { compressed } => {
                let file = fs::File::open(capture_path).await?;
                let lines = if compressed {
                    let reader = BufReader::new(file);
                    let decoder = ZstdDecoder::new(reader);
                    let reader = BufReader::new(decoder);
                    either::Either::Right(LinesStream::new(reader.lines()))
                } else {
                    let reader = BufReader::new(file);
                    either::Either::Left(LinesStream::new(reader.lines()))
                };

                let lines_vec: Vec<Line> = lines
                    .map(|l| {
                        let line_str = l.expect("failed to read line");
                        serde_json::from_str(&line_str).expect("failed to deserialize line")
                    })
                    .collect()
                    .await;

                analyze::jsonl::analyze_metric(&lines_vec, metric_name)
            }
        };

        if series_stats.is_empty() {
            error!("No data found for metric {metric_name}");
        } else {
            for stats in series_stats.values() {
                let labels_str = stats
                    .labels
                    .iter()
                    .cloned()
                    .collect::<Vec<String>>()
                    .join(",");
                info!(
                    "{metric_name}[{labels_str}]: min: {}, mean: {}, max: {}, is_monotonic: {}",
                    stats.min, stats.mean, stats.max, stats.is_monotonic
                );
            }

            if args.dump_values {
                for stats in series_stats.values() {
                    for (fetch_index, value) in &stats.values {
                        println!("{fetch_index}: {value}");
                    }
                }
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

    let format = detect_format(capture_path).map_err(|e| {
        error!("Failed to detect capture format: {e}");
        Error::InvalidArgs
    })?;

    let result = match format {
        CaptureFormat::Parquet => {
            info!("Detected parquet format, using columnar validation");
            let path_str = capture_path_str.to_owned();
            tokio::task::spawn_blocking(move || parquet::validate_parquet(&path_str, min_seconds))
                .await
                .expect("validation task panicked")
                .map_err(|e| {
                    error!("Parquet validation failed: {e}");
                    Error::InvalidArgs
                })?
        }
        CaptureFormat::Jsonl { compressed } => {
            let file = fs::File::open(capture_path).await?;

            let lines = if compressed {
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
                let line: Line =
                    serde_json::from_str(&line_str).expect("failed to deserialize line");
                line
            });

            while let Some(line) = lines_stream.next().await {
                lines_vec.push(line);
            }

            jsonl::validate_lines(&lines_vec, min_seconds)
        }
    };

    report_validation_result(&result, min_seconds)
}

fn report_validation_result(
    result: &lading_capture::validate::ValidationResult,
    min_seconds: Option<u64>,
) -> Result<(), Error> {
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
