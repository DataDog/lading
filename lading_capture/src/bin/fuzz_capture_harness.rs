//! AFL++ external mode fuzzer for `lading_capture`
//!
//! This binary is designed to be run by AFL++ in external mode. It reads
//! arbitrary bytes from stdin, deserializes into a fuzz input configuration,
//! runs `CaptureManager` for the specified duration, and validates the the
//! capture file maintains all essential properties.

#![allow(clippy::print_stderr)]
#![allow(clippy::cast_precision_loss)]

use anyhow::{Context, Result};
use arbitrary::Arbitrary;
use lading_capture::{
    formats::jsonl,
    line::Line,
    manager::{CaptureManager, SignalWatchers, DEFAULT_TICK_DURATION_MS},
    test::writer::InMemoryWriter,
    validate,
};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use std::io::{self, Read};
use std::process::ExitCode;
use std::time::{Duration, Instant};

const MAX_RUNTIME_SECS: u16 = 150;

/// Simple string pool for generating random metric names and label values,
/// taken from the payloads, should be shared common.
struct StrPool {
    inner: String,
}

impl StrPool {
    fn new<R: Rng>(rng: &mut R, bytes: usize) -> Self {
        const ALPHANUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_";
        let mut inner = String::with_capacity(bytes);
        for _ in 0..bytes {
            let idx = rng.random_range(0..ALPHANUM.len());
            inner.push(ALPHANUM[idx] as char);
        }
        Self { inner }
    }

    fn of_size<R: Rng>(&self, rng: &mut R, bytes: usize) -> &str {
        if bytes >= self.inner.len() {
            return &self.inner[..];
        }
        let max_lower = self.inner.len() - bytes;
        let lower = rng.random_range(0..max_lower);
        &self.inner[lower..lower + bytes]
    }
}

#[derive(Debug, Copy, Clone)]
enum Op {
    CounterIncr {
        name_idx: usize,
        value: u64,
    },
    GaugeSet {
        name_idx: usize,
        value: f64,
    },
    HistoricalCounterIncr {
        name_idx: usize,
        label_idx: usize,
        value: u64,
        age_secs: u64,
    },
    HistoricalCounterAbs {
        name_idx: usize,
        label_idx: usize,
        value: u64,
        age_secs: u64,
    },
    HistoricalGaugeIncr {
        name_idx: usize,
        label_idx: usize,
        value: f64,
        age_secs: u64,
    },
    HistoricalGaugeDec {
        name_idx: usize,
        label_idx: usize,
        value: f64,
        age_secs: u64,
    },
    HistoricalGaugeSet {
        name_idx: usize,
        label_idx: usize,
        value: f64,
        age_secs: u64,
    },
    AdvanceTime {
        millis: u64,
    },
}

struct OpIterator {
    rng: SmallRng,
    label_pool: Vec<Vec<(String, String)>>,
}

impl OpIterator {
    fn new(seed: u64) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed);
        let str_pool = StrPool::new(&mut rng, 10_000);

        // Pre-generate pool of label sets (each set is a Vec of key/value pairs)
        let label_pool: Vec<Vec<(String, String)>> = (0..100)
            .map(|_| {
                let num_labels = rng.random_range(0..5);
                (0..num_labels)
                    .map(|_| {
                        let key_len = rng.random_range(3..10);
                        let key = str_pool.of_size(&mut rng, key_len).to_string();
                        let val_len = rng.random_range(3..15);
                        let value = str_pool.of_size(&mut rng, val_len).to_string();
                        (key, value)
                    })
                    .collect()
            })
            .collect();

        Self { rng, label_pool }
    }

    fn get_labels(&self, idx: usize) -> &[(String, String)] {
        &self.label_pool[idx % self.label_pool.len()]
    }
}

impl Iterator for OpIterator {
    type Item = Op;

    fn next(&mut self) -> Option<Self::Item> {
        let op_type = self.rng.random_range(0..8);

        match op_type {
            0 => {
                let name_idx = self.rng.random_range(0..50);
                let value = self.rng.random_range(1..100);
                Some(Op::CounterIncr { name_idx, value })
            }
            1 => {
                let name_idx = self.rng.random_range(0..50);
                let value = self.rng.random_range(0.0..1000.0);
                Some(Op::GaugeSet { name_idx, value })
            }
            2 => {
                let name_idx = self.rng.random_range(0..50);
                let label_idx = self.rng.random_range(0..self.label_pool.len());
                let value = self.rng.random_range(1..100);
                let age_secs = self.rng.random_range(0..10);
                Some(Op::HistoricalCounterIncr {
                    name_idx,
                    label_idx,
                    value,
                    age_secs,
                })
            }
            3 => {
                let name_idx = self.rng.random_range(0..50);
                let label_idx = self.rng.random_range(0..self.label_pool.len());
                let value = self.rng.random_range(1..1000);
                let age_secs = self.rng.random_range(0..10);
                Some(Op::HistoricalCounterAbs {
                    name_idx,
                    label_idx,
                    value,
                    age_secs,
                })
            }
            4 => {
                let name_idx = self.rng.random_range(0..50);
                let label_idx = self.rng.random_range(0..self.label_pool.len());
                let value = self.rng.random_range(0.0..100.0);
                let age_secs = self.rng.random_range(0..10);
                Some(Op::HistoricalGaugeIncr {
                    name_idx,
                    label_idx,
                    value,
                    age_secs,
                })
            }
            5 => {
                let name_idx = self.rng.random_range(0..50);
                let label_idx = self.rng.random_range(0..self.label_pool.len());
                let value = self.rng.random_range(0.0..100.0);
                let age_secs = self.rng.random_range(0..10);
                Some(Op::HistoricalGaugeDec {
                    name_idx,
                    label_idx,
                    value,
                    age_secs,
                })
            }
            6 => {
                let name_idx = self.rng.random_range(0..50);
                let label_idx = self.rng.random_range(0..self.label_pool.len());
                let value = self.rng.random_range(0.0..1000.0);
                let age_secs = self.rng.random_range(0..10);
                Some(Op::HistoricalGaugeSet {
                    name_idx,
                    label_idx,
                    value,
                    age_secs,
                })
            }
            7 => {
                let millis = self.rng.random_range(10..100);
                Some(Op::AdvanceTime { millis })
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    seed: u64,
    runtime_secs: u16,
}

impl FuzzInput {
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.runtime_secs > 0 && self.runtime_secs <= MAX_RUNTIME_SECS,
            "runtime_secs must be 1-{MAX_RUNTIME_SECS}, got {}",
            self.runtime_secs
        );
        Ok(())
    }
}

fn read_input() -> Result<Vec<u8>> {
    let mut input = Vec::new();
    io::stdin()
        .read_to_end(&mut input)
        .context("Failed to read input from stdin")?;
    Ok(input)
}

#[allow(clippy::too_many_lines)]
async fn execute_op(op: Op, ops: &OpIterator, start: Instant) {
    match op {
        Op::AdvanceTime { millis } => {
            tokio::time::advance(Duration::from_millis(millis)).await;
        }
        Op::CounterIncr { name_idx, value } => {
            let name = format!("c{name_idx}");
            metrics::counter!(name).increment(value);
        }
        Op::GaugeSet { name_idx, value } => {
            let name = format!("g{name_idx}");
            metrics::gauge!(name).set(value);
        }
        Op::HistoricalCounterIncr {
            name_idx,
            label_idx,
            value,
            age_secs,
        } => {
            let name = format!("hc{name_idx}");
            let labels = ops.get_labels(label_idx);
            let now = Instant::now();
            let timestamp = now
                .checked_sub(Duration::from_secs(age_secs))
                .unwrap_or(start);
            let label_refs: Vec<_> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let _ = lading_capture::counter_incr(&name, &label_refs, value, timestamp).await;
        }
        Op::HistoricalCounterAbs {
            name_idx,
            label_idx,
            value,
            age_secs,
        } => {
            let name = format!("hc{name_idx}");
            let labels = ops.get_labels(label_idx);
            let now = Instant::now();
            let timestamp = now
                .checked_sub(Duration::from_secs(age_secs))
                .unwrap_or(start);
            let label_refs: Vec<_> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let _ = lading_capture::counter_absolute(&name, &label_refs, value, timestamp).await;
        }
        Op::HistoricalGaugeIncr {
            name_idx,
            label_idx,
            value,
            age_secs,
        } => {
            let name = format!("hg{name_idx}");
            let labels = ops.get_labels(label_idx);
            let now = Instant::now();
            let timestamp = now
                .checked_sub(Duration::from_secs(age_secs))
                .unwrap_or(start);
            let label_refs: Vec<_> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let _ = lading_capture::gauge_increment(&name, &label_refs, value, timestamp).await;
        }
        Op::HistoricalGaugeDec {
            name_idx,
            label_idx,
            value,
            age_secs,
        } => {
            let name = format!("hg{name_idx}");
            let labels = ops.get_labels(label_idx);
            let now = Instant::now();
            let timestamp = now
                .checked_sub(Duration::from_secs(age_secs))
                .unwrap_or(start);
            let label_refs: Vec<_> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let _ = lading_capture::gauge_decrement(&name, &label_refs, value, timestamp).await;
        }
        Op::HistoricalGaugeSet {
            name_idx,
            label_idx,
            value,
            age_secs,
        } => {
            let name = format!("hg{name_idx}");
            let labels = ops.get_labels(label_idx);
            let now = Instant::now();
            let timestamp = now
                .checked_sub(Duration::from_secs(age_secs))
                .unwrap_or(start);
            let label_refs: Vec<_> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let _ = lading_capture::gauge_set(&name, &label_refs, value, timestamp).await;
        }
    }
}

async fn run_capture_manager(config: &FuzzInput) -> Result<InMemoryWriter> {
    let writer = InMemoryWriter::new();

    // Create signal watchers for CaptureManager
    let (shutdown_watcher, shutdown_broadcast) = lading_signal::signal();
    let (experiment_watcher, experiment_broadcast) = lading_signal::signal();
    let (target_watcher, target_broadcast) = lading_signal::signal();

    let format = jsonl::Format::new(writer.clone());
    let signals = SignalWatchers {
        shutdown: shutdown_watcher,
        experiment_started: experiment_watcher,
        target_running: target_watcher,
    };
    let manager = CaptureManager::new_with_format(
        format,
        1,
        DEFAULT_TICK_DURATION_MS,
        Duration::from_secs(3600),
        signals,
    );

    let start = Instant::now();
    let manager_handle = tokio::spawn(async move { manager.start().await });

    // Add random delays between signals to simulate real-world conditions
    // where experiment and target startup timing varies
    let mut rng = SmallRng::seed_from_u64(config.seed);
    let experiment_delay_ms = rng.random_range(0..60_000);
    let target_delay_ms = rng.random_range(0..60_000);

    tokio::time::advance(Duration::from_millis(experiment_delay_ms)).await;
    experiment_broadcast.signal();

    tokio::time::advance(Duration::from_millis(target_delay_ms)).await;
    target_broadcast.signal();

    let runtime_ms = u64::from(config.runtime_secs) * 1000;
    let mut elapsed_ms = 0u64;

    let mut op_iter = OpIterator::new(config.seed);

    // Rip through operations, tracking total time advanced
    while elapsed_ms < runtime_ms {
        let op = op_iter.next().expect("infinite iterator");
        if let Op::AdvanceTime { millis } = op {
            elapsed_ms += millis;
        }
        execute_op(op, &op_iter, start).await;
    }

    // Shutdown
    shutdown_broadcast.signal();
    manager_handle.await??;

    Ok(writer)
}

fn validate_capture(writer: &InMemoryWriter, min_seconds: u16) -> Result<()> {
    // Parse capture lines from in-memory buffer
    let bytes = writer.get_bytes();
    let content = String::from_utf8_lossy(&bytes);

    let mut lines = Vec::new();
    for line_str in content.lines() {
        if line_str.is_empty() {
            continue;
        }
        let line: Line = serde_json::from_str(line_str).context("Failed to parse JSON line")?;
        lines.push(line);
    }

    // Validate using canonical validation logic with min_seconds check
    let result = validate::jsonl::validate_lines(&lines, Some(u64::from(min_seconds)));

    // Check all invariants
    if !result.is_valid() {
        let error_msg = result.first_error.as_ref().map_or_else(
            || "unknown error".to_string(),
            |(line, series, msg)| format!("line {line}, series {series}: {msg}"),
        );
        anyhow::bail!("Validation failed: {error_msg}");
    }

    Ok(())
}

async fn run_test() -> Result<()> {
    let input_bytes = read_input()?;
    let config = FuzzInput::arbitrary(&mut arbitrary::Unstructured::new(&input_bytes))
        .context("Failed to parse input")?;

    lading_fuzz::debug_input(&config);
    config.validate()?;

    let writer = run_capture_manager(&config).await?;
    validate_capture(&writer, config.runtime_secs)?;

    Ok(())
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> ExitCode {
    match run_test().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e:#}");
            ExitCode::from(1)
        }
    }
}
