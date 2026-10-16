//! Adaptive sampling scenario.
//!
//! Tests the agent's experimental adaptive sampling feature — a per-pattern
//! credit-based rate limiter. Uses action sequences (write, sleep, write, ...)
//! of variable length to stress-test the credit model.

use std::time::Duration;

use proptest::prelude::*;

use crate::config::LogSourceConfig;
use crate::log_format::PROPTEST_MARKER;
use crate::log_gen::LogLine;
use crate::orchestrator::Action;
use crate::property;

/// Fixed adaptive sampling parameters for testing.
const BURST_SIZE: usize = 10;
/// Credits per second.
const RATE_LIMIT: f64 = 2.0;
/// Tolerance for count assertions. 0 = exact model matching.
const TOLERANCE: usize = 0;

/// A single step in a generated action sequence.
#[derive(Debug, Clone, Copy)]
pub enum Step {
    /// Write N lines.
    Write(usize),
    /// Sleep N seconds.
    Sleep(u64),
}

/// Parameters for adaptive sampling testing — a variable-length
/// sequence of write and sleep steps.
#[derive(Debug, Clone)]
pub struct AdaptiveSamplingParams {
    /// The steps to execute.
    pub steps: Vec<Step>,
}

/// Strategy for a single step.
fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        (5_usize..25).prop_map(Step::Write),
        (2_u64..8).prop_map(Step::Sleep),
    ]
}

/// Proptest strategy for adaptive sampling parameters.
///
/// Generates a variable-length sequence of write and sleep steps.
/// Always starts with a write (to exercise the burst) and ensures
/// at least one sleep exists (to exercise credit refill).
pub fn strategy() -> BoxedStrategy<AdaptiveSamplingParams> {
    // Generate 2-6 additional steps after the mandatory write+sleep+write
    (
        15_usize..25,   // initial write count
        2_u64..8,       // first sleep seconds
        5_usize..15,    // second write count
        proptest::collection::vec(step_strategy(), 0..4), // extra steps
    )
        .prop_map(|(first_write, first_sleep, second_write, extra)| {
            let mut steps = vec![
                Step::Write(first_write),
                Step::Sleep(first_sleep),
                Step::Write(second_write),
            ];
            steps.extend(extra);
            AdaptiveSamplingParams { steps }
        })
        .boxed()
}

/// Counter for generating unique numeric IDs across all calls within a case.
static LINE_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Generate N lines that all share the same structural pattern.
///
/// Uses a zero-padded numeric ID instead of UUIDs so the agent's tokenizer
/// produces identical token sequences for every line.
#[must_use]
pub fn generate_same_pattern_lines(count: usize) -> Vec<LogLine> {
    (0..count)
        .map(|_| {
            let n = LINE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let id = format!("{n:010}");
            let content = format!("[{PROPTEST_MARKER}:{id}] adaptive sampling test message");
            LogLine { id, content }
        })
        .collect()
}

/// Result of building an action sequence.
#[derive(Debug)]
pub struct ActionSequenceResult {
    /// The orchestrator actions to execute.
    pub actions: Vec<Action>,
    /// The model actions for property checking (with IDs).
    pub sampling_actions: Vec<property::SamplingAction>,
}

/// Build the action sequence from params.
#[must_use]
pub fn build_actions(params: &AdaptiveSamplingParams) -> ActionSequenceResult {
    LINE_COUNTER.store(0, std::sync::atomic::Ordering::Relaxed);

    let mut actions = Vec::new();
    let mut sampling_actions = Vec::new();

    for step in &params.steps {
        match step {
            Step::Write(count) => {
                let lines = generate_same_pattern_lines(*count);
                let ids: Vec<String> = lines.iter().map(|l| l.id.clone()).collect();
                sampling_actions.push(property::SamplingAction::Write(ids));
                actions.push(Action::WriteLines(lines));
            }
            Step::Sleep(seconds) => {
                sampling_actions.push(property::SamplingAction::Sleep(*seconds));
                actions.push(Action::Sleep(Duration::from_secs(*seconds)));
            }
        }
    }

    ActionSequenceResult {
        actions,
        sampling_actions,
    }
}

/// Build the properties to assert.
#[must_use]
pub fn build_properties(
    result: &ActionSequenceResult,
) -> Vec<Box<dyn crate::property::Property>> {
    #[expect(clippy::cast_precision_loss)]
    let burst_size = BURST_SIZE as f64;
    vec![Box::new(property::AdaptiveSamplingModel {
        actions: result.sampling_actions.clone(),
        burst_size,
        rate_limit: RATE_LIMIT,
        tolerance: TOLERANCE,
    })]
}

/// The log source configuration for adaptive sampling.
#[must_use]
pub fn log_source_config() -> LogSourceConfig {
    #[expect(clippy::cast_precision_loss)]
    LogSourceConfig::AdaptiveSampling {
        burst_size: BURST_SIZE as f64,
        rate_limit: RATE_LIMIT,
    }
}
