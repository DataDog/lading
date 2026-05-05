//! Deterministic test execution engine.
//!
//! The orchestrator manages the full lifecycle of a single proptest case:
//! start the intake server, configure and launch the agent, feed logs,
//! wait for drain, collect output, and check properties.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

use crate::agent::AgentTarget;
use crate::config::{self, AgentConfigParams, LogSourceConfig};
use crate::intake::{LogIntakeServer, ReceivedLogEntry};
use crate::log_gen::{LogBatch, LogLine};
use crate::property::{Property, PropertyFailure};
use crate::scenario::Scenario;

/// Return a temp directory base that is visible inside Docker VMs.
///
/// Colima/Lima only mount `$HOME` by default. The system temp dir
/// (`/var/folders/` on macOS) is not visible inside the VM, so bind
/// mounts from there fail. We use `$HOME/.lading_proptest_tmp` instead.
fn dirs_or_home() -> PathBuf {
    let base = PathBuf::from(
        std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string()),
    )
    .join(".lading_proptest_tmp");
    std::fs::create_dir_all(&base).expect("failed to create temp base dir");
    base
}

/// Errors from the orchestrator.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Intake server error.
    #[error("intake server error: {0}")]
    Intake(#[from] crate::intake::Error),
    /// Agent error.
    #[error("agent error: {0}")]
    Agent(#[from] crate::agent::Error),
    /// Configuration error.
    #[error("config error: {0}")]
    Config(#[from] crate::config::Error),
}

/// Configuration for the orchestrator.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// How to run the agent.
    pub agent_target: AgentTarget,
    /// Duration to wait after agent readiness before writing logs,
    /// giving the logs pipeline time to initialize and start tailing.
    pub pipeline_warmup: Duration,
    /// Duration to wait after last log sent before collecting output.
    pub drain_timeout: Duration,
    /// Maximum time to wait for agent readiness.
    pub readiness_timeout: Duration,
    /// Whether the agent should use compression.
    pub use_compression: bool,
    /// Agent batch wait time in ms (lower = faster drain).
    pub batch_wait_ms: u64,
    /// Always preserve the temp directory, even on success.
    /// Set via `LADING_KEEP_TEMP=1`.
    pub keep_temp: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            agent_target: AgentTarget::from_env(),
            pipeline_warmup: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(15),
            readiness_timeout: Duration::from_secs(60),
            use_compression: true,
            batch_wait_ms: 1000,
            keep_temp: std::env::var("LADING_KEEP_TEMP").is_ok(),
        }
    }
}

/// Result of a single test case execution.
#[derive(Debug)]
pub struct TestCaseResult {
    /// The input log batch.
    pub input: LogBatch,
    /// The received output entries.
    pub output: Vec<ReceivedLogEntry>,
    /// Results of each property check.
    pub property_results: Vec<Result<(), PropertyFailure>>,
    /// Path to the temp directory (persisted on failure for debugging).
    temp_dir_path: PathBuf,
    /// The temp directory handle (dropped on success to clean up).
    _temp_dir: Option<TempDir>,
}

impl TestCaseResult {
    /// Path to the temp directory containing configs, logs, and agent output.
    #[must_use]
    pub fn temp_dir(&self) -> &Path {
        &self.temp_dir_path
    }

    /// Whether all properties passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.property_results.iter().all(Result::is_ok)
    }
}

/// Run a single proptest case to completion.
///
/// # Errors
///
/// Returns error if any infrastructure step fails. Property failures
/// are returned in the result, not as errors.
///
/// # Panics
///
/// Panics if the log directory path is not valid UTF-8.
///
/// This function:
/// 1. Creates a temp directory for configs and log files
/// 2. Starts the log intake server (ephemeral port)
/// 3. Writes agent configuration
/// 4. Starts the agent
/// 5. Waits for agent readiness
/// 6. Writes log lines to the file
/// 7. Waits for drain (`drain_timeout` after last write)
/// 8. Stops the agent
/// 9. Collects output from the intake server
/// 10. Checks all properties
///
/// # Errors
///
/// Returns error if any infrastructure step fails. Property failures
/// are returned in the result, not as errors.
pub async fn run_case<S: Scenario>(
    config: &OrchestratorConfig,
    params: &S::Params,
) -> Result<TestCaseResult, Error> {
    // 1. Create temp directory under $HOME so it's visible inside Docker VMs
    //    (Colima/Lima only mount $HOME by default, not /var/folders/).
    let temp_base = dirs_or_home();
    let temp_dir = TempDir::with_prefix_in("lading_proptest_", temp_base)?;
    let temp_path = temp_dir.path().to_owned();
    let config_dir = temp_path.join("config");
    let log_dir = temp_path.join("logs");
    std::fs::create_dir_all(&config_dir)?;
    std::fs::create_dir_all(&log_dir)?;

    info!("test case temp dir: {}", temp_path.display());

    // 2. Start intake server
    let intake = LogIntakeServer::start().await?;
    let intake_port = intake.port();
    debug!("intake server on port {intake_port}");

    // 3. Write agent config
    let log_file_path = match &config.agent_target {
        AgentTarget::Container(_) => "/var/log/proptest/proptest.log".to_string(),
        AgentTarget::Binary(_) => {
            log_dir
                .join("proptest.log")
                .to_str()
                .expect("log path must be valid UTF-8")
                .to_string()
        }
    };

    let agent_config_params = AgentConfigParams {
        intake_port,
        log_file_path,
        use_compression: config.use_compression,
        batch_wait_ms: config.batch_wait_ms,
        log_source_config: S::log_source_config(params),
        max_message_size_bytes: S::max_message_size_bytes(params),
    };
    config::write_agent_config(&config_dir, &agent_config_params)?;

    // 4. Start agent
    let agent = config.agent_target.start(&config_dir, &log_dir, intake_port).await?;

    // 5. Wait for readiness
    agent.wait_ready(config.readiness_timeout).await?;
    debug!("agent is ready, waiting for logs pipeline to initialize");

    // Give the agent time to fully initialize its logs pipeline and start
    // tailing files. The container being "running" doesn't mean the logs
    // pipeline is ready.
    tokio::time::sleep(config.pipeline_warmup).await;
    debug!("pipeline warmup complete");

    // 6. Generate and write log lines
    let input = S::generate_input(params);
    let log_file = log_dir.join("proptest.log");
    write_log_batch(&log_file, &input).await?;
    info!("wrote {} log lines to {}", input.lines.len(), log_file.display());

    // 7. Wait for drain
    debug!("waiting {:?} for drain", config.drain_timeout);
    tokio::time::sleep(config.drain_timeout).await;

    // 8. Stop agent
    agent.stop().await?;
    debug!("agent stopped");

    // 9. Collect output
    let output = intake.stop().await;
    info!("collected {} output entries", output.len());

    // Dump output and summary to temp dir for inspection
    dump_output(&temp_path, &output)?;
    let proptest_output = output
        .iter()
        .filter(|e| crate::log_format::LogFormat::extract_id(&e.message).is_some())
        .count();
    dump_summary(&temp_path, &[
        "=== Single Batch ===".to_string(),
        format!("Lines written: {}", input.lines.len()),
        format!("Total output entries: {}", output.len()),
        format!("Proptest output entries: {proptest_output}"),
    ])?;

    // 10. Check properties
    let properties = S::properties(params);
    let property_results: Vec<Result<(), PropertyFailure>> = properties
        .iter()
        .map(|prop| prop.check(&input, &output))
        .collect();

    let all_passed = property_results.iter().all(Result::is_ok);

    Ok(TestCaseResult {
        input,
        output,
        property_results,
        temp_dir_path: temp_path,
        _temp_dir: if !all_passed || config.keep_temp {
            let path = temp_dir.keep();
            info!("preserving temp dir: {}", path.display());
            None
        } else {
            Some(temp_dir)
        },
    })
}

/// Write a log batch to a file, one line per line.
async fn write_log_batch(path: &Path, batch: &LogBatch) -> Result<(), std::io::Error> {
    let mut file = tokio::fs::File::create(path).await?;
    for line in &batch.lines {
        file.write_all(line.content.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }
    file.flush().await?;
    Ok(())
}

// --- Action Sequence Support ---

/// A step in an action sequence.
#[derive(Debug, Clone)]
pub enum Action {
    /// Append lines to the log file.
    WriteLines(Vec<LogLine>),
    /// Sleep for a duration (e.g., to allow credit refill).
    Sleep(Duration),
}

/// Run an action sequence against the agent.
///
/// Like [`run_case`] but instead of writing all logs at once, executes a
/// sequence of write and sleep actions. Used for scenarios that depend on
/// timing (e.g., adaptive sampling credit refill).
///
/// # Errors
///
/// Returns error if any infrastructure step fails.
///
/// # Panics
///
/// Panics if the log directory path is not valid UTF-8.
#[expect(clippy::too_many_lines)]
pub async fn run_action_sequence(
    config: &OrchestratorConfig,
    log_source_config: LogSourceConfig,
    max_message_size_bytes: Option<usize>,
    actions: &[Action],
    properties: Vec<Box<dyn Property>>,
) -> Result<TestCaseResult, Error> {
    let temp_base = dirs_or_home();
    let temp_dir = TempDir::with_prefix_in("lading_proptest_", temp_base)?;
    let temp_path = temp_dir.path().to_owned();
    let config_dir = temp_path.join("config");
    let log_dir = temp_path.join("logs");
    std::fs::create_dir_all(&config_dir)?;
    std::fs::create_dir_all(&log_dir)?;

    info!("test case temp dir: {}", temp_path.display());

    let intake = LogIntakeServer::start().await?;
    let intake_port = intake.port();
    debug!("intake server on port {intake_port}");

    let log_file_path = match &config.agent_target {
        AgentTarget::Container(_) => "/var/log/proptest/proptest.log".to_string(),
        AgentTarget::Binary(_) => {
            log_dir
                .join("proptest.log")
                .to_str()
                .expect("log path must be valid UTF-8")
                .to_string()
        }
    };

    let agent_config_params = AgentConfigParams {
        intake_port,
        log_file_path,
        use_compression: config.use_compression,
        batch_wait_ms: config.batch_wait_ms,
        log_source_config,
        max_message_size_bytes,
    };
    config::write_agent_config(&config_dir, &agent_config_params)?;

    let agent = config.agent_target.start(&config_dir, &log_dir, intake_port).await?;
    agent.wait_ready(config.readiness_timeout).await?;
    debug!("agent is ready, waiting for logs pipeline to initialize");
    tokio::time::sleep(config.pipeline_warmup).await;
    debug!("pipeline warmup complete");

    // Execute actions, collecting all written lines and a summary log
    let mut all_lines = Vec::new();
    let mut summary_lines: Vec<String> = Vec::new();
    let log_file = log_dir.join("proptest.log");
    let mut total_lines_written: usize = 0;

    summary_lines.push("=== Action Sequence ===".to_string());

    for (step_idx, action) in actions.iter().enumerate() {
        match action {
            Action::WriteLines(lines) => {
                append_lines(&log_file, lines).await?;
                let first_id = lines.first().map_or("?", |l| l.id.as_str());
                let last_id = lines.last().map_or("?", |l| l.id.as_str());
                summary_lines.push(format!(
                    "Step {step_idx}: Write {} lines (IDs {first_id}..{last_id})",
                    lines.len(),
                ));
                info!("wrote {} lines to {}", lines.len(), log_file.display());
                total_lines_written += lines.len();
                all_lines.extend_from_slice(lines);
            }
            Action::Sleep(duration) => {
                summary_lines.push(format!("Step {step_idx}: Sleep {duration:?}"));
                info!("sleeping {:?}", duration);
                tokio::time::sleep(*duration).await;
            }
        }
    }

    summary_lines.push(format!("\nTotal lines written: {total_lines_written}"));

    // Drain
    debug!("waiting {:?} for drain", config.drain_timeout);
    tokio::time::sleep(config.drain_timeout).await;

    agent.stop().await?;
    debug!("agent stopped");

    let output = intake.stop().await;
    info!("collected {} output entries", output.len());

    summary_lines.push(format!("Total output entries: {}", output.len()));

    // Count proptest entries vs agent-internal entries
    let proptest_output = output
        .iter()
        .filter(|e| crate::log_format::LogFormat::extract_id(&e.message).is_some())
        .count();
    summary_lines.push(format!("Proptest output entries: {proptest_output}"));
    summary_lines.push(format!(
        "Lines dropped: {}",
        total_lines_written.saturating_sub(proptest_output),
    ));

    dump_output(&temp_path, &output)?;
    dump_summary(&temp_path, &summary_lines)?;

    let input = LogBatch {
        lines: all_lines,
        format: crate::log_format::LogFormat::PlainText,
        expected_continuations: Vec::new(),
        expected_json: None,
    };

    let property_results: Vec<Result<(), PropertyFailure>> = properties
        .iter()
        .map(|prop| prop.check(&input, &output))
        .collect();

    let all_passed = property_results.iter().all(Result::is_ok);

    Ok(TestCaseResult {
        input,
        output,
        property_results,
        temp_dir_path: temp_path,
        _temp_dir: if !all_passed || config.keep_temp {
            let path = temp_dir.keep();
            info!("preserving temp dir: {}", path.display());
            None
        } else {
            Some(temp_dir)
        },
    })
}

/// Append lines to an existing file (or create it).
async fn append_lines(path: &Path, lines: &[LogLine]) -> Result<(), std::io::Error> {
    use tokio::fs::OpenOptions;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    for line in lines {
        file.write_all(line.content.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }
    file.flush().await?;
    Ok(())
}

/// Dump received output entries to the temp dir for manual inspection.
fn dump_output(temp_path: &Path, output: &[ReceivedLogEntry]) -> Result<(), std::io::Error> {
    use std::io::Write;

    // Raw JSON array of all received entries
    let json_path = temp_path.join("output.json");
    let json = serde_json::to_string_pretty(output).unwrap_or_else(|_| "[]".to_string());
    std::fs::write(&json_path, json)?;

    // One message per line (easier to diff against input)
    let messages_path = temp_path.join("output_messages.txt");
    let mut f = std::fs::File::create(&messages_path)?;
    for (i, entry) in output.iter().enumerate() {
        writeln!(f, "[{i}] ({} bytes) {}", entry.message.len(), entry.message)?;
    }

    info!("output dumped to {} and {}", json_path.display(), messages_path.display());
    Ok(())
}

/// Dump a summary of the test case execution.
fn dump_summary(temp_path: &Path, lines: &[String]) -> Result<(), std::io::Error> {
    let path = temp_path.join("summary.txt");
    std::fs::write(&path, lines.join("\n"))?;
    info!("summary dumped to {}", path.display());
    Ok(())
}
