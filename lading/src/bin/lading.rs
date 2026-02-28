//! Main lading binary for load testing.

use std::{
    env,
    fmt::{self, Display},
    io,
    num::NonZeroU32,
    path::{Path, PathBuf},
    str::FromStr,
};

use clap::{ArgGroup, Args, Parser, Subcommand};
use lading::{
    blackhole,
    config::{self, Config, Telemetry},
    generator, inspector, observer,
    target::{self, Behavior, Output},
    target_metrics,
};
use lading_capture::manager::{CaptureManager, SignalWatchers, DEFAULT_TICK_DURATION_MS};
use metrics::gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use regex::Regex;
use rustc_hash::FxHashMap;
use std::sync::LazyLock;
use tokio::{
    runtime::{Builder, Handle},
    signal,
    sync::broadcast,
    task,
    time::{self, Duration, sleep},
};
use tracing::{Instrument, debug, error, info, info_span, warn};
use tracing_subscriber::{EnvFilter, util::SubscriberInitExt};

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Target related error: {0}")]
    Target(target::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Lading generator returned an error: {0}")]
    LadingGenerator(#[from] lading::generator::Error),
    #[error("Lading blackhole returned an error: {0}")]
    LadingBlackhole(#[from] lading::blackhole::Error),
    #[error("Lading inspector returned an error: {0}")]
    LadingInspector(#[from] lading::inspector::Error),
    #[error("Lading observer returned an error: {0}")]
    LadingObserver(#[from] lading::observer::Error),
    #[error("Failed to load or parse configuration: {0}")]
    Config(config::Error),
    #[error("Failed to deserialize Lading config: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
    #[error("Lading failed to sync servers {0}")]
    Send(#[from] broadcast::error::SendError<Option<i32>>),
    #[error("Parsing Prometheus address failed: {0}")]
    PrometheusAddr(#[from] std::net::AddrParseError),
    #[error("Invalid capture path")]
    CapturePath,
    #[error("Invalid path for prometheus socket")]
    PrometheusPath,
    #[error("Invalid capture format, must be 'jsonl', 'parquet', or 'multi'")]
    InvalidCaptureFormat,
    #[error(
        "Telemetry must be configured either via command-line flags (--capture-path, --prometheus-addr, or --prometheus-path) or in the configuration file"
    )]
    MissingTelemetry,
    #[error(transparent)]
    Registration(#[from] lading_signal::RegisterError),
}

fn default_config_path() -> String {
    "/etc/lading/lading.yaml".to_string()
}

fn default_target_behavior() -> Behavior {
    Behavior::Quiet
}

#[derive(Default, Clone)]
struct CliKeyValues {
    inner: FxHashMap<String, String>,
}

impl CliKeyValues {
    #[cfg(test)]
    fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
    }
}

impl Display for CliKeyValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        for (k, v) in &self.inner {
            write!(f, "{k}={v},")?;
        }
        Ok(())
    }
}

impl FromStr for CliKeyValues {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // A key matches `[[:alnum:]_-]+` (letters, digits, underscores,
        // hyphens) and a value conforms to `[[:alpha:]_:,`. A key is always
        // followed by a '=' and then a value. A key and value pair are
        // delimited from other pairs by ','. But note that ',' is a valid
        // character in a value, so it's ambiguous whether the last member of
        // a value after a ',' is a key.
        //
        // The approach taken here is to use the key notion as delimiter and
        // then tidy up afterward to find values.
        //
        // NOTE: the key regex must include hyphens and digits so that keys
        // like "example-tag" or "image_tag" are matched as a single token.
        // Without this, "example-tag=val" would be misparsed as key="tag"
        // and the "example-" fragment would pollute the preceding value.
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"([[:alnum:]_-]+)=").expect("Invalid regex pattern provided")
        });

        let mut labels = FxHashMap::default();

        for cap in RE.captures_iter(input) {
            let key = cap[1].to_string();
            let start = cap.get(0).expect("value 0 not found in Captures").end();

            // Find the next key or run into the end of the input.
            let end = RE.find_at(input, start).map_or(input.len(), |m| m.start());

            // Extract the value.
            let value = input[start..end].trim_end_matches(',').to_string();

            labels.insert(key, value);
        }

        Ok(Self { inner: labels })
    }
}

// Parser for subcommand structure
#[derive(Parser)]
#[clap(version, about, long_about = None)]
struct CliWithSubcommands {
    /// output logs in JSON format instead of text
    #[clap(long)]
    json_logs: bool,
    #[command(subcommand)]
    command: Commands,
}

// Parser for legacy flat structure (deprecated)
#[derive(Parser)]
#[clap(version, about, long_about = None)]
struct CliFlatLegacy {
    /// output logs in JSON format instead of text
    #[clap(long)]
    json_logs: bool,
    #[command(flatten)]
    args: LadingArgs,
}

// Shared arguments used by both modes
#[derive(clap::Args)]
#[allow(clippy::struct_excessive_bools)]
#[clap(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["target_path", "target_pid", "target_container", "no_target"]),
))]
#[clap(group(
    ArgGroup::new("telemetry")
        .required(false)
        .args(&["capture_path", "prometheus_addr", "prometheus_path"]),
))]
#[clap(group(
     ArgGroup::new("experiment-duration")
           .required(false)
           .args(&["experiment_duration_seconds", "experiment_duration_infinite"]),
))]
struct LadingArgs {
    /// path on disk to a configuration file or directory containing config files
    #[clap(long, default_value_t = default_config_path())]
    config_path: String,
    /// additional labels to apply to all captures, format KEY=VAL,KEY2=VAL
    #[clap(long)]
    global_labels: Option<CliKeyValues>,
    /// measure an externally-launched process by PID
    #[clap(long)]
    target_pid: Option<NonZeroU32>,
    /// measure an externally-launched container by name
    #[clap(long)]
    target_container: Option<String>,
    /// disable target measurement
    #[clap(long)]
    no_target: bool,
    /// the path of the target executable
    #[clap(long, group = "binary-target")]
    target_path: Option<PathBuf>,
    /// inherit the target environment variables from lading's environment
    #[clap(long, requires = "binary-target", action)]
    target_inherit_environment: bool,
    /// additional environment variables to apply to the target, format
    /// KEY=VAL,KEY2=VAL
    #[clap(long, requires = "binary-target")]
    target_environment_variables: Option<CliKeyValues>,
    /// arguments for the target executable
    #[clap(requires = "binary-target")]
    target_arguments: Vec<String>,
    /// the path to write target's stdout
    #[clap(long, default_value_t = default_target_behavior(), requires = "binary-target")]
    target_stdout_path: Behavior,
    /// the path to write target's stderr
    #[clap(long, default_value_t = default_target_behavior(), requires = "binary-target")]
    target_stderr_path: Behavior,
    /// path on disk to write captures, exclusive of prometheus-path and
    /// prometheus-addr
    #[clap(long)]
    capture_path: Option<String>,
    /// time that capture metrics will expire by if they are not seen again, only useful when capture-path is set
    #[clap(long)]
    capture_expiriation_seconds: Option<u64>,
    /// capture file format: jsonl, parquet, or multi (default: jsonl)
    #[clap(long, default_value = "jsonl")]
    capture_format: String,
    /// number of seconds to buffer before flushing capture file (default: 60)
    #[clap(long, default_value_t = 60)]
    capture_flush_seconds: u64,
    /// parquet compression level (1-22, default: 3), only used when capture-format=parquet
    #[clap(long, default_value_t = 3)]
    capture_compression_level: i32,
    /// address to bind prometheus exporter to, exclusive of prometheus-path and
    /// promtheus-addr
    #[clap(long)]
    prometheus_path: Option<String>,
    /// socket to bind prometheus exporter to, exclusive of prometheus-addr and
    /// capture-path
    #[clap(long)]
    prometheus_addr: Option<String>,
    /// the maximum time to wait, in seconds, for controlled shutdown
    #[clap(long, default_value_t = 30)]
    max_shutdown_delay: u16,
    /// the time, in seconds, to run the target and collect samples about it
    #[clap(long, default_value_t = 120)]
    experiment_duration_seconds: u32,
    /// flag to allow infinite experiment duration
    #[clap(long)]
    experiment_duration_infinite: bool,
    /// the time, in seconds, to allow the target to run without collecting
    /// samples
    #[clap(long, default_value_t = 30)]
    warmup_duration_seconds: u32,
    /// whether to ignore inspector configuration, if present, and not run the inspector
    #[clap(long)]
    disable_inspector: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run lading with specified configuration
    Run(Box<RunCommand>),
    /// Validate configuration file and exit
    ConfigCheck(ConfigCheckCommand),
}

#[derive(Args)]
struct RunCommand {
    #[command(flatten)]
    args: LadingArgs,
}

#[derive(Args)]
struct ConfigCheckCommand {
    /// path on disk to a configuration file or directory containing config files
    #[clap(long, default_value_t = default_config_path())]
    config_path: String,
}

fn parse_config(contents: &str) -> Result<Config, Error> {
    serde_yaml::from_str(contents).map_err(|err| {
        error!("Configuration validation failed: {}", err);
        Error::SerdeYaml(err)
    })
}

fn validate_config(config_path: &str) -> Result<Config, Error> {
    // Check if config is provided via environment variable
    if let Ok(env_var_value) = env::var("LADING_CONFIG") {
        debug!("Using config from env var 'LADING_CONFIG'");
        let config = parse_config(&env_var_value)?;
        info!("Configuration file is valid");
        Ok(config)
    } else {
        // Load from path (file or directory)
        debug!("Attempting to load configuration from: {config_path}");
        let config = config::load_config_from_path(Path::new(config_path)).map_err(|err| {
            error!("Could not load config from '{config_path}': {err}");
            Error::Config(err)
        })?;
        info!("Configuration file is valid");
        Ok(config)
    }
}

fn get_config(args: &LadingArgs, config: Option<String>) -> Result<Config, Error> {
    let mut config = if let Some(contents) = config {
        // Config provided via environment variable - parse as single file
        parse_config(&contents)?
    } else {
        // Load from path (auto-detect file or directory)
        config::load_config_from_path(Path::new(&args.config_path)).map_err(Error::Config)?
    };

    let target = if args.no_target {
        None
    } else if let Some(pid) = args.target_pid {
        Some(target::Config::Pid(target::PidConfig {
            pid: pid.try_into().expect("Could not convert pid to i32"),
        }))
    } else if let Some(name) = &args.target_container {
        Some(target::Config::Docker(target::DockerConfig {
            name: name.clone(),
        }))
    } else if let Some(path) = &args.target_path {
        Some(target::Config::Binary(target::BinaryConfig {
            command: path.clone(),
            arguments: args.target_arguments.clone(),
            inherit_environment: args.target_inherit_environment,
            environment_variables: args
                .target_environment_variables
                .clone()
                .unwrap_or_default()
                .inner,
            output: Output {
                stderr: args.target_stderr_path.clone(),
                stdout: args.target_stdout_path.clone(),
            },
        }))
    } else {
        unreachable!("clap ensures that exactly one target option is selected");
    };
    config.target = target;

    let options_global_labels = args.global_labels.clone().unwrap_or_default();
    if let Some(ref prom_addr) = args.prometheus_addr {
        config.telemetry = Some(Telemetry::Prometheus {
            addr: prom_addr.parse()?,
            global_labels: options_global_labels.inner,
        });
    } else if let Some(ref prom_path) = args.prometheus_path {
        config.telemetry = Some(Telemetry::PrometheusSocket {
            path: prom_path.parse().map_err(|_| Error::PrometheusPath)?,
            global_labels: options_global_labels.inner,
        });
    } else if let Some(ref capture_path) = args.capture_path {
        let format = match args.capture_format.as_str() {
            "jsonl" => config::CaptureFormat::Jsonl {
                flush_seconds: args.capture_flush_seconds,
            },
            "parquet" => config::CaptureFormat::Parquet {
                flush_seconds: args.capture_flush_seconds,
                compression_level: args.capture_compression_level,
            },
            "multi" => config::CaptureFormat::Multi {
                flush_seconds: args.capture_flush_seconds,
                compression_level: args.capture_compression_level,
            },
            _ => return Err(Error::InvalidCaptureFormat),
        };

        config.telemetry = Some(Telemetry::Log {
            path: capture_path.parse().map_err(|_| Error::CapturePath)?,
            global_labels: options_global_labels.inner,
            expiration: Duration::from_secs(args.capture_expiriation_seconds.unwrap_or(u64::MAX)),
            format,
        });
    } else if let Some(ref mut telemetry) = config.telemetry {
        // Telemetry was configured in YAML, merge in global_labels from CLI if any
        match telemetry {
            Telemetry::Prometheus { global_labels, .. }
            | Telemetry::PrometheusSocket { global_labels, .. }
            | Telemetry::Log { global_labels, .. } => {
                for (k, v) in options_global_labels.inner {
                    global_labels.insert(k, v);
                }
            }
        }
    }

    // Validate that telemetry was configured somewhere
    if config.telemetry.is_none() {
        return Err(Error::MissingTelemetry);
    }

    Ok(config)
}

#[allow(clippy::too_many_lines)]
async fn inner_main(
    experiment_duration: Duration,
    warmup_duration: Duration,
    disable_inspector: bool,
    config: Config,
) -> Result<(), Error> {
    let (shutdown_watcher, shutdown_broadcast) = lading_signal::signal();
    let (experiment_started_watcher, experiment_started_broadcast) = lading_signal::signal();
    let (target_running_watcher, target_running_broadcast) = lading_signal::signal();

    // Set up the telemetry sub-system.
    //
    // We support two methods to exflitrate telemetry about the target from rig:
    // a passive prometheus export and an active log file. Only one can be
    // active at a time.
    let mut capture_manager_handle: Option<tokio::task::JoinHandle<()>> = None;
    match config
        .telemetry
        .expect("telemetry should be validated in get_config")
    {
        Telemetry::PrometheusSocket {
            path,
            global_labels,
        } => {
            let mut builder = PrometheusBuilder::new().with_http_uds_listener(path);
            for (k, v) in global_labels {
                builder = builder.add_global_label(k, v);
            }
            tokio::spawn(async move {
                builder
                    .install()
                    .expect("failed to install prometheus recorder");
            });
        }
        Telemetry::Prometheus {
            addr,
            global_labels,
        } => {
            let mut builder = PrometheusBuilder::new().with_http_listener(addr);
            for (k, v) in global_labels {
                builder = builder.add_global_label(k, v);
            }
            tokio::spawn(async move {
                builder
                    .install()
                    .expect("failed to install prometheus recorder");
            });
        }
        Telemetry::Log {
            path,
            global_labels,
            expiration,
            format,
        } => match format {
            config::CaptureFormat::Jsonl { flush_seconds } => {
                let signals = SignalWatchers {
                    shutdown: shutdown_watcher.register()?,
                    experiment_started: experiment_started_watcher.clone(),
                    target_running: target_running_watcher.clone(),
                };
                let mut capture_manager = CaptureManager::new_jsonl(
                    path,
                    flush_seconds,
                    DEFAULT_TICK_DURATION_MS,
                    expiration,
                    signals,
                )
                .await?;
                for (k, v) in global_labels {
                    capture_manager.add_global_label(k, v);
                }
                let handle = tokio::task::spawn_blocking(move || {
                    Handle::current()
                        .block_on(capture_manager.start())
                        .expect("failed to start capture manager");
                });
                capture_manager_handle = Some(handle);
            }
            config::CaptureFormat::Multi {
                flush_seconds,
                compression_level,
            } => {
                let signals = SignalWatchers {
                    shutdown: shutdown_watcher.register()?,
                    experiment_started: experiment_started_watcher.clone(),
                    target_running: target_running_watcher.clone(),
                };
                let mut capture_manager = CaptureManager::new_multi(
                    path,
                    compression_level,
                    flush_seconds,
                    DEFAULT_TICK_DURATION_MS,
                    expiration,
                    signals,
                )
                .await
                .map_err(io::Error::other)?;
                for (k, v) in global_labels {
                    capture_manager.add_global_label(k, v);
                }
                let handle = tokio::task::spawn_blocking(move || {
                    Handle::current()
                        .block_on(capture_manager.start())
                        .expect("failed to start capture manager");
                });
                capture_manager_handle = Some(handle);
            }
            config::CaptureFormat::Parquet {
                flush_seconds,
                compression_level,
            } => {
                let signals = SignalWatchers {
                    shutdown: shutdown_watcher.register()?,
                    experiment_started: experiment_started_watcher.clone(),
                    target_running: target_running_watcher.clone(),
                };
                let mut capture_manager = CaptureManager::new_parquet(
                    path,
                    compression_level,
                    flush_seconds,
                    DEFAULT_TICK_DURATION_MS,
                    expiration,
                    signals,
                )
                .await
                .map_err(io::Error::other)?;
                for (k, v) in global_labels {
                    capture_manager.add_global_label(k, v);
                }
                let handle = tokio::task::spawn_blocking(move || {
                    Handle::current()
                        .block_on(capture_manager.start())
                        .expect("failed to start capture manager");
                });
                capture_manager_handle = Some(handle);
            }
        },
    }

    // Set up the application servers. These are, depending on configuration:
    //
    // * the "generator" which pushes load into
    // * the "target" which is the measured system and might push load into
    // * the "blackhole" which may or may not exist.
    //
    // There is also, maybe:
    //
    // * the "inspector" which is a sub-process that users can rig to inspect
    //   the target.
    // * the "observer" which reads procfs on Linux and reports relevant process
    //   detail to the capture log

    let (tgt_snd, _tgt_rcv) = broadcast::channel(1);

    let mut gsrv_joinset = task::JoinSet::new();
    //
    // GENERATOR
    //
    for cfg in config.generator {
        let tgt_rcv = tgt_snd.subscribe();
        let generator_server = generator::Server::new(cfg, shutdown_watcher.clone())?;
        gsrv_joinset.spawn(generator_server.run(tgt_rcv));
    }

    //
    // INSPECTOR
    //
    if let Some(inspector_conf) = config.inspector
        && !disable_inspector
    {
        let tgt_rcv = tgt_snd.subscribe();
        let inspector_server = inspector::Server::new(inspector_conf, shutdown_watcher.clone())?;
        let _isrv = tokio::spawn(inspector_server.run(tgt_rcv));
    }

    //
    // BLACKHOLE
    //
    for cfg in config.blackhole {
        let blackhole_server = blackhole::Server::new(cfg, shutdown_watcher.clone())?;
        let _bsrv = tokio::spawn(async {
            match blackhole_server.run().await {
                Ok(()) => debug!("blackhole shut down successfully"),
                Err(err) => warn!("blackhole failed with {:?}", err),
            }
        });
    }

    //
    // TARGET METRICS
    //
    if let Some(cfgs) = config.target_metrics {
        let sample_period = Duration::from_millis(config.sample_period_milliseconds);

        for cfg in cfgs {
            let metrics_server = target_metrics::Server::new(
                cfg,
                shutdown_watcher.clone(),
                experiment_started_watcher.clone(),
                sample_period,
            );
            tokio::spawn(async {
                match metrics_server.run().await {
                    Ok(()) => debug!("target_metrics shut down successfully"),
                    Err(err) => warn!("target_metrics failed with {:?}", err),
                }
            });
        }
    }

    let mut tsrv_joinset = task::JoinSet::new();
    let mut osrv_joinset = task::JoinSet::new();
    //
    // OBSERVER
    //
    // Observer is not used when there is no target.
    if let Some(target) = config.target {
        let obs_rcv = tgt_snd.subscribe();
        let observer_server = observer::Server::new(config.observer, shutdown_watcher.clone())?;
        let sample_period = Duration::from_millis(config.sample_period_milliseconds);
        osrv_joinset.spawn(observer_server.run(obs_rcv, sample_period));

        //
        // TARGET
        //
        let target_server = target::Server::new(target, shutdown_watcher.clone());
        tsrv_joinset.spawn(target_server.run(tgt_snd, target_running_broadcast));
    } else {
        // Many lading servers synchronize on target startup using the PID sender. Some by necessity, others by legacy.
        tgt_snd.send(None)?;
        // Newer usage prefers the `target_running` signal where the PID isn't needed.
        target_running_broadcast.signal();
    }

    let (timer_watcher, timer_broadcast) = lading_signal::signal();

    tokio::spawn(
        async move {
            info!("waiting for target startup");
            target_running_watcher.recv().await;
            info!("target is running, now sleeping for warmup");
            sleep(warmup_duration).await;
            experiment_started_broadcast.signal();
            info!("warmup completed, collecting samples");
            sleep(experiment_duration).await;
            info!("experiment duration exceeded, signaling for shutdown");
            timer_broadcast.signal();
        }
        .instrument(info_span!("experiment_sequence")),
    );

    // We must be sure to drop any unused watcher at this point. Below in
    // `signal_and_wait` if a watcher remains derived from `shutdown_watcher` the run will not shut down.
    drop(shutdown_watcher);
    drop(experiment_started_watcher);
    let timer_watcher_wait = timer_watcher.recv();
    tokio::pin!(timer_watcher_wait);
    let mut interval = time::interval(Duration::from_millis(400));
    let res = loop {
        tokio::select! {
            _instant = interval.tick() => {
                gauge!("lading.running").set(1.0);
            },

            _res = signal::ctrl_c() => {
                info!("received ctrl-c");
                break Ok(());
            },
            () = &mut timer_watcher_wait => {
                info!("shutdown signal received.");
                break Ok(());
            }
            Some(res) = osrv_joinset.join_next() => {
                match res {
                    Ok(observer_result) => match observer_result {
                        Ok(()) => { /* Observer shut down successfully */ }
                        Err(err) => {
                            error!("Observer shut down unexpectedly: {err}");
                            break Err(Error::LadingObserver(err));
                        }
                    }
                    Err(err) => error!("Could not join the spawned observer task: {}", err),
                }
            },
            Some(res) = gsrv_joinset.join_next() => {
                match res {
                    Ok(generator_result) => match generator_result {
                        Ok(()) => { /* Generator shut down successfully */ }
                        Err(err) => {
                            error!("Generator shut down unexpectedly: {}", err);
                            break Err(Error::LadingGenerator(err));
                        }
                    }
                    Err(err) => error!("Could not join the spawned generator task: {}", err),
                }
            },
            Some(target_result) = tsrv_joinset.join_next() => {
                match target_result {
                    // joined successfully, but how did the target server exit?
                    Ok(target_result) => match target_result {
                        Ok(()) => {
                            debug!("Target shut down successfully");
                            break Ok(());
                        }
                        Err(err) => {
                            error!("Target shut down unexpectedly: {}", err);
                            break Err(Error::Target(err));
                        }
                    }
                    Err(err) => panic!("Could not join the spawned target task: {err}"),
                }
            },
        }
    };

    shutdown_broadcast.signal_and_wait().await;

    // Await the capture manager task, if it exists, to ensure all data is
    // flushed.
    if let Some(handle) = capture_manager_handle {
        let _ = handle.await;
    }

    res
}

fn init_tracing(json_output: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error"));

    if json_output {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .finish()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(false)
            .finish()
            .init();
    }
}

fn main() -> Result<(), Error> {
    // Two-parser fallback logic until CliFlatLegacy is removed
    let (json_output, args) = match CliWithSubcommands::try_parse() {
        Ok(cli) => match cli.command {
            Commands::Run(run_cmd) => (cli.json_logs, run_cmd.args),
            Commands::ConfigCheck(config_check_cmd) => {
                // Handle config-check command
                init_tracing(cli.json_logs);
                match validate_config(&config_check_cmd.config_path) {
                    Ok(_) => std::process::exit(0),
                    Err(_) => std::process::exit(1),
                }
            }
        },
        Err(_) => {
            // Fall back to legacy parsing
            match CliFlatLegacy::try_parse() {
                Ok(legacy) => (legacy.json_logs, legacy.args),
                Err(err) => err.exit(),
            }
        }
    };

    init_tracing(json_output);

    let version = env!("CARGO_PKG_VERSION");
    info!("Starting lading {version} run.");
    let memory_limit =
        lading::get_available_memory().get_appropriate_unit(byte_unit::UnitType::Binary);
    info!(
        "Lading running with {limit} amount of memory.",
        limit = memory_limit.to_string()
    );

    let config = get_config(&args, None);

    let experiment_duration = if args.experiment_duration_infinite {
        Duration::MAX
    } else {
        Duration::from_secs(args.experiment_duration_seconds.into())
    };

    let warmup_duration = Duration::from_secs(args.warmup_duration_seconds.into());
    // The maximum shutdown delay is shared between `inner_main` and this
    // function, hence the divide by two.
    let max_shutdown_delay = Duration::from_secs(args.max_shutdown_delay.into());
    let disable_inspector = args.disable_inspector;

    let runtime = Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?;
    let res = runtime.block_on(inner_main(
        experiment_duration,
        warmup_duration,
        disable_inspector,
        config?,
    ));
    // The splunk_hec generator spawns long running tasks that are not plugged
    // into the shutdown mechanism we have here. This is a bug and needs to be
    // addressed. However as a workaround we explicitly shutdown the
    // runtime. Even when the splunk_hec issue is addressed we'll continue this
    // practice as it's a reasonable safeguard.
    info!(
        "Shutting down runtime with a {} second delay. May leave orphaned tasks.",
        max_shutdown_delay.as_secs(),
    );
    runtime.shutdown_timeout(max_shutdown_delay);
    info!("Bye. :)");
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn inner_main_capture_has_data() {
        let contents = r#"
generator: []
"#;

        let tmp_dir = tempfile::tempdir().expect("directory could not be created");
        let capture_path = tmp_dir.path().join("capture");
        let capture_arg = format!("--capture-path={}", capture_path.display());

        let args = vec!["lading", "--no-target", capture_arg.as_str()];
        let legacy_cli = CliFlatLegacy::parse_from(args);
        let config = get_config(&legacy_cli.args, Some(contents.to_string()));
        let exit_code = inner_main(
            Duration::from_millis(2500),
            Duration::from_millis(5000),
            false,
            config.expect("Could not convert to valid Config"),
        )
        .await;

        if let Err(e) = exit_code {
            panic!("inner_main failed: {:?}", e);
        }

        let contents = std::fs::read_to_string(capture_path)
            .expect("File path does not already exist or does not contain valid utf-8");
        assert!(contents.rmatches("lading.running").count() > 5);
    }

    #[test]
    fn cli_key_values_deserializes_empty_string_to_empty_set() {
        let val = "";
        let deser = CliKeyValues::from_str(val);
        let deser = deser
            .expect("String could not be converted into valid CliKeyValues")
            .to_string();
        assert_eq!("", deser);
    }

    #[test]
    fn cli_key_values_deserializes_kv_list() {
        let val = "first=one,second=two";
        let deser =
            CliKeyValues::from_str(val).expect("String cannot be converted into CliKeyValues");

        assert_eq!(
            deser.get("first").expect("Deser does not have key first"),
            "one"
        );
        assert_eq!(
            deser.get("second").expect("Deser does not have key second"),
            "two"
        );
    }

    #[test]
    fn cli_key_values_deserializes_trailing_comma_kv_list() {
        let val = "first=one,";
        let deser =
            CliKeyValues::from_str(val).expect("String cannot be converted into CliKeyValues");

        assert_eq!(
            deser.get("first").expect("Deser does not have key first"),
            "one"
        );
    }

    #[test]
    fn cli_key_values_deserializes_separated_value_kv_comma() {
        let val = "DD_API_KEY=00000001,DD_TELEMETRY_ENABLED=true,DD_TAGS=uqhwd:b2xiyw,hf9gy:uwcy04";
        let deser = CliKeyValues::from_str(val);
        let deser = deser.expect("String cannot be converted into CliKeyValues");

        println!("RESULT: {deser}");

        assert_eq!(
            deser
                .get("DD_API_KEY")
                .expect("DD_API_KEY is not a valid key for the map"),
            "00000001"
        );
        assert_eq!(
            deser
                .get("DD_TELEMETRY_ENABLED")
                .expect("DD_TELEMETRY_ENABLED is not a valid key for the map"),
            "true"
        );
        assert_eq!(
            deser
                .get("DD_TAGS")
                .expect("DD_TAGS is not a valid key for the map"),
            "uqhwd:b2xiyw,hf9gy:uwcy04"
        );
    }

    #[test]
    fn cli_key_values_parses_hyphenated_keys() {
        let val = "purpose=test,example-tag=example-value";
        let deser =
            CliKeyValues::from_str(val).expect("String cannot be converted into CliKeyValues");

        assert_eq!(
            deser.get("purpose").expect("purpose key missing"),
            "test",
            "purpose value should be exactly 'test', not polluted by adjacent keys"
        );
        assert_eq!(
            deser.get("example-tag").expect("example-tag key missing"),
            "example-value",
            "hyphenated key 'example-tag' should be parsed as a single key"
        );
    }

    #[test]
    fn cli_key_values_parses_production_label_set() {
        // Construct the comma-separated string the same way the harness does
        let labels: Vec<(&str, &str)> = vec![
            ("team_id", "11111111"),
            ("experiment", "some_experiment"),
            ("example-tag", "example-value"),
            ("image_tag", "1.11.1"),
            ("target", "datadog-agent"),
            ("replicate", "3"),
            ("purpose", "testing"),
        ];

        let mut input = String::new();
        for (k, v) in &labels {
            input.push_str(k);
            input.push('=');
            input.push_str(v);
            input.push(',');
        }
        input.pop(); // remove trailing comma

        let deser =
            CliKeyValues::from_str(&input).expect("String cannot be converted into CliKeyValues");

        for (k, v) in &labels {
            let actual = deser.get(*k).unwrap_or_else(|| {
                panic!(
                    "key '{k}' missing from parsed result; got keys: {:?}",
                    deser.inner.keys().collect::<Vec<_>>()
                )
            });
            assert_eq!(
                actual, *v,
                "key '{k}': expected '{v}', got '{actual}' — possible cross-label pollution"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn yaml_only_telemetry_configuration_works() {
        // Test that telemetry can be configured in YAML without CLI flags
        let contents = r#"
generator: []
telemetry:
  addr: "0.0.0.0:9876"
  global_labels: {}
"#;

        let tmp_dir = tempfile::tempdir().expect("directory could not be created");
        let config_path = tmp_dir.path().join("config.yaml");
        tokio::fs::write(&config_path, contents)
            .await
            .expect("Failed to write config file");

        let args = vec![
            "lading",
            "--no-target",
            "--config-path",
            config_path.to_str().unwrap(),
        ];
        let legacy_cli = CliFlatLegacy::parse_from(args);
        let config =
            get_config(&legacy_cli.args, None).expect("Failed to get config with YAML telemetry");

        // Verify telemetry was loaded from YAML
        assert!(
            config.telemetry.is_some(),
            "Telemetry should be loaded from YAML"
        );
        match config.telemetry.unwrap() {
            Telemetry::Prometheus { addr, .. } => {
                assert_eq!(addr.to_string(), "0.0.0.0:9876");
            }
            _ => panic!("Expected Prometheus telemetry"),
        }
    }

    #[test]
    fn missing_telemetry_returns_error() {
        // Test that missing telemetry (no CLI flags, no YAML) returns an error
        let contents = r#"
generator: []
"#;

        let args = vec!["lading", "--no-target"];
        let legacy_cli = CliFlatLegacy::parse_from(args);
        let result = get_config(&legacy_cli.args, Some(contents.to_string()));

        assert!(
            result.is_err(),
            "Should return error when telemetry is not configured"
        );
        match result.unwrap_err() {
            Error::MissingTelemetry => {} // Expected error
            other => panic!("Expected MissingTelemetry error, got: {:?}", other),
        }
    }
}
