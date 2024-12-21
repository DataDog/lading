use std::{
    env,
    fmt::{self, Display},
    io::Read,
    num::NonZeroU32,
    path::PathBuf,
    str::FromStr,
};

use clap::{ArgGroup, Parser, Subcommand};
use lading::{
    blackhole,
    captures::CaptureManager,
    config::{Config, Telemetry},
    generator::{self, process_tree},
    inspector, observer,
    target::{self, Behavior, Output},
    target_metrics,
};
use metrics::gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use once_cell::sync::Lazy;
use rand::{rngs::StdRng, SeedableRng};
use regex::Regex;
use rustc_hash::FxHashMap;
use tokio::{
    runtime::Builder,
    signal,
    sync::broadcast,
    time::{self, sleep, Duration},
};
use tracing::{debug, error, info, info_span, warn, Instrument};
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter};

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
    #[error("Failed to deserialize Lading config: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
    #[error("Lading failed to sync servers {0}")]
    Send(#[from] tokio::sync::broadcast::error::SendError<Option<i32>>),
    #[error("Parsing Prometheus address failed: {0}")]
    PrometheusAddr(#[from] std::net::AddrParseError),
    #[error("Invalid capture path")]
    CapturePath,
    #[error("Invalid path for prometheus socket")]
    PrometheusPath,
    #[error("Process tree failed to generate tree")]
    ProcessTree(#[from] process_tree::Error),
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
        for (k, v) in self.inner.iter() {
            write!(f, "{k}={v},")?;
        }
        Ok(())
    }
}

impl FromStr for CliKeyValues {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // A key always matches `[[:alpha:]_]+` and a value conforms to
        // `[[:alpha:]_:,`. A key is always followed by a '=' and then a
        // value. A key and value pair are delimited from other pairs by
        // ','. But note that ',' is a valid character in a value, so it's
        // ambiguous whether the last member of a value after a ',' is a key.
        //
        // The approach taken here is to use the key notion as delimiter and
        // then tidy up afterward to find values.
        static RE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"([[:alpha:]_]+)=").expect("Invalid regex pattern provided"));

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

#[derive(Parser)]
#[clap(version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["target_path", "target_pid", "target_container", "no_target"]),
))]
#[clap(group(
    ArgGroup::new("telemetry")
        .required(true)
        .args(&["capture_path", "prometheus_addr", "prometheus_path"]),
))]
#[clap(group(
     ArgGroup::new("experiment-duration")
           .required(false)
           .args(&["experiment_duration_seconds", "experiment_duration_infinite"]),
))]
struct Opts {
    /// path on disk to the configuration file
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
    /// Extra sub commands
    #[clap(subcommand)]
    extracmds: Option<ExtraCommands>,
}

#[derive(Subcommand, Debug)]
#[clap(hide = true)]
enum ExtraCommands {
    ProcessTreeGen(ProcessTreeGen),
}

#[derive(Parser, Debug)]
#[clap(group(
    ArgGroup::new("config")
        .required(true)
        .args(&["config-path", "config-content"]),
))]
struct ProcessTreeGen {
    /// path on disk to the configuration file
    #[clap(long)]
    config_path: Option<PathBuf>,
    /// string repesanting the configuration
    #[clap(long)]
    config_content: Option<String>,
}

fn get_config(ops: &Opts, config: Option<String>) -> Result<Config, Error> {
    let contents = if let Some(config) = config {
        config
    } else if let Ok(env_var_value) = env::var("LADING_CONFIG") {
        debug!("Using config from env var 'LADING_CONFIG'");
        env_var_value
    } else {
        debug!(
            "Attempting to open configuration file at: {}",
            ops.config_path
        );
        let mut file: std::fs::File = std::fs::OpenOptions::new()
            .read(true)
            .open(&ops.config_path)
            .unwrap_or_else(|_| {
                panic!("Could not open configuration file at: {}", &ops.config_path)
            });
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        contents
    };

    let mut config: Config = serde_yaml::from_str(&contents)?;

    let target = if ops.no_target {
        None
    } else if let Some(pid) = ops.target_pid {
        Some(target::Config::Pid(target::PidConfig {
            pid: pid.try_into().expect("Could not convert pid to i32"),
        }))
    } else if let Some(name) = &ops.target_container {
        Some(target::Config::Docker(target::DockerConfig {
            name: name.clone(),
        }))
    } else if let Some(path) = &ops.target_path {
        Some(target::Config::Binary(target::BinaryConfig {
            command: path.clone(),
            arguments: ops.target_arguments.clone(),
            inherit_environment: ops.target_inherit_environment,
            environment_variables: ops
                .target_environment_variables
                .clone()
                .unwrap_or_default()
                .inner,
            output: Output {
                stderr: ops.target_stderr_path.clone(),
                stdout: ops.target_stdout_path.clone(),
            },
        }))
    } else {
        unreachable!("clap ensures that exactly one target option is selected");
    };
    config.target = target;

    let options_global_labels = ops.global_labels.clone().unwrap_or_default();
    if let Some(ref prom_addr) = ops.prometheus_addr {
        config.telemetry = Telemetry::Prometheus {
            addr: prom_addr.parse()?,
            global_labels: options_global_labels.inner,
        };
    } else if let Some(ref prom_path) = ops.prometheus_path {
        config.telemetry = Telemetry::PrometheusSocket {
            path: prom_path.parse().map_err(|_| Error::PrometheusPath)?,
            global_labels: options_global_labels.inner,
        };
    } else if let Some(ref capture_path) = ops.capture_path {
        config.telemetry = Telemetry::Log {
            path: capture_path.parse().map_err(|_| Error::CapturePath)?,
            global_labels: options_global_labels.inner,
            expiration: Duration::from_secs(ops.capture_expiriation_seconds.unwrap_or(u64::MAX)),
        };
    } else {
        match config.telemetry {
            Telemetry::Prometheus {
                ref mut global_labels,
                ..
            } => {
                for (k, v) in options_global_labels.inner {
                    global_labels.insert(k, v);
                }
            }
            Telemetry::PrometheusSocket {
                ref mut global_labels,
                ..
            } => {
                for (k, v) in options_global_labels.inner {
                    global_labels.insert(k, v);
                }
            }
            Telemetry::Log {
                ref mut global_labels,
                ..
            } => {
                for (k, v) in options_global_labels.inner {
                    global_labels.insert(k, v);
                }
            }
        }
    }
    Ok(config)
}

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
    match config.telemetry {
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
        } => {
            let mut capture_manager = CaptureManager::new(
                path,
                shutdown_watcher.register()?,
                experiment_started_watcher.clone(),
                target_running_watcher.clone(),
                expiration,
            )
            .await?;
            for (k, v) in global_labels {
                capture_manager.add_global_label(k, v);
            }
            tokio::spawn(async {
                capture_manager
                    .start()
                    .expect("failed to start capture manager");
            });
        }
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

    let mut gsrv_joinset = tokio::task::JoinSet::new();
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
    if let Some(inspector_conf) = config.inspector {
        if !disable_inspector {
            let tgt_rcv = tgt_snd.subscribe();
            let inspector_server =
                inspector::Server::new(inspector_conf, shutdown_watcher.clone())?;
            let _isrv = tokio::spawn(inspector_server.run(tgt_rcv));
        }
    }

    //
    // BLACKHOLE
    //
    if let Some(cfgs) = config.blackhole {
        for cfg in cfgs {
            let blackhole_server = blackhole::Server::new(cfg, shutdown_watcher.clone())?;
            let _bsrv = tokio::spawn(async {
                match blackhole_server.run().await {
                    Ok(()) => debug!("blackhole shut down successfully"),
                    Err(err) => warn!("blackhole failed with {:?}", err),
                }
            });
        }
    }

    //
    // TARGET METRICS
    //
    if let Some(cfgs) = config.target_metrics {
        for cfg in cfgs {
            let metrics_server = target_metrics::Server::new(
                cfg,
                shutdown_watcher.clone(),
                target_running_watcher.clone(),
            );
            tokio::spawn(async {
                match metrics_server.run().await {
                    Ok(()) => debug!("target_metrics shut down successfully"),
                    Err(err) => warn!("target_metrics failed with {:?}", err),
                }
            });
        }
    }

    let mut tsrv_joinset = tokio::task::JoinSet::new();
    let mut osrv_joinset = tokio::task::JoinSet::new();
    //
    // OBSERVER
    //
    // Observer is not used when there is no target.
    if let Some(target) = config.target {
        let obs_rcv = tgt_snd.subscribe();
        let observer_server = observer::Server::new(config.observer, shutdown_watcher.clone())?;
        osrv_joinset.spawn(observer_server.run(obs_rcv));

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
    };

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
            _ = interval.tick() => {
                gauge!("lading.running").set(1.0);
            },

            _ = signal::ctrl_c() => {
                info!("received ctrl-c");
                break Ok(());
            },
            _ = &mut timer_watcher_wait => {
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
                        Ok(_) => {
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
    res
}

fn run_process_tree(opts: ProcessTreeGen) -> Result<(), Error> {
    let mut contents = String::new();

    if let Some(path) = opts.config_path {
        debug!(
            "Attempting to open configuration file at: {}",
            path.display()
        );
        let mut file: std::fs::File = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .unwrap_or_else(|_| panic!("Could not open configuration file at: {}", path.display()));

        file.read_to_string(&mut contents)?;
    } else if let Some(str) = &opts.config_content {
        contents = str.to_string()
    } else {
        unreachable!("clap ensures that exactly one target option is selected");
    };

    match process_tree::get_config(&contents) {
        Ok(config) => {
            info!("Generating a process tree.");

            let mut rng = StdRng::from_seed(config.seed);
            let nodes = process_tree::generate_tree(&mut rng, &config)?;

            process_tree::spawn_tree(&nodes, config.process_sleep_ns.get())?;

            info!("Bye. :)");
        }
        Err(e) => panic!("invalide configuration: {e}"),
    }
    Ok(())
}

fn run_extra_cmds(cmds: ExtraCommands) -> Result<(), Error> {
    match cmds {
        // This command will call fork and the process must be kept fork-safe up to this point.
        ExtraCommands::ProcessTreeGen(opts) => run_process_tree(opts),
    }
}

fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .finish()
        .init();

    let version = env!("CARGO_PKG_VERSION");
    info!("Starting lading {version} run.");
    let opts: Opts = Opts::parse();

    // handle extra commands
    if let Some(cmds) = opts.extracmds {
        run_extra_cmds(cmds)?;
        return Ok(());
    }

    let config = get_config(&opts, None);

    let experiment_duration = if opts.experiment_duration_infinite {
        Duration::MAX
    } else {
        Duration::from_secs(opts.experiment_duration_seconds.into())
    };

    let warmup_duration = Duration::from_secs(opts.warmup_duration_seconds.into());
    // The maximum shutdown delay is shared between `inner_main` and this
    // function, hence the divide by two.
    let max_shutdown_delay = Duration::from_secs(opts.max_shutdown_delay.into());
    let disable_inspector = opts.disable_inspector;

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
        let ops: &Opts = &Opts::parse_from(args);
        let config = get_config(ops, Some(contents.to_string()));
        let exit_code = inner_main(
            Duration::from_millis(2500),
            Duration::from_millis(5000),
            false,
            config.expect("Could not convert to valid Config"),
        )
        .await;

        assert!(exit_code.is_ok());

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
}
