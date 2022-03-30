use clap::Parser;
use lading::{
    blackhole,
    captures::CaptureManager,
    config::{Config, Telemetry},
    generator,
    signals::Shutdown,
    target::{self, Behavior, Cmd, Output},
};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::{collections::HashMap, io::Read};
use std::{
    fmt::{self, Display},
    str::FromStr,
};
use tokio::{
    runtime::Builder,
    signal,
    time::{sleep, Duration},
};
use tracing::{debug, error, info};

fn default_config_path() -> String {
    "/etc/lading/lading.yaml".to_string()
}

fn default_target_behavior() -> Behavior {
    Behavior::Quiet
}

#[derive(Default, Clone)]
struct CliKeyValues {
    inner: HashMap<String, String>,
}

impl Display for CliKeyValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        for (k, v) in self.inner.iter() {
            write!(f, "{}={},", k, v)?;
        }
        Ok(())
    }
}

impl FromStr for CliKeyValues {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let pair_err = String::from("pairs must be separated by '='");
        let mut labels = HashMap::new();
        for kv in input.split(',') {
            let mut pair = kv.split('=');
            let key = pair.next().ok_or_else(|| pair_err.clone())?;
            let value = pair.next().ok_or_else(|| pair_err.clone())?;
            labels.insert(key.into(), value.into());
        }
        Ok(Self { inner: labels })
    }
}

#[derive(Parser)]
#[clap(version, about, long_about = None)]
struct Opts {
    /// path on disk to the configuration file
    #[clap(long, default_value_t = default_config_path())]
    config_path: String,
    /// additional labels to apply to all captures, format KEY=VAL,KEY2=VAL
    #[clap(long)]
    global_labels: Option<CliKeyValues>,
    /// additional environment variables to apply to the target, format KEY=VAL,KEY2=VAL
    #[clap(long)]
    target_environment_variables: Option<CliKeyValues>,
    /// the path of the target executable
    target_path: String,
    /// arguments for the target executable
    target_arguments: Vec<String>,
    /// the path to write target's stdout
    #[clap(long, default_value_t = default_target_behavior())]
    target_stdout_path: Behavior,
    /// the path to write target's stderr
    #[clap(long, default_value_t = default_target_behavior())]
    target_stderr_path: Behavior,
    /// if specified, runs target under Linux perf and writes collected perf
    /// data to this location
    #[clap(long)]
    #[cfg(target_os = "linux")]
    target_perf_data_path: Option<String>,
    /// path on disk to write captures, will override prometheus-addr if both
    /// are set
    #[clap(long)]
    capture_path: Option<String>,
    /// address to bind prometheus exporter to, will be overridden by
    /// capture-path if both are set
    #[clap(long)]
    prometheus_addr: Option<String>,
    /// the maximum time to wait, in seconds, for controlled shutdown
    #[clap(long, default_value_t = 30)]
    max_shutdown_delay: u16,
    /// the time, in seconds, to run the target and collect samples about it
    #[clap(long, default_value_t = 120)]
    experiment_duration_seconds: u32,
    /// the time, in seconds, to allow the target to run without collecting samples
    #[clap(long, default_value_t = 30)]
    warmup_duration_seconds: u32,
}

fn get_config() -> (Opts, Config) {
    let ops: Opts = Opts::parse();
    debug!(
        "Attempting to open configuration file at: {}",
        ops.config_path
    );
    let mut file: std::fs::File = std::fs::OpenOptions::new()
        .read(true)
        .open(&ops.config_path)
        .unwrap_or_else(|_| panic!("Could not open configuration file at: {}", &ops.config_path));
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let mut config: Config = serde_yaml::from_str(&contents).unwrap();
    let target_config = target::Config {
        command: ops
            .target_perf_data_path
            .clone()
            .map(|pd| Cmd::Perf {
                inner_command: ops.target_path.clone(),
                perf_data_path: pd,
            })
            .unwrap_or_else(|| Cmd::Path(ops.target_path.clone())),
        arguments: ops.target_arguments.clone(),
        environment_variables: ops
            .target_environment_variables
            .clone()
            .unwrap_or_default()
            .inner,
        output: Output {
            stderr: ops.target_stderr_path.clone(),
            stdout: ops.target_stdout_path.clone(),
        },
    };
    config.target = Some(target_config);
    let options_global_labels = ops.global_labels.clone().unwrap_or_default();
    if let Some(ref prom_addr) = ops.prometheus_addr {
        config.telemetry = Telemetry::Prometheus {
            prometheus_addr: prom_addr.parse().unwrap(),
            global_labels: options_global_labels.inner,
        };
    } else if let Some(ref capture_path) = ops.capture_path {
        config.telemetry = Telemetry::Log {
            path: capture_path.parse().unwrap(),
            global_labels: options_global_labels.inner,
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
    (ops, config)
}

async fn inner_main(
    experiment_duration: Duration,
    warmup_duration: Duration,
    max_shutdown_delay: Duration,
    config: Config,
) {
    let shutdown = Shutdown::new();

    // Set up the telemetry sub-system.
    //
    // We support two methods to exflitrate telemetry about the target from rig:
    // a passive prometheus export and an active log file. Only one can be
    // active at a time.
    match config.telemetry {
        Telemetry::Prometheus {
            prometheus_addr,
            global_labels,
        } => {
            let mut builder = PrometheusBuilder::new().with_http_listener(prometheus_addr);
            for (k, v) in global_labels {
                builder = builder.add_global_label(k, v);
            }
            let _: () = builder.install().unwrap();
        }
        Telemetry::Log {
            path,
            global_labels,
        } => {
            let mut capture_manager = CaptureManager::new(path, shutdown.clone()).await;
            capture_manager.install();
            for (k, v) in global_labels {
                capture_manager.add_global_label(k, v);
            }
            let _capmgr = tokio::spawn(capture_manager.run());
        }
    }

    // Set up the application servers. These are, depending on configuration:
    //
    // * the "generator" which pushes load into
    // * the "target" which is the measured system and might push load into
    // * the "blackhole" which may or may not exist.

    let generator_server = generator::Server::new(config.generator, shutdown.clone()).unwrap();
    let _gsrv = tokio::spawn(generator_server.run());

    let target_server = target::Server::new(config.target.unwrap(), shutdown.clone()).unwrap();
    let tsrv = tokio::spawn(target_server.run());

    if let Some(blackhole_conf) = config.blackhole {
        let blackhole_server = blackhole::Server::new(blackhole_conf, shutdown.clone());
        let _bsrv = tokio::spawn(blackhole_server.run());
    }

    info!("target is running, now sleeping for warmup");
    sleep(warmup_duration).await;
    info!("warmup completed, collecting samples");

    let experiment_duration = sleep(experiment_duration);
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("received ctrl-c");
            shutdown.signal().unwrap();
        },
        _ = experiment_duration => {
            info!("experiment duration exceeded");
            shutdown.signal().unwrap();
        }
        tgt = tsrv => {
            error!("target shut down unexpectedly with {:?}", tgt);
            shutdown.signal().unwrap();
        }
    }
    info!(
        "Waiting for {} seconds for tasks to shutdown.",
        max_shutdown_delay.as_secs(),
    );
    shutdown.wait(max_shutdown_delay).await;
}

fn main() {
    tracing_subscriber::fmt::init();

    info!("Starting lading run.");
    let (opts, config): (Opts, Config) = get_config();
    let experiment_duration = Duration::from_secs(opts.experiment_duration_seconds.into());
    let warmup_duration = Duration::from_secs(opts.warmup_duration_seconds.into());
    // The maximum shutdown delay is shared between `inner_main` and this
    // function, hence the divide by two.
    let max_shutdown_delay = Duration::from_secs(opts.max_shutdown_delay.into()) / 2;

    let runtime = Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(inner_main(
        experiment_duration,
        warmup_duration,
        max_shutdown_delay,
        config,
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
}
