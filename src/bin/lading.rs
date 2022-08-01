use std::{
    collections::HashMap,
    fmt::{self, Display},
    io::Read,
    num::NonZeroU32,
    path::PathBuf,
    str::FromStr,
};

use clap::Parser;
use lading::{
    blackhole,
    captures::CaptureManager,
    config::{self, Config, Target, Telemetry},
    external_target, generator, inspector, observer,
    signals::Shutdown,
    target::{self, Behavior, Output},
};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{
    runtime::Builder,
    signal,
    sync::broadcast,
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn};

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
    /// measure an externally-launched process by PID
    external_target_pid: Option<NonZeroU32>,
    /// additional environment variables to apply to the target, format
    /// KEY=VAL,KEY2=VAL
    #[clap(long)]
    target_environment_variables: Option<CliKeyValues>,
    /// the path of the target executable
    target_path: Option<PathBuf>,
    /// arguments for the target executable
    target_arguments: Vec<String>,
    /// the path to write target's stdout
    #[clap(long, default_value_t = default_target_behavior())]
    target_stdout_path: Behavior,
    /// the path to write target's stderr
    #[clap(long, default_value_t = default_target_behavior())]
    target_stderr_path: Behavior,
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
    /// the time, in seconds, to allow the target to run without collecting
    /// samples
    #[clap(long, default_value_t = 30)]
    warmup_duration_seconds: u32,
    /// whether to ignore inspector configuration, if present, and not run the inspector
    #[clap(long)]
    disable_inspector: bool,
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

    if ops.external_target_pid.is_some() && ops.target_path.is_some() {
        error!(
            "Conflicting target configuration: target path and external target PID cannot be used together"
        );
        std::process::exit(-1);
    }
    let target = if let Some(pid) = ops.external_target_pid {
        Target::PID(external_target::Config { pid })
    } else if let Some(path) = &ops.target_path {
        let target_config = target::Config {
            command: path.clone(),
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
        Target::Binary(target_config)
    } else {
        error!("No target specified");
        std::process::exit(-1);
    };
    config.target = Some(target);

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
    disable_inspector: bool,
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
            builder.install().unwrap();
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
    //
    // There is also, maybe:
    //
    // * the "inspector" which is a sub-process that users can rig to inspect
    //   the target.
    // * the "observer" which reads procfs on Linux and reports relevant process
    //   detail to the capture log

    let (tgt_snd, _) = broadcast::channel(1);

    //
    // GENERATOR
    //
    match config.generator {
        config::Generator::One(cfg) => {
            let tgt_rcv = tgt_snd.subscribe();
            let generator_server = generator::Server::new(*cfg, shutdown.clone()).unwrap();
            let _gsrv = tokio::spawn(generator_server.run(tgt_rcv));
        }
        config::Generator::Many(cfgs) => {
            for cfg in cfgs {
                let tgt_rcv = tgt_snd.subscribe();
                let generator_server = generator::Server::new(cfg, shutdown.clone()).unwrap();
                let _gsrv = tokio::spawn(generator_server.run(tgt_rcv));
            }
        }
    }

    //
    // INSPECTOR
    //
    if let Some(inspector_conf) = config.inspector {
        if !disable_inspector {
            let tgt_rcv = tgt_snd.subscribe();
            let inspector_server =
                inspector::Server::new(inspector_conf, shutdown.clone()).unwrap();
            let _isrv = tokio::spawn(inspector_server.run(tgt_rcv));
        }
    }

    //
    // BLACKHOLE
    //
    match config.blackhole {
        Some(config::Blackhole::One(cfg)) => {
            let blackhole_server = blackhole::Server::new(*cfg, shutdown.clone());
            let _bsrv = tokio::spawn(async {
                match blackhole_server.run().await {
                    Ok(()) => debug!("blackhole shut down successfully"),
                    Err(err) => warn!("blackhole failed with {:?}", err),
                }
            });
        }
        Some(config::Blackhole::Many(cfgs)) => {
            for cfg in cfgs {
                let blackhole_server = blackhole::Server::new(cfg, shutdown.clone());
                let _bsrv = tokio::spawn(async {
                    match blackhole_server.run().await {
                        Ok(()) => debug!("blackhole shut down successfully"),
                        Err(err) => warn!("blackhole failed with {:?}", err),
                    }
                });
            }
        }
        None => {}
    }

    //
    // OBSERVER
    //
    let obs_rcv = tgt_snd.subscribe();
    let observer_server = observer::Server::new(config.observer, shutdown.clone()).unwrap();
    let _osrv = tokio::spawn(observer_server.run(obs_rcv));

    let target_fut = async {
        match config.target.unwrap() {
            Target::Binary(cfg) => {
                let target_server = target::Server::new(cfg, shutdown.clone()).unwrap();
                let tsrv = tokio::spawn(target_server.run(tgt_snd));
                let res = tsrv.await;
                match res {
                    Ok(Ok(status)) => info!("process exited with {:?}", status),
                    Ok(Err(e)) => error!("target shut down with {:?}", e),
                    Err(e) => error!("internal error: {:?}", e),
                }
            }
            Target::PID(cfg) => {
                let target_server = external_target::Server::new(cfg, shutdown.clone());
                let tsrv = tokio::spawn(target_server.run(tgt_snd));
                let res = tsrv.await;
                if let Ok(Err(e)) = res {
                    error!("pid target shut down with {:?}", e);
                }
            }
        }
    };

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
        _ = target_fut => {
            error!("target shut down unexpectedly");
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
    let disable_inspector = opts.disable_inspector;

    let runtime = Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(inner_main(
        experiment_duration,
        warmup_duration,
        max_shutdown_delay,
        disable_inspector,
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
