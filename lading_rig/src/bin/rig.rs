use argh::{FromArgValue, FromArgs};
use lading_rig::{
    blackhole,
    captures::CaptureManager,
    config::{Config, Telemetry},
    generator,
    signals::Shutdown,
    target,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::{collections::HashMap, io::Read};
use tokio::{
    runtime::Builder,
    signal,
    sync::broadcast,
    time::{sleep, Duration},
};
use tracing::info;

fn default_config_path() -> String {
    "/etc/lading/rig.yaml".to_string()
}

struct CliLabels {
    inner: HashMap<String, String>,
}

impl FromArgValue for CliLabels {
    fn from_arg_value(input: &str) -> Result<Self, String> {
        let pair_err = String::from("pairs must be separated by '='");
        let mut labels = HashMap::new();
        for kv in input.split(',') {
            let mut pair = kv.split('=');
            let key = pair.next().ok_or(pair_err.clone())?;
            let value = pair.next().ok_or(pair_err.clone())?;
            labels.insert(key.into(), value.into());
        }
        Ok(Self { inner: labels })
    }
}

#[derive(FromArgs)]
/// `rig` options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option, default = "default_config_path()")]
    config_path: String,
    /// additional labels to apply to all captures, format KEY=VAL,KEY2=VAL
    #[argh(option)]
    global_labels: CliLabels,
}

fn get_config() -> Config {
    let ops: Opts = argh::from_env();
    let mut file: std::fs::File = std::fs::OpenOptions::new()
        .read(true)
        .open(ops.config_path)
        .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let mut config: Config = serde_yaml::from_str(&contents).unwrap();
    match config.telemetry {
        Telemetry::Prometheus {
            ref mut global_labels,
            ..
        } => {
            for (k, v) in ops.global_labels.inner {
                global_labels.insert(k.into(), v.into());
            }
        }
        Telemetry::Log {
            ref mut global_labels,
            ..
        } => {
            for (k, v) in ops.global_labels.inner {
                global_labels.insert(k.into(), v.into());
            }
        }
    }
    return config;
}

// TODO log target stdout to another file
// TODO require healthcheck for target, use k8s approach?

async fn inner_main(config: Config) {
    let (shutdown_snd, shutdown_rcv) = broadcast::channel(1);

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
            let mut capture_manager =
                CaptureManager::new(path, Shutdown::new(shutdown_snd.subscribe())).await;
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

    let generator_server =
        generator::Server::new(config.generator, Shutdown::new(shutdown_snd.subscribe())).unwrap();
    let _gsrv = tokio::spawn(generator_server.spin());

    let target_server = target::Server::new(config.target, Shutdown::new(shutdown_snd.subscribe()));
    let tsrv = tokio::spawn(target_server.run());

    if let Some(blackhole) = config.blackhole {
        let blackhole_server = blackhole::Server::new(
            blackhole.binding_addr,
            Shutdown::new(shutdown_snd.subscribe()),
        );
        let _bsrv = tokio::spawn(blackhole_server.run());
    }

    // Tidy up our stray shutdown_rcv, avoiding a situation where we infinitely
    // wait to shut down.
    drop(shutdown_rcv);
    let experiment_duration = sleep(Duration::from_secs(config.experiment_duration.into()));

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("received ctrl-c");
            shutdown_snd.send(()).unwrap();
        },
        _ = experiment_duration => {
            info!("experiment duration exceeded");
            shutdown_snd.send(()).unwrap();
        }
        tgt = tsrv => {
            info!("{:?}", tgt);
            shutdown_snd.send(()).unwrap();
        }
    }

    loop {
        let remaining: usize = shutdown_snd.receiver_count();
        if remaining != 0 {
            sleep(Duration::from_millis(750)).await;
            info!("waiting for {} tasks to shutdown", remaining);
        } else {
            break;
        }
    }
}

fn main() {
    tracing_subscriber::fmt().init();

    let config: Config = get_config();
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(inner_main(config));
}
