use argh::FromArgs;
use lading_rig::{blackhole, config::Config, generator, signals::Shutdown, target};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::io::Read;
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

#[derive(FromArgs)]
/// `rig` options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option, default = "default_config_path()")]
    config_path: String,
}

fn get_config() -> Config {
    let ops: Opts = argh::from_env();
    let mut file: std::fs::File = std::fs::OpenOptions::new()
        .read(true)
        .open(ops.config_path)
        .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    serde_yaml::from_str(&contents).unwrap()
}

// TODO log captures out to a file
// TODO log target stdout to another file
// TODO introduce 'replica' notion

async fn inner_main(config: Config) {
    // TODO we won't be using prometheus but it's a useful debug tool for now
    let _: () = PrometheusBuilder::new()
        .listen_address(config.prometheus_addr)
        .install()
        .unwrap();
    let (shutdown_snd, shutdown_rcv) = broadcast::channel(1);
    //    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    // Set up the application servers. These are, depending on configuration:
    //
    // * the "generator" which pushes load into
    // * the "target" which is the measured system and might push load into
    // * the "blackhole" which may or may not exist.

    let generator_server =
        generator::Server::new(config.generator, Shutdown::new(shutdown_rcv)).unwrap();
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

    while shutdown_snd.receiver_count() != 0 {
        sleep(Duration::from_millis(750)).await;
        info!("waiting for shutdown");
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
