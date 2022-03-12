use argh::FromArgs;
use futures::stream::StreamExt;
use lading_rig::config::{Config, Target};
use lading_rig::generator;
use metrics::counter;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::process::{ExitStatus, Stdio};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    runtime::Builder,
    signal,
    sync::{broadcast, mpsc},
};
use tokio_util::io::ReaderStream;
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

struct BlackholeServer {
    addr: SocketAddr,
}

impl BlackholeServer {
    fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    async fn handle_connection(socket: TcpStream) {
        let mut stream = ReaderStream::new(socket);

        while let Some(_) = stream.next().await {
            counter!("message_received", 1);
        }
    }

    async fn run(self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(self.addr).await.unwrap();

        loop {
            let (socket, _) = listener.accept().await?;
            counter!("connection_accepted", 1);
            tokio::spawn(async move {
                Self::handle_connection(socket).await;
            });
        }
    }
}

#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,

    /// The receive half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        // Remember that the signal has been received.
        self.shutdown = true;
    }
}

struct TargetServer {
    command: Command,
    shutdown: Shutdown,
}

impl TargetServer {
    fn new(config: Target, shutdown: Shutdown) -> Self {
        let mut command = Command::new(config.command);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env_clear()
            .kill_on_drop(true)
            .args(config.arguments)
            .envs(config.environment_variables.iter());
        Self { command, shutdown }
    }

    // TODO have actual return
    async fn run(mut self) -> Result<(), io::Error> {
        let mut child = self.command.spawn().unwrap();
        let wait = child.wait();
        tokio::select! {
            res = wait => {
                info!("child exited");
                res.unwrap();
            },
            _ = self.shutdown.recv() => {
                info!("shutdown signal received");
                child.kill().await.unwrap();
            }
            //        _ = shutdown_recv.recv() => {},
        }
        Ok(())
    }
}

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

    let generator_server = generator::Server::new(config.generator).unwrap();
    let gsrv = tokio::spawn(generator_server.spin());

    let target_server = TargetServer::new(config.target, Shutdown::new(shutdown_rcv));
    let tsrv = tokio::spawn(target_server.run());

    if let Some(blackhole) = config.blackhole {
        let blackhole_server = BlackholeServer::new(blackhole.binding_addr);
        let _bsrv = tokio::spawn(blackhole_server.run());
    }

    tokio::select! {
            _ = signal::ctrl_c() => {
                info!("received ctrl-c");
                shutdown_snd.send(()).unwrap();
            }
            ,
    //        _ = shutdown_recv.recv() => {},
        }

    tsrv.await.unwrap().unwrap();
    // bsrv.await.unwrap();
}

fn main() {
    tracing_subscriber::fmt().init();

    let config: Config = get_config();
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .build()
        .unwrap();
    runtime.block_on(inner_main(config));
}
