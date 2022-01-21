use argh::FromArgs;
use futures::stream::StreamExt;
use metrics::counter;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Deserialize;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use tokio_util::io::ReaderStream;

fn default_config_path() -> String {
    "/etc/lading/tcp_blackhole.yaml".to_string()
}

#[derive(FromArgs)]
/// `tcp_blackhole` options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option, default = "default_config_path()")]
    config_path: String,
}

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// number of worker threads to use in this program
    pub worker_threads: u16,
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
    /// Address and port for prometheus exporter
    pub prometheus_addr: SocketAddr,
}

struct Server {
    addr: SocketAddr,
    prom_addr: SocketAddr,
}

impl Server {
    fn new(addr: SocketAddr, prom_addr: SocketAddr) -> Self {
        Self { addr, prom_addr }
    }

    async fn handle_connection(socket: TcpStream) {
        let mut stream = ReaderStream::new(socket);

        while let Some(_) = stream.next().await {
            counter!("message_received", 1);
        }
    }

    async fn run(self) -> Result<(), io::Error> {
        let _: () = PrometheusBuilder::new()
            .listen_address(self.prom_addr)
            .install()
            .unwrap();

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

fn main() {
    let config: Config = get_config();
    let server = Server::new(config.binding_addr, config.prometheus_addr);
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .build()
        .unwrap();
    runtime.block_on(server.run()).unwrap();
}
