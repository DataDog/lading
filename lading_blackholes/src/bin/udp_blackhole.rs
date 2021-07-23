use argh::FromArgs;
use metrics::counter;
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::runtime::Builder;

#[derive(FromArgs)]
/// `udp_blackhole` options
struct Opts {
    /// number of worker threads to use in this program
    #[argh(option)]
    pub worker_threads: u16,
    /// address -- IP plus port -- to bind to
    #[argh(option)]
    pub binding_addr: SocketAddr,
}

struct Server {
    addr: SocketAddr,
}

impl Server {
    fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    async fn run(self) -> Result<(), io::Error> {
        let socket = UdpSocket::bind(&self.addr).await?;
        let mut buf: Vec<u8> = vec![0; 4096];

        loop {
            counter!("packet_received", 1);
            let _ = socket.recv_from(&mut buf).await?;
        }
    }
}

fn main() {
    let ops: Opts = argh::from_env();
    let server = Server::new(ops.binding_addr);
    let runtime = Builder::new_multi_thread()
        .worker_threads(ops.worker_threads as usize)
        .enable_io()
        .build()
        .unwrap();
    runtime.block_on(server.run()).unwrap();
}
