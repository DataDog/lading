use futures::stream::{FuturesUnordered, StreamExt};
use std::path::Path;
use tokio::runtime::Builder;
use tokio::time;

use file_gen::HELLO;

async fn file_writer<P: AsRef<Path>>(path: P) {
    if let Some(p) = path.as_ref().to_str() {
        let mut interval = time::interval(time::Duration::from_millis(1_000));

        loop {
            interval.tick().await;
            println!("{} goes brrrr...", p);
        }
    }
}

async fn run() {
    let mut workers = FuturesUnordered::new();

    workers.push(file_writer(Path::new("./foo/bar.txt")));

    loop {
        match workers.next().await {
            Some(
        }
    }
}

fn main() {
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();

    runtime.block_on(run());
}
