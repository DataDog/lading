use crate::{config::Target, signals::Shutdown};
use std::io;
use std::process::Stdio;
use tokio::process::Command;
use tracing::info;

pub struct Server {
    command: Command,
    shutdown: Shutdown,
}

impl Server {
    pub fn new(config: Target, shutdown: Shutdown) -> Self {
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
    pub async fn run(mut self) -> Result<(), io::Error> {
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
        }
        Ok(())
    }
}
