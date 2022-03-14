use crate::{
    config::{Behavior, Target},
    signals::Shutdown,
};
use std::{fs, io, process::Stdio};
use tokio::process::Command;
use tracing::info;

pub struct Server {
    command: Command,
    shutdown: Shutdown,
}

fn stdio(behavior: &Behavior) -> Stdio {
    match behavior {
        Behavior::Quiet => Stdio::null(),
        Behavior::Log(path) => {
            let fp = fs::File::create(path).unwrap();
            Stdio::from(fp)
        }
    }
}

impl Server {
    pub fn new(config: Target, shutdown: Shutdown) -> Self {
        let mut command = Command::new(config.command);
        command
            .stdin(Stdio::null())
            .stdout(stdio(&config.output.stdout))
            .stderr(stdio(&config.output.stderr))
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
