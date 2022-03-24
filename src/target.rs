use crate::signals::Shutdown;
use serde::Deserialize;
use std::{collections::HashMap, env, fmt, fs, io, path::PathBuf, process::Stdio, str};
use tokio::process::Command;
use tracing::{error, info};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Env(env::VarError),
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub command: Cmd,
    pub arguments: Vec<String>,
    pub environment_variables: HashMap<String, String>,
    pub output: Output,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cmd {
    Path(String),
    EnvironmentVariable(String),
}

impl Default for Cmd {
    fn default() -> Self {
        Self::EnvironmentVariable(String::from("LADING_TARGET"))
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum Behavior {
    /// Redirect stdout, stderr to /dev/null
    Quiet,
    Log(
        /// Location to write stdio/stderr
        PathBuf,
    ),
}

impl Default for Behavior {
    fn default() -> Self {
        Self::Quiet
    }
}

impl fmt::Display for Behavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Behavior::Quiet => write!(f, "/dev/null")?,
            Behavior::Log(ref path) => write!(f, "{}", path.display())?,
        }
        Ok(())
    }
}

impl str::FromStr for Behavior {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut path = PathBuf::new();
        path.push(input);
        Ok(Behavior::Log(path))
    }
}

#[derive(Debug, Deserialize)]
pub struct Output {
    #[serde(default)]
    pub stderr: Behavior,
    #[serde(default)]
    pub stdout: Behavior,
}

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
    /// Create a new [`Server`] instance
    ///
    /// The target `Server` is responsible for managing the sub-process under
    /// examination.
    ///
    /// # Errors
    ///
    /// Function will error if the path to the sub-process is not valid or if
    /// the path is valid but is not to file executable by this program.
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let path = match config.command {
            Cmd::Path(p) => p,
            Cmd::EnvironmentVariable(e) => env::var(e).map_err(Error::Env)?,
        };
        let mut command = Command::new(path);
        command
            .stdin(Stdio::null())
            .stdout(stdio(&config.output.stdout))
            .stderr(stdio(&config.output.stderr))
            .env_clear()
            .kill_on_drop(true)
            .args(config.arguments)
            .envs(config.environment_variables.iter());
        Ok(Self { command, shutdown })
    }

    /// Run this [`Server`] to completion
    ///
    /// This function runs the user supplied process to its completion, or until
    /// a shutdown signal is received. Child exit status does not currently
    /// propagate. This is less than ideal.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying process cannot be waited
    /// on or will not shutdown when signaled to.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(mut self) -> Result<(), Error> {
        let mut child = self.command.spawn().map_err(Error::Io)?;
        let wait = child.wait();
        tokio::select! {
            res = wait => {
                match res {
                    Ok(status) => {
                        error!("child exited with status: {}", status);
                    }
                    Err(err) => {
                        error!("child exited with error: {}", err);
                        return Err(Error::Io(err));
                    }
                }
            },
            _ = self.shutdown.recv() => {
                info!("shutdown signal received");
                child.kill().await.map_err(Error::Io)?;
            }
        }
        Ok(())
    }
}
