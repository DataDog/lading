use crate::signals::Shutdown;
use nix::{
    errno::Errno,
    sys::signal::{kill, SIGTERM},
    unistd::Pid,
};
use serde::Deserialize;
use std::{
    collections::HashMap, env, fmt, fs, io, path::PathBuf, process::Stdio, str, str::FromStr,
};
use tokio::process::Command;
use tracing::{error, info};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Env(env::VarError),
    Errno(Errno),
}

#[derive(Debug)]
pub struct Config {
    pub command: Cmd,
    pub arguments: Vec<String>,
    pub environment_variables: HashMap<String, String>,
    pub output: Output,
}

#[derive(Debug)]
pub enum Cmd {
    Path(String),
    Perf {
        inner_command: String,
        perf_data_path: String,
    },
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
    config: Config,
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
        Ok(Self { config, shutdown })
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
        let config = self.config;
        // We setup potentially two child processes here. We always set up a
        // 'target' sub-process to run load through. Additionally we have a
        // 'perf' sub-process that examines the 'target' when the user requests
        // it. Linux perf is very sensitive to how it's shut down and when it
        // exists we SIGTERM the child, then let perf naturally shut down, which
        // can take a non-trivial amount of time.
        let (mut target_child, perf_child) = match config.command {
            Cmd::Path(p) => {
                let mut target_cmd = Command::new(p);
                target_cmd
                    .stdin(Stdio::null())
                    .stdout(stdio(&config.output.stdout))
                    .stderr(stdio(&config.output.stderr))
                    .env_clear()
                    .kill_on_drop(true)
                    .args(config.arguments)
                    .envs(config.environment_variables.iter());
                let target_child = target_cmd.spawn().map_err(Error::Io)?;
                (target_child, None)
            }
            Cmd::Perf {
                inner_command,
                perf_data_path,
            } => {
                let mut target_cmd = Command::new(inner_command);
                target_cmd
                    .stdin(Stdio::null())
                    .stdout(stdio(&config.output.stdout))
                    .stderr(stdio(&config.output.stderr))
                    .env_clear()
                    .kill_on_drop(true)
                    .args(config.arguments)
                    .envs(config.environment_variables.iter());
                let target_child = target_cmd.spawn().map_err(Error::Io)?;

                let mut perf_cmd = Command::new("perf");
                let args = vec![
                    "record".into(),
                    "--verbose".into(),
                    "--compression-level=22".into(),
                    "--call-graph=dwarf".into(),
                    format!("--pid={}", target_child.id().unwrap()),
                    format!("--output={}", perf_data_path),
                ];
                perf_cmd
                    .stdin(Stdio::null())
                    .stdout(stdio(
                        &Behavior::from_str("/tmp/captures/perf.stdout.log").unwrap(),
                    ))
                    .stderr(stdio(
                        &Behavior::from_str("/tmp/captures/perf.stderr.log").unwrap(),
                    ))
                    .env_clear()
                    .kill_on_drop(true)
                    .args(args)
                    .envs(config.environment_variables.iter());
                let perf_child = perf_cmd.spawn().map_err(Error::Io)?;
                (target_child, Some(perf_child))
            }
        };

        let target_wait = target_child.wait();
        tokio::select! {
            res = target_wait => {
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
                // Note that `Child::kill` sends SIGKILL which is not what we
                // want. We instead send SIGTERM so that the child has a chance
                // to clean up.
                let pid: Pid = Pid::from_raw(target_child.id().unwrap().try_into().unwrap());
                kill(pid, SIGTERM).map_err(Error::Errno)?;
                target_child.wait().await.map_err(Error::Io)?;
                if let Some(mut perf_child) = perf_child {
                    perf_child.wait().await.map_err(Error::Io)?;
                }
            }
        }
        Ok(())
    }
}
