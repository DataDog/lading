//! The process tree generator.
//!
//! Unlike the other generators the process tree generator does not "connect" however
//! loosely to the target but instead, without coordination, merely generates
//! a process tree.
//!
//! ## Metrics
//!
//! This generator does not emit any metrics. Some metrics may be emitted by the
//! configured [throttle].
//!

use is_executable::IsExecutable;
use lading_throttle::Throttle;
use nix::{
    sys::wait::{waitpid, WaitPidFlag, WaitStatus},
    unistd::{fork, ForkResult, Pid},
};
use rand::{
    distributions::{Alphanumeric, DistString},
    rngs::StdRng,
    seq::SliceRandom,
};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::{vec_deque, VecDeque},
    env, error, fmt,
    iter::Peekable,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    process::{exit, Stdio},
    str, thread,
    time::Duration,
};
use tokio::process::Command;
use tracing::{error, info};

#[derive(Debug)]
/// Not executable
pub struct NotExecutable {
    executable: PathBuf,
}

impl error::Error for NotExecutable {}

impl fmt::Display for NotExecutable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} not executable", self.executable.display())
    }
}

#[derive(Debug)]
/// Execution error
pub struct ExecutionError {
    stderr: String,
}

impl error::Error for ExecutionError {}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "execution failed: {}", self.stderr)
    }
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`ProcessTree`].
pub enum Error {
    /// The file is not executable
    #[error("Not Executable!")]
    NotExecutable(#[from] NotExecutable),
    /// Wrapper around [`serde_yaml::Error`].
    #[error("Serialization failed with error: {0}")]
    Serialization(#[from] serde_yaml::Error),
    /// Wrapper around [`std::io::Error`].
    #[error("IO error: {0}")]
    Io(#[from] ::std::io::Error),
    /// Process tree command execution error
    #[error("Execution error: {0}")]
    ExecutionError(#[from] ExecutionError),
    /// The rng slice is empty
    #[error("Rng slice is empty")]
    RngEmpty,
    /// Error converting path to string
    #[error("Error converting path to string")]
    ToStr,
    /// Utf8 error
    #[error("Utf8 error: {0}")]
    Utf8(#[from] str::Utf8Error),
}

fn default_max_depth() -> NonZeroU32 {
    NonZeroU32::new(10).expect("default max depth given was 0")
}

fn default_max_tree_per_second() -> NonZeroU32 {
    NonZeroU32::new(5).expect("default max tree per second given was 0")
}

// default to 100ms
fn default_process_sleep_ns() -> NonZeroU32 {
    NonZeroU32::new(100_000_000).expect("default process sleep ns given was 0")
}

fn default_max_children() -> NonZeroU32 {
    NonZeroU32::new(10).expect("default max children given was 0")
}

fn default_args_len() -> NonZeroUsize {
    NonZeroUsize::new(10).expect("default args len given was 0")
}

fn default_args_count() -> NonZeroU32 {
    NonZeroU32::new(16).expect("default args count given was 0")
}

fn default_envs_len() -> NonZeroUsize {
    NonZeroUsize::new(16).expect("default envs len given was 0")
}

fn default_envs_count() -> NonZeroU32 {
    NonZeroU32::new(10).expect("default envs count given was 0")
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Args {
    /// Statically defined arguments
    Static(StaticArgs),
    /// Generated arguments
    Generate(GenerateArgs),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
pub struct StaticArgs {
    /// Argumments used with the `static` mode
    pub values: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
pub struct GenerateArgs {
    /// The maximum number argument per Process. Used by the `generate` mode
    #[serde(default = "default_args_len")]
    pub length: NonZeroUsize,
    /// The maximum number of arguments. Used by the `generate` mode
    #[serde(default = "default_args_count")]
    pub count: NonZeroU32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Envs {
    /// Statically defined environment variables
    Static(StaticEnvs),
    /// Generated environment variables
    Generate(GenerateEnvs),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
pub struct StaticEnvs {
    /// Environment variables used with the `static` mode
    pub values: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
pub struct GenerateEnvs {
    /// The maximum number environment variable per Process. Used by the `generate` mode
    #[serde(default = "default_envs_len")]
    pub length: NonZeroUsize,
    /// The maximum number of environment variable.  Used by the `generate` mode
    #[serde(default = "default_envs_count")]
    pub count: NonZeroU32,
}

impl StaticEnvs {
    #[must_use]
    /// return environment variables as a hashmap
    pub fn to_map(&self) -> FxHashMap<String, String> {
        let mut envs: FxHashMap<String, String> = FxHashMap::default();
        for env in &self.values {
            if let Some(kv) = env.split_once('=') {
                envs.insert(kv.0.to_string(), kv.1.to_string());
            } else {
                envs.insert(env.clone(), String::new());
            }
        }
        envs
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
pub struct Executable {
    /// Path of the executable
    pub executable: PathBuf,
    /// Command line arguments
    pub args: Args,
    /// Environment variables
    pub envs: Envs,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`ProcessTree`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The number of process created per second
    #[serde(default = "default_max_tree_per_second")]
    pub max_tree_per_second: NonZeroU32,
    /// The maximum depth of the process tree
    #[serde(default = "default_max_depth")]
    pub max_depth: NonZeroU32,
    /// The maximum children per level
    #[serde(default = "default_max_children")]
    pub max_children: NonZeroU32,
    /// Sleep applied at process start
    #[serde(default = "default_process_sleep_ns")]
    pub process_sleep_ns: NonZeroU32,
    /// List of executables
    pub executables: Vec<Executable>,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

impl Config {
    /// Validate the configuration
    ///
    /// # Errors
    ///
    /// Validation will fail if one executable path is not executable.
    pub fn validate(&self) -> Result<(), Error> {
        let iter = self.executables.iter();
        for exec in iter {
            if !exec.executable.is_executable() {
                return Err(Error::from(NotExecutable {
                    executable: exec.executable.clone(),
                }));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
/// The `ProcessTree` generator.
///
/// This generator generates a random `ProcessTree`,
/// this without coordination to the target.
pub struct ProcessTree {
    lading_path: PathBuf,
    config_content: String,
    throttle: Throttle,
    shutdown: lading_signal::Watcher,
}

impl ProcessTree {
    /// Create a new [`ProcessTree`]
    ///
    /// # Errors
    ///
    /// Return an error if the config can be serialized.
    ///
    pub fn new(config: &Config, shutdown: lading_signal::Watcher) -> Result<Self, Error> {
        let lading_path = match env::current_exe() {
            Ok(path) => path,
            Err(e) => return Err(Error::from(e)),
        };

        let _labels = [
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "process_tree".to_string()),
        ];

        let throttle = Throttle::new_with_config(config.throttle, config.max_tree_per_second);
        match serde_yaml::to_string(config) {
            Ok(serialized) => Ok(Self {
                lading_path,
                config_content: serialized,
                throttle,
                shutdown,
            }),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Run [`ProcessTree`] to completion or until a shutdown signal is received.
    ///
    /// In this loop the process tree will be generated.
    ///
    /// # Errors
    ///
    /// Return an error if the process tree generator command fails.
    ///
    /// # Panics
    ///
    /// Panic if the lading path can't determine.
    ///
    pub async fn spin(mut self) -> Result<(), Error> {
        let lading_path = self.lading_path.to_str().ok_or(Error::ToStr)?;

        loop {
            tokio::select! {
                _ = self.throttle.wait() => {
                    // using pid as target pid just to pass laging clap constraints
                    let output = Command::new(lading_path)
                        .args(["--target-pid", "1"])
                        .arg("process-tree-gen")
                        .arg("--config-content")
                        .arg(&self.config_content)
                        .stdin(Stdio::null())
                        .output().await?;

                    if !output.status.success() {
                        error!("process tree generator execution error");
                        return Err(Error::from(ExecutionError {
                            stderr: str::from_utf8(&output.stderr)?.to_string()
                        }));
                    }
                },

                () = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    break;
                },
            }
        }
        Ok(())
    }
}

#[inline]
fn rnd_str(rng: &mut StdRng, len: usize) -> String {
    Alphanumeric.sample_string(rng, len)
}

#[inline]
fn gen_rnd_args(rng: &mut StdRng, len: usize, max: u32) -> Vec<String> {
    let mut args = Vec::new();
    for _ in 0..max {
        args.push(rnd_str(rng, len));
    }
    args
}

#[inline]
fn gen_rnd_envs(rng: &mut StdRng, len: usize, max: u32) -> FxHashMap<String, String> {
    let key_size = len / 2;
    let value_size = len - key_size;

    let mut envs = FxHashMap::default();
    for _ in 0..max {
        let key = rnd_str(rng, key_size);
        let value = rnd_str(rng, value_size);
        envs.insert(key, value);
    }
    envs
}

/// Defines a execution of an executable with args and envs
#[derive(Debug)]
pub struct Exec {
    executable: String,
    args: Vec<String>,
    envs: FxHashMap<String, String>,
}

impl Exec {
    fn new(rng: &mut StdRng, config: &Config) -> Result<Self, Error> {
        let exec = config.executables.choose(rng).ok_or(Error::RngEmpty)?;

        let args = match &exec.args {
            Args::Static(params) => params.values.clone(),
            Args::Generate(params) => gen_rnd_args(rng, params.length.get(), params.count.get()),
        };

        let envs = match &exec.envs {
            Envs::Static(params) => params.to_map(),
            Envs::Generate(params) => gen_rnd_envs(rng, params.length.get(), params.count.get()),
        };

        Ok(Self {
            executable: exec.executable.to_str().ok_or(Error::ToStr)?.to_string(),
            args,
            envs,
        })
    }
}

/// Defines a process node
#[derive(Debug)]
pub struct Process {
    depth: u32,
    exec: Option<Exec>,
}

impl Process {
    fn new(depth: u32, exec: Option<Exec>) -> Self {
        Self { depth, exec }
    }
}

/// Spawn the process tree
///
/// # Errors
///
/// Function will error if the nodes list in incorrectly proccessed.
///
/// # Panics
///
/// Function will panic if the process execution fails.
///
pub fn spawn_tree(nodes: &VecDeque<Process>, sleep_ns: u32) -> Result<(), Error> {
    let mut iter = nodes.iter().peekable();
    let mut pids_to_wait: FxHashSet<Pid> = FxHashSet::default();
    let mut depth = 0;

    loop {
        try_wait_pid(&mut pids_to_wait);

        if iter.len() == 0 {
            if !pids_to_wait.is_empty() {
                continue;
            }

            // do not exit from the root node
            if depth > 0 {
                exit(0)
            }

            return Ok(());
        }

        let duration = Duration::from_nanos(sleep_ns.into());
        thread::sleep(duration);

        let process = iter.next().expect("process is not populated properly");

        if let Some(exec) = &process.exec {
            let status = std::process::Command::new(&exec.executable)
                .args(&exec.args)
                .envs(&exec.envs)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;
            exit(status.code().expect("failed to get exit code"));
        }

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child, .. }) => {
                pids_to_wait.insert(child);
                goto_next_sibling(process.depth, &mut iter);
            }
            Ok(ForkResult::Child) => {
                depth = process.depth;
                pids_to_wait.clear();
            }
            Err(_) => {}
        }
    }
}

#[inline]
fn try_wait_pid(pids: &mut FxHashSet<Pid>) {
    let mut exited: Option<Pid> = None;

    for pid in pids.iter() {
        match waitpid(*pid, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => {}
            Ok(_) | Err(_) => {
                exited = Some(*pid);
                break;
            }
        }
    }
    if let Some(pid) = exited {
        pids.remove(&pid);
    }
}

#[inline]
fn goto_next_sibling(depth: u32, iter: &mut Peekable<vec_deque::Iter<'_, Process>>) {
    while let Some(child) = iter.peek() {
        if child.depth == depth {
            break;
        }
        iter.next();
    }
}

/// Generate a process tree
///
/// # Errors
///
/// Return an error if the exec cannot be created
pub fn generate_tree(rng: &mut StdRng, config: &Config) -> Result<VecDeque<Process>, Error> {
    let mut nodes = VecDeque::new();
    let mut stack = Vec::new();

    stack.push(Process::new(1, None));

    while let Some(process) = stack.pop() {
        let curr_depth = process.depth;

        nodes.push_back(process);

        if curr_depth + 1 > config.max_depth.get() {
            let exec = Exec::new(rng, config)?;

            let process = Process::new(curr_depth + 1, Some(exec));
            nodes.push_back(process);
        } else {
            for _ in 0..config.max_children.get() {
                let process = Process::new(curr_depth + 1, None);
                stack.push(process);
            }
        }
    }

    Ok(nodes)
}

/// Parse the configuration of the process tree
///
/// # Errors
///
/// Return an error if the content is incorrect
///
pub fn get_config(content: &str) -> Result<Config, Error> {
    match serde_yaml::from_str::<Config>(content) {
        Ok(config) => {
            config.validate()?;
            Ok(config)
        }
        Err(e) => Err(Error::from(e)),
    }
}
