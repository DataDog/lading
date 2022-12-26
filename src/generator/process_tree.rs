//! The process tree generator.
//!
//! Unlike the other generators the process tree generator does not "connect" however
//! losely to the target but instead, without coordination, merely generates
//! a process tree.

use nix::{
    sys::wait::{waitpid, WaitPidFlag, WaitStatus},
    unistd::{fork, ForkResult, Pid},
};
use rand::{
    distributions::{Alphanumeric, DistString},
    seq::SliceRandom,
};
use std::{
    collections::{vec_deque, HashMap, HashSet, VecDeque},
    iter::Peekable,
    num::{NonZeroU32, NonZeroUsize},
    process::{exit, Command, Stdio},
    str, thread, time,
};

use governor::{Quota, RateLimiter};
use rand::{prelude::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug)]
/// Errors produced by [`ProcessTree`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    Io(::std::io::Error),
}

impl From<::std::io::Error> for Error {
    fn from(error: ::std::io::Error) -> Self {
        Error::Io(error)
    }
}

fn default_max_depth() -> NonZeroU32 {
    NonZeroU32::new(10).unwrap()
}

fn default_max_tree_per_second() -> NonZeroU32 {
    NonZeroU32::new(5).unwrap()
}

// default to 100ms
fn default_process_sleep_ns() -> NonZeroU32 {
    NonZeroU32::new(100_000_000).unwrap()
}

fn default_max_children() -> NonZeroU32 {
    NonZeroU32::new(10).unwrap()
}

fn default_args_len() -> NonZeroUsize {
    NonZeroUsize::new(10).unwrap()
}

fn default_args_count() -> NonZeroU32 {
    NonZeroU32::new(16).unwrap()
}

fn default_envs_len() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn default_envs_count() -> NonZeroU32 {
    NonZeroU32::new(10).unwrap()
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
/// Configuration of [`ProcessTree`]
pub struct Args {
    /// The mode, can be static or generate
    pub mode: String,
    /// Argumments used with the `static` mode
    #[serde(default)]
    pub values: Vec<String>,
    /// The maximum number argument per Process. Used by the `generate` mode
    #[serde(default = "default_args_len")]
    pub length: NonZeroUsize,
    /// The maximum number of arguments. Used by the `generate` mode
    #[serde(default = "default_args_count")]
    pub count: NonZeroU32,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
/// Configuration of [`ProcessTree`]
pub struct Envs {
    /// The mode, can be static or generate
    pub mode: String,
    /// Environment variables used with the `static` mode
    #[serde(default)]
    pub values: Vec<String>,
    /// The maximum number environment variable per Process. Used by the `generate` mode
    #[serde(default = "default_envs_len")]
    pub length: NonZeroUsize,
    /// The maximum number of environment variable.  Used by the `generate` mode
    #[serde(default = "default_envs_count")]
    pub count: NonZeroU32,
}

impl Envs {
    fn to_hash(&self) -> HashMap<String, String> {
        let mut envs: HashMap<String, String> = HashMap::new();
        for env in &self.values {
            let mut iter = env.split('=');
            let key = iter.next().unwrap().to_string();
            let value = iter.next().unwrap().to_string();
            envs.insert(key, value);
        }
        envs
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
/// Configuration of [`ProcessTree`]
pub struct Executable {
    /// Path of the executable
    pub executable: String,
    /// Command line arguments
    pub args: Args,
    /// Environment variables
    pub envs: Envs,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
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
fn gen_rnd_envs(rng: &mut StdRng, len: usize, max: u32) -> HashMap<String, String> {
    let key_size = len / 2;
    let value_size = len - key_size;

    let mut envs = HashMap::new();
    for _ in 0..max {
        let key = rnd_str(rng, key_size);
        let value = rnd_str(rng, value_size);
        envs.insert(key, value);
    }
    envs
}

#[derive(Debug, Serialize, Deserialize)]
struct Process {
    executable: String,
    args: Vec<String>,
    envs: HashMap<String, String>,
    depth: u32,
}

impl Process {
    fn new(depth: u32) -> Self {
        Self {
            executable: String::new(),
            args: Vec::new(),
            envs: HashMap::new(),
            depth,
        }
    }

    fn new_exec(rng: &mut StdRng, config: &Config, depth: u32) -> Self {
        let exec = config.executables.choose(rng).unwrap();

        let mut args = exec.args.values.clone();
        if exec.args.mode == "generate" {
            args = gen_rnd_args(rng, exec.args.length.get(), exec.args.count.get());
        }

        let mut envs = exec.envs.to_hash();
        if exec.envs.mode == "generate" {
            envs = gen_rnd_envs(rng, exec.envs.length.get(), exec.envs.count.get());
        }

        Self {
            executable: exec.executable.to_string(),
            args,
            envs,
            depth,
        }
    }

    fn is_exec(&self) -> bool {
        !self.executable.is_empty()
    }
}

#[derive(Debug)]
/// The `ProcessTree` generator.
///
/// This generator generates a random `ProcessTree`,
/// this without coordination to the target.
pub struct ProcessTree {
    nodes: VecDeque<Process>,
    process_sleep_ns: NonZeroU32,
    max_tree_per_second: NonZeroU32,
    shutdown: Shutdown,
}

impl ProcessTree {
    /// Create a new [`ProcessTree`]
    ///
    /// # Errors
    ///
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let nodes = generate_tree(&mut rng, config);

        Ok(Self {
            nodes,
            process_sleep_ns: config.process_sleep_ns,
            max_tree_per_second: config.max_tree_per_second,
            shutdown,
        })
    }

    /// Run [`ProcessTree`] to completion or until a shutdown signal is received.
    ///
    /// In this loop the process tree will be generated.
    ///
    /// # Errors
    ///
    /// # Panics
    ///
    pub async fn spin(mut self) -> Result<(), Error> {
        let process_rate_limiter = RateLimiter::direct(Quota::per_second(self.max_tree_per_second));

        loop {
            tokio::select! {
                _ = process_rate_limiter.until_ready() => {
                    spawn_tree(&self.nodes, self.process_sleep_ns.get());
                },
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    break;
                },
            }
        }
        Ok(())
    }
}

fn spawn_tree(nodes: &VecDeque<Process>, sleep_ns: u32) {
    let mut iter = nodes.iter().peekable();
    let mut pids_to_wait: HashSet<Pid> = HashSet::new();
    let mut depth = 0;

    loop {
        if iter.len() == 0 {
            try_wait_pid(&mut pids_to_wait);
            if !pids_to_wait.is_empty() {
                continue;
            }

            // do not exit from the root node
            if depth > 0 {
                exit(0)
            }

            return;
        }

        let duration = time::Duration::from_nanos(sleep_ns.into());
        thread::sleep(duration);

        let process = iter.next().unwrap();

        if process.is_exec() {
            let status = Command::new(&process.executable)
                .args(&process.args)
                .envs(&process.envs)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .ok()
                .unwrap();
            exit(status.code().unwrap())
        }

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child, .. }) => {
                pids_to_wait.insert(child);
                goto_next_sibling(process, &mut iter);
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
fn try_wait_pid(pids: &mut HashSet<Pid>) {
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
fn goto_next_sibling(process: &Process, iter: &mut Peekable<vec_deque::Iter<'_, Process>>) {
    while let Some(child) = iter.peek() {
        if child.depth == process.depth {
            break;
        }
        iter.next();
    }
}

fn generate_tree(rng: &mut StdRng, config: &Config) -> VecDeque<Process> {
    let mut nodes = VecDeque::new();
    let mut stack = Vec::new();

    stack.push(Process::new(1));

    while let Some(process) = stack.pop() {
        let curr_depth = process.depth;

        nodes.push_back(process);

        if curr_depth + 1 > config.max_depth.get() {
            let process = Process::new_exec(rng, config, curr_depth + 1);
            nodes.push_back(process);
        } else {
            for _ in 0..config.max_children.get() {
                let process = Process::new(curr_depth + 1);
                stack.push(process);
            }
        }
    }

    nodes
}
