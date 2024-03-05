use std::{fmt, fs, path::PathBuf, process::Stdio, str};

use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Defines how sub-process stderr and stdout are handled.
pub struct Output {
    #[serde(default)]
    /// Determines how stderr is routed.
    pub stderr: Behavior,
    #[serde(default)]
    /// Determines how stderr is routed.
    pub stdout: Behavior,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
/// Defines the [`Output`] behavior for stderr and stdout.
pub enum Behavior {
    /// Redirect stdout, stderr to /dev/null
    Quiet,
    /// Write to a location on-disk.
    Log(PathBuf),
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

pub(crate) fn stdio(behavior: &Behavior) -> Stdio {
    match behavior {
        Behavior::Quiet => Stdio::null(),
        Behavior::Log(path) => {
            let fp = fs::File::create(path).unwrap_or_else(|_| {
                panic!(
                    "Full directory path does not exist: {path}",
                    path = path.display()
                );
            });
            Stdio::from(fp)
        }
    }
}

#[derive(Debug)]
pub(crate) struct PeekableReceiver<T> {
    receiver: mpsc::Receiver<T>,
    buffer: Option<T>,
}

impl<T> PeekableReceiver<T> {
    pub(crate) fn new(receiver: mpsc::Receiver<T>) -> Self {
        Self {
            receiver,
            buffer: None,
        }
    }

    #[inline]
    pub(crate) async fn next(&mut self) -> Option<T> {
        match self.buffer.take() {
            Some(t) => Some(t),
            None => self.receiver.recv().await,
        }
    }

    pub(crate) async fn peek(&mut self) -> Option<&T> {
        if self.buffer.is_none() {
            self.buffer = self.receiver.recv().await;
        }
        self.buffer.as_ref()
    }
}
