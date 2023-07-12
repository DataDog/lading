use std::{fmt, fs, path::PathBuf, process::Stdio, str};

use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
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
            let fp = fs::File::create(path).unwrap();
            Stdio::from(fp)
        }
    }
}
