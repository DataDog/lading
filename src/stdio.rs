use std::{fmt, path::PathBuf, process::Stdio, str};

use serde::Deserialize;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::{ChildStderr, ChildStdout},
    task::JoinHandle,
};
use tracing::info;

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

/// Errors produced by [`Behavior`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Unable to create file
    #[error("Unable to create {0}: {1}")]
    CreateLogFile(PathBuf, std::io::Error),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
/// Defines the [`Output`] behavior for stderr and stdout.
pub enum Behavior {
    /// Redirect stdout, stderr to /dev/null
    Quiet,
    /// Write to lading logs & a location on-disk.
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

impl Behavior {
    pub(crate) fn stdio(&self) -> Stdio {
        match self {
            Behavior::Quiet => Stdio::null(),
            Behavior::Log(_path) => Stdio::piped(),
        }
    }

    /// Handle child stdio stream. The returned value is a task handle that
    /// should be kept alive for as long as stream handling is desired. Dropping
    /// this handle will abort the stream handling task.
    ///
    /// For values of `Behavior::Log`, this function will write the given stream
    /// to a file and echo it to lading's logs.
    ///
    /// Values of `Behavior::Quiet` will neither write nor echo the stream.
    pub(crate) async fn spin<R: AsyncRead + Send + Unpin + 'static>(
        &self,
        stream: R,
        name: &'static str,
    ) -> Result<StdioHandle<()>, Error> {
        let mut file = match &self {
            Behavior::Quiet => return Ok(StdioHandle { _inner: None }),
            Behavior::Log(path) => {
                let file = File::create(path)
                    .await
                    .map_err(|e| Error::CreateLogFile(path.to_owned(), e))?;
                file
            }
        };

        let mut stdout = BufReader::new(stream).lines();

        let fwd_task = tokio::spawn(async move {
            while let Ok(Some(output)) = stdout.next_line().await {
                info!(output, name);

                // Ignore io errors: there's nothing we can do to surface them
                // from here. Keeping this task going allows the tracing output
                // to continue.
                let _ = file.write_all(output.as_bytes()).await;
                let _ = file.write_all("\n".as_bytes()).await;
            }
            let _ = file.flush();
        });

        Ok(StdioHandle {
            _inner: Some(fwd_task),
        })
    }
}

/// The stdio forwarder will run until this handle is dropped
pub(crate) struct StdioHandle<T> {
    _inner: Option<JoinHandle<T>>,
}

impl<T> Drop for StdioHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self._inner {
            handle.abort();
        }
    }
}
