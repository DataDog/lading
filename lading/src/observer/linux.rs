//! Linux observer
//!
//! On Linux, this observer looks up the target process in procfs and writes
//! out key details about memory and CPU consumption into the capture data. On
//! non-Linux systems the observer, if enabled, will emit a warning.

mod cgroup;
mod procfs;
mod utils;
mod wss;

use serde::Deserialize;
use tracing::{error, warn};

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configuration for Linux observer
pub struct Config {}

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub enum Error {
    /// Wrapper for [`cgroup::Error`]
    #[error("Cgroup: {0}")]
    CGroup(#[from] cgroup::Error),
    /// Wrapper for [`procfs::Error`]
    #[error("Procfs: {0}")]
    Procfs(#[from] procfs::Error),
    /// Wrapper for [`wss::Error`]
    #[error("WSS: {0}")]
    Wss(#[from] wss::Error),
}

#[derive(Debug)]
pub(crate) struct Sampler {
    procfs: procfs::Sampler,
    cgroup: cgroup::Sampler,
    wss: Option<wss::Sampler>,
    tick_counter: u8,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32, labels: Vec<(String, String)>) -> Result<Self, Error> {
        let procfs_sampler = procfs::Sampler::new(parent_pid)?;
        let cgroup_sampler = cgroup::Sampler::new(parent_pid, labels)?;
        let wss_sampler = if wss::Sampler::is_available() {
            Some(wss::Sampler::new(parent_pid)?)
        } else {
            warn!(
                r"{} isn’t accessible.
Either the kernel hasn’t been compiled with CONFIG_IDLE_PAGE_TRACKING
or the process doesn’t have access to it.
WSS sampling is not available.

Kernel support can be checked with
```
grep CONFIG_IDLE_PAGE_TRACKING /boot/config-$(uname -r)
```

Permissions can be checked with
```
id
ls -l /sys/kernel/mm/page_idle/bitmap
```
",
                wss::PAGE_IDLE_BITMAP
            );
            None
        };

        Ok(Self {
            procfs: procfs_sampler,
            cgroup: cgroup_sampler,
            wss: wss_sampler,
            tick_counter: 0,
        })
    }

    pub(crate) async fn sample(&mut self) -> Result<(), Error> {
        let sample_smaps = self.tick_counter.is_multiple_of(10);
        let sample_wss = self.tick_counter.is_multiple_of(60);
        self.tick_counter += 1;
        if self.tick_counter == 60 {
            self.tick_counter = 0;
        }

        self.procfs.poll(sample_smaps).await?;
        self.cgroup.poll().await?;

        if let Some(wss) = &mut self.wss {
            // WSS measures the amount of memory that has been accessed since the last poll.
            // As a consequence, the poll interval impacts the measure.
            // That’s why we need to be sure we don’t poll more often than once per minute.
            if sample_wss {
                wss.poll().await?;
            }
        }

        Ok(())
    }
}
