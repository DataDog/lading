mod cgroup;
mod procfs;
mod wss;

use tracing::{error, warn};

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
    smaps_interval: u8,
    last_wss_sample: tokio::time::Instant,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32, labels: Vec<(String, String)>) -> Result<Self, Error> {
        let procfs_sampler = procfs::Sampler::new(parent_pid)?;
        let cgroup_sampler = cgroup::Sampler::new(parent_pid, labels)?;
        let wss_sampler = if wss::Sampler::is_available() {
            Some(wss::Sampler::new(parent_pid)?)
        } else {
            warn!(
                "{} isn’t accessible. Either the kernel hasn’t been compiled with CONFIG_IDLE_PAGE_TRACKING or the process doesn’t have access to it. WSS sampling is not available",
                wss::PAGE_IDLE_BITMAP
            );
            None
        };

        Ok(Self {
            procfs: procfs_sampler,
            cgroup: cgroup_sampler,
            wss: wss_sampler,
            smaps_interval: 10,
            last_wss_sample: tokio::time::Instant::now() - tokio::time::Duration::from_secs(61),
        })
    }

    pub(crate) async fn sample(&mut self) -> Result<(), Error> {
        self.smaps_interval -= 1;
        let sample_smaps = if self.smaps_interval == 0 {
            self.smaps_interval = 10;
            true
        } else {
            false
        };

        self.procfs.poll(sample_smaps).await?;
        self.cgroup.poll().await?;

        if let Some(wss) = &mut self.wss {
            // WSS measures the amount of memory that has been accessed since the last poll.
            // As a consequence, the poll interval impacts the measure.
            // That’s why we need to be sure we don’t poll more often than once per minute.
            if self.last_wss_sample.elapsed() > tokio::time::Duration::from_secs(60) {
                wss.poll().await?;
                self.last_wss_sample = tokio::time::Instant::now();
            }
        }

        Ok(())
    }
}
