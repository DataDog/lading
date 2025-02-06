mod cgroup;
mod procfs;

use tracing::error;

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub enum Error {
    /// Wrapper for [`cgroup::Error`]
    #[error("Cgroup: {0}")]
    CGroup(#[from] cgroup::Error),
    /// Wrapper for [`procfs::Error`]
    #[error("Procfs: {0}")]
    Procfs(#[from] procfs::Error),
}

#[derive(Debug)]
pub(crate) struct Sampler {
    procfs_sampler: procfs::Sampler,
    cgroup_sampler: cgroup::Sampler,
    smaps_interval: u8,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32, labels: Vec<(String, String)>) -> Result<Self, Error> {
        let procfs_sampler = procfs::Sampler::new(parent_pid)?;
        let cgroup_sampler = cgroup::Sampler::new(parent_pid, labels)?;

        Ok(Self {
            procfs_sampler,
            cgroup_sampler,
            smaps_interval: 10,
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

        self.procfs_sampler.poll(sample_smaps).await?;
        self.cgroup_sampler.poll().await?;

        Ok(())
    }
}
