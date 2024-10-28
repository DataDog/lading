//! A filesystem that mimics logs with rotation

#![allow(clippy::cast_sign_loss)] // TODO remove these clippy allows
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(missing_docs)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::needless_pass_by_value)]

use fuser::{
    spawn_mount2, BackgroundSession, FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, Request,
};
use lading_payload::block;
use rand::{rngs::SmallRng, SeedableRng};
use tokio::task;
use tracing::{error, info};
// use lading_payload::block;
use crate::generator;
use nix::libc::{self, ENOENT};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::OsStr,
    num::NonZeroU32,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

mod model;

const TTL: Duration = Duration::from_secs(1); // Attribute cache timeout

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of [`FileGen`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// Total number of concurrent logs.
    concurrent_logs: u16,
    /// The maximum byte size of each log.
    maximum_bytes_per_log: byte_unit::Byte,
    /// The number of rotations per log file.
    total_rotations: u8,
    /// The maximum directory depth allowed below the root path. If 0 all log
    /// files will be present in the root path.
    max_depth: u8,
    /// Sets the [`crate::payload::Config`] of this template.
    variant: lading_payload::Config,
    /// Defines the number of bytes that written in each log file.
    bytes_per_second: byte_unit::Byte,
    /// Defines the maximum internal cache of this log target. `file_gen` will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    maximum_block_size: byte_unit::Byte,
    /// The mount-point for this filesystem
    mount_point: PathBuf,
}

#[derive(thiserror::Error, Debug)]
/// Error for `LogrotateFs`
pub enum Error {
    #[error(transparent)]
    /// IO error
    Io(#[from] std::io::Error),
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
    /// Failed to convert, value is 0
    #[error("Value provided must not be zero")]
    Zero,
}

#[derive(Debug)]
/// The logrotate filesystem server.
///
/// This generator manages a FUSE filesystem which "writes" files to a mounted
/// filesystem, rotating them as appropriate. It does this without coordination
/// to the target _but_ keeps track of how many bytes are written and read
/// during operation.
pub struct Server {
    //    handles: Vec<JoinHandle<Result<(), Error>>>,
    shutdown: lading_signal::Watcher,
    background_session: BackgroundSession,
}

impl Server {
    pub fn new(
        _: generator::General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        // TODO spawn a filesystem thread here and not in spin but bubble up any
        // errors and make it so that spin waits for the FS thread and also for
        // the shutdown signal. We have a synchronous try_recv on the Watcher
        // but the way that fuser runs it just blocks the thread and there's no
        // place to poll.
        let mut rng = SmallRng::from_seed(config.seed);

        let total_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                .ok_or(Error::Zero)?;
        let block_cache = block::Cache::fixed(
            &mut rng,
            total_bytes,
            config.maximum_block_size.get_bytes(),
            &config.variant,
        )?;

        let state = model::State::new(
            &mut rng,
            config.bytes_per_second.get_bytes() as u64,
            config.total_rotations,
            config.maximum_bytes_per_log.get_bytes() as u64,
            block_cache,
            config.max_depth,
            config.concurrent_logs,
        );

        // Initialize the FUSE filesystem
        let fs = LogrotateFS {
            state: Arc::new(Mutex::new(state)),
            open_files: Arc::new(Mutex::new(HashMap::new())),
            start_time: std::time::Instant::now(),
            start_time_system: std::time::SystemTime::now(),
        };

        let options = vec![
            MountOption::FSName("lading_logrotate_fs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ];

        // Mount the filesystem in the background
        let background_session = spawn_mount2(fs, config.mount_point, &options)
            .expect("Failed to mount FUSE filesystem");

        Ok(Self {
            shutdown,
            background_session,
        })
    }

    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_possible_truncation)]
    pub async fn spin(self) -> Result<(), Error> {
        self.shutdown.recv().await;

        let handle = task::spawn_blocking(|| self.background_session.join());
        let _ = handle.await;

        Ok(())
    }
}

#[derive(Debug)]
struct LogrotateFS {
    state: Arc<Mutex<model::State>>,
    open_files: Arc<Mutex<HashMap<u64, model::FileHandle>>>,

    start_time: std::time::Instant,
    start_time_system: std::time::SystemTime,
}

impl LogrotateFS {
    #[tracing::instrument(skip(self))]
    fn get_current_tick(&self) -> model::Tick {
        self.start_time.elapsed().as_secs()
    }
}

#[tracing::instrument(skip(state))]
fn getattr_helper(
    state: &mut MutexGuard<model::State>,
    start_time_system: std::time::SystemTime,
    tick: model::Tick,
    inode: usize,
) -> Option<FileAttr> {
    let nlink = state.nlink(inode) as u32;

    state.getattr(tick, inode).map(|attr| {
        // Convert ticks to durations
        let access_duration = Duration::from_secs(attr.access_tick);
        let modified_duration = Duration::from_secs(attr.modified_tick);
        let status_duration = Duration::from_secs(attr.status_tick);
        let created_duration = Duration::from_secs(attr.created_tick);

        // Calculate SystemTime instances
        let atime = start_time_system + access_duration;
        let mtime = start_time_system + modified_duration;
        let ctime = start_time_system + status_duration;
        let crtime = start_time_system + created_duration;

        FileAttr {
            ino: attr.inode as u64,
            size: attr.size,
            blocks: (attr.size + 511) / 512,
            atime,
            mtime,
            ctime,
            crtime,
            kind: match attr.kind {
                model::NodeType::File => fuser::FileType::RegularFile,
                model::NodeType::Directory => fuser::FileType::Directory,
            },
            perm: if matches!(attr.kind, model::NodeType::Directory) {
                0o755
            } else {
                0o644
            },
            nlink,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    })
}

impl Filesystem for LogrotateFS {
    #[tracing::instrument(skip(self))]
    fn init(&mut self, _: &Request, _: &mut fuser::KernelConfig) -> Result<(), libc::c_int> {
        Ok(())
    }

    #[tracing::instrument(skip(self, reply))]
    fn lookup(&mut self, _: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        let name_str = name.to_str().unwrap_or("");
        if let Some(ino) = state.lookup(tick, parent as usize, name_str) {
            if let Some(attr) = getattr_helper(&mut state, self.start_time_system, tick, ino) {
                info!("lookup: returning attr for inode {}: {:?}", ino, attr);
                reply.entry(&TTL, &attr, 0);
                return;
            }
            error!("lookup: getattr_helper returned None for inode {}", ino);
        } else {
            error!("lookup: state.lookup returned None for name {}", name_str);
        }
        reply.error(ENOENT);
    }

    #[tracing::instrument(skip(self, reply))]
    fn getattr(&mut self, _: &Request, ino: u64, reply: ReplyAttr) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        if let Some(attr) = getattr_helper(&mut state, self.start_time_system, tick, ino as usize) {
            reply.attr(&TTL, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    #[tracing::instrument(skip(self, reply))]
    fn read(
        &mut self,
        _: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _: i32,
        _: Option<u64>,
        reply: ReplyData,
    ) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        // Get the FileHandle from fh
        let file_handle = {
            let open_files = self.open_files.lock().expect("lock poisoned");
            open_files.get(&fh).copied()
        };

        if let Some(file_handle) = file_handle {
            assert!(
                file_handle.inode() as u64 == ino,
                "file handle inode and passed ino do not match"
            );
            if let Some(data) = state.read(file_handle, offset as usize, size as usize, tick) {
                reply.data(&data);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    #[tracing::instrument(skip(self, reply))]
    fn release(
        &mut self,
        _: &Request,
        _: u64,
        fh: u64,
        _: i32,
        _: Option<u64>,
        _: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        // Remove the FileHandle from the mapping
        let file_handle = {
            let mut open_files = self.open_files.lock().expect("lock poisoned");
            open_files.remove(&fh)
        };

        if let Some(file_handle) = file_handle {
            // Close the file in the state
            state.close_file(tick, file_handle);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    #[tracing::instrument(skip(self, reply))]
    fn readdir(&mut self, _: &Request, ino: u64, _: u64, offset: i64, mut reply: ReplyDirectory) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");
        state.advance_time(tick);

        let root_inode = state.root_inode();

        // TODO building up a vec of entries here to handle offset really does
        // suggest that the model needs to be exposed in such a way that this
        // needn't be done.
        //
        // Ah, actually, the right buffer to push into is reply.add. There's no
        // need for `entries` at all.
        let mut entries = Vec::new();

        // '.' and '..'
        entries.push((ino, fuser::FileType::Directory, ".".to_string()));
        if ino != root_inode as u64 {
            let parent_ino = state
                .get_parent_inode(ino as usize)
                .expect("inode must have parent");
            entries.push((
                parent_ino as u64,
                fuser::FileType::Directory,
                "..".to_string(),
            ));
        }

        // remaining children
        if let Some(child_inodes) = state.readdir(ino as usize) {
            for child_ino in child_inodes {
                let file_type = state
                    .get_file_type(*child_ino)
                    .expect("inode must have file type");
                let child_name = state.get_name(*child_ino).expect("inode must have a name");
                entries.push((*child_ino as u64, file_type, child_name.to_string()));
            }
        } else {
            reply.error(ENOENT);
            return;
        }

        let mut idx = offset as usize;
        while idx < entries.len() {
            let (inode, file_type, name) = &entries[idx];
            let next_offset = (idx + 1) as i64;
            if reply.add(*inode, next_offset, *file_type, name) {
                // Buffer is full, exit the loop
                break;
            }
            idx += 1;
        }
        reply.ok();
    }

    #[tracing::instrument(skip(self, _req, reply))]
    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        if let Some(file_handle) = state.open_file(tick, ino as usize) {
            let fh = file_handle.id();
            {
                let mut open_files = self.open_files.lock().expect("lock poisoned");
                open_files.insert(fh, file_handle);
            }
            reply.opened(fh, flags as u32);
        } else {
            reply.error(ENOENT);
        }
    }
}