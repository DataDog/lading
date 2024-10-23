use byte_unit::Byte;
use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use lading::generator::file_gen::model;
use lading_payload::block;
use rand::{rngs::SmallRng, SeedableRng};
use tracing::{error, info};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};
// use lading_payload::block;
use nix::libc::{self, ENOENT};
use serde::Deserialize;
use std::{
    ffi::OsStr,
    num::NonZeroU32,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

// fn default_config_path() -> String {
//     "/etc/lading/logrotate_fs.yaml".to_string()
// }

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // /// path on disk to the configuration file
    // #[clap(long, default_value_t = default_config_path())]
    // config_path: String,
    #[clap(long)]
    bytes_per_second: Byte,
    #[clap(long)]
    mount_point: PathBuf,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of [`FileGen`]
struct Config {
    // // /// The seed for random operations against this target
    // // pub seed: [u8; 32],
    // /// Total number of concurrent logs.
    // concurrent_logs: u16,
    // /// The **soft** maximum byte size of each log.
    // maximum_bytes_per_log: Byte,
    // /// The number of rotations per log file.
    // total_rotations: u8,
    // /// The maximum directory depth allowed below the root path. If 0 all log
    // /// files will be present in the root path.
    // max_depth: u8,
    // /// Sets the [`crate::payload::Config`] of this template.
    // variant: lading_payload::Config,
    /// Defines the number of bytes that written in each log file.
    bytes_per_second: Byte,
    // /// Defines the maximum internal cache of this log target. `file_gen` will
    // /// pre-build its outputs up to the byte capacity specified here.
    // maximum_prebuild_cache_size_bytes: Byte,
    // /// The maximum size in bytes of the largest block in the prebuild cache.
    // #[serde(default = "lading_payload::block::default_maximum_block_size")]
    // maximum_block_size: byte_unit::Byte,
    // /// Whether to use a fixed or streaming block cache
    // #[serde(default = "lading_payload::block::default_cache_method")]
    // block_cache_method: block::CacheMethod,
    // /// The load throttle configuration
    // #[serde(default)]
    // throttle: lading_throttle::Config,
    /// The mount-point for this filesystem
    mount_point: PathBuf,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Failed to deserialize configuration: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
}

const TTL: Duration = Duration::from_secs(1); // Attribute cache timeout

#[derive(Debug)]
struct LogrotateFS {
    state: Arc<Mutex<model::State>>,
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

        // Calculate SystemTime instances
        let atime = start_time_system + access_duration;
        let mtime = start_time_system + modified_duration;
        let ctime = start_time_system + status_duration;
        let crtime = start_time_system; // Assume creation time is when the filesystem started

        FileAttr {
            ino: attr.inode as u64,
            size: attr.size,
            blocks: (attr.size + 511) / 512,
            // TODO these times should reflect those in the model, will need to
            // be translated from the tick to systemtime. Implies we'll need to
            // adjust up from start_time, knowing that a tick is one second
            // wide.
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
    #[tracing::instrument(skip(self, _req, _config))]
    fn init(
        &mut self,
        _req: &Request,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        Ok(())
    }

    #[tracing::instrument(skip(self, _req, reply))]
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        let name_str = name.to_str().unwrap_or("");
        if let Some(ino) = state.lookup(tick, parent as usize, name_str) {
            if let Some(attr) = getattr_helper(&mut state, self.start_time_system, tick, ino) {
                info!("lookup: returning attr for inode {}: {:?}", ino, attr);
                reply.entry(&TTL, &attr, 0);
                return;
            } else {
                error!("lookup: getattr_helper returned None for inode {}", ino);
            }
        } else {
            error!("lookup: state.lookup returned None for name {}", name_str);
        }
        reply.error(ENOENT);
    }

    #[tracing::instrument(skip(self, _req, reply))]
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        if let Some(attr) = getattr_helper(&mut state, self.start_time_system, tick, ino as usize) {
            reply.attr(&TTL, &attr);
        } else {
            reply.error(ENOENT);
        }
    }

    #[tracing::instrument(skip(self, _req, reply))]
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");

        // NOTE this call to State::read is almost surely allocating. It'd be
        // pretty slick if we could get the buffer directly from the OS to pass
        // down for writing but we can't. I suppose we could send up the raw
        // blocks and then chain them together as needed but absent a compelling
        // reason to do that the simplicity of this API is nice.
        if let Some(data) = state.read(ino as usize, offset as usize, size as usize, tick) {
            reply.data(&data);
        } else {
            reply.error(ENOENT);
        }
    }

    #[tracing::instrument(skip(self, _req, reply))]
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let tick = self.get_current_tick();
        let mut state = self.state.lock().expect("lock poisoned");
        state.advance_time(tick);

        let root_inode = state.root_inode();

        // TODO building up a vec of entries here to handle offset really does
        // suggest that the model needs to be exposed in such a way that this
        // needn't be done.
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

        // reaming children
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
        reply.opened(ino, flags as u32);
    }
}

#[tracing::instrument]
fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_ansi(false)
        .finish()
        .init();

    info!("Hello, welcome. I hope things are well with you.");

    let args = Args::parse();
    // let config_contents = std::fs::read_to_string(&args.config_path)?;
    // let config: Config = serde_yaml::from_str(&config_contents)?;

    let primes: [u8; 32] = [
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89,
        97, 101, 103, 107, 109, 113, 127, 131,
    ];
    let mut rng = SmallRng::from_seed(primes);

    let block_cache = block::Cache::fixed(
        &mut rng,
        NonZeroU32::new(100_000_000).expect("zero value"), // TODO make this an Error
        10_000,                                            // 10 KiB
        &lading_payload::Config::Ascii,
    )
    .expect("block construction"); // TODO make this an Error

    let state = model::State::new(
        &mut rng,
        args.bytes_per_second.get_bytes() as u64, // Adjust units accordingly
        5,                                        // TODO make an argument
        1_000_000,                                // 1MiB
        block_cache,
        10, // max_depth
        8,  // concurrent_logs
    );

    // Initialize the FUSE filesystem
    let fs = LogrotateFS {
        state: Arc::new(Mutex::new(state)),
        start_time: std::time::Instant::now(),
        start_time_system: std::time::SystemTime::now(),
    };

    // Mount the filesystem
    fuser::mount2(
        fs,
        &args.mount_point,
        &[
            MountOption::FSName("logrotate_fs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ],
    )
    .expect("Failed to mount FUSE filesystem");

    Ok(())
}
