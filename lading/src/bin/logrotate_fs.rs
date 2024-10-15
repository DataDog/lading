use byte_unit::Byte;
use clap::Parser;
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use model::NodeType;
use tracing::{error, info};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt};
// use lading_payload::block;
use nix::libc::{self, ENOENT};
use serde::Deserialize;
use std::{
    ffi::OsStr,
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
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

mod model {
    pub type Inode = usize;
    pub type Tick = u64;

    #[derive(Debug)]
    pub struct File<'a> {
        name: String,
        accumulated_bytes: u64,
        last_tick: Tick,
        content: &'a [u8],
        parent: Inode,
    }

    #[derive(Debug)]
    pub struct Directory {
        name: String,
        children: &'static [Inode],
        parent: Option<Inode>,
    }

    #[derive(Debug)]
    pub enum Node<'a> {
        File(File<'a>),
        Directory(Directory),
    }

    #[derive(Debug)]
    pub struct State<'a> {
        nodes: Vec<Node<'a>>,
        pub(super) root_inode: Inode,
        bytes_per_tick: u64,
    }

    #[derive(Debug)]
    pub struct NodeAttributes {
        pub(super) inode: Inode,
        pub(super) kind: NodeType,
        pub(super) size: u64,
    }

    #[derive(Debug)]
    pub enum NodeType {
        File,
        Directory,
    }

    impl<'a> State<'a> {
        #[tracing::instrument(skip(content))]
        pub fn new(bytes_per_tick: u64, content: &'a [u8]) -> State<'a> {
            let root_inode: Inode = 0; // `/`
            let logs_inode: Inode = 1; // `/logs`
            let foo_log_inode: Inode = 2; // `/logs/foo.log`

            let mut nodes = Vec::new();

            let root_dir = Directory {
                name: "/".to_string(),
                children: &[], // Will update later
                parent: None,
            };
            nodes.push(Node::Directory(root_dir));

            let logs_dir = Directory {
                name: "logs".to_string(),
                children: &[], // Will update later
                parent: Some(root_inode),
            };
            nodes.push(Node::Directory(logs_dir));

            let foo_log = File {
                name: "foo.log".to_string(),
                accumulated_bytes: 0,
                last_tick: 0,
                content,
                parent: logs_inode,
            };
            nodes.push(Node::File(foo_log));

            // Update children
            let logs_children: &'static [Inode] = Box::leak(vec![foo_log_inode].into_boxed_slice());

            if let Node::Directory(ref mut dir) = nodes[logs_inode] {
                dir.children = logs_children;
            }

            let root_children: &'static [Inode] = Box::leak(vec![logs_inode].into_boxed_slice());

            if let Node::Directory(ref mut dir) = nodes[root_inode] {
                dir.children = root_children;
            }

            State {
                nodes,
                root_inode,
                bytes_per_tick,
            }
        }

        #[tracing::instrument(skip(self))]
        pub fn lookup(&self, parent_inode: Inode, name: &str) -> Option<Inode> {
            if let Some(Node::Directory(dir)) = self.nodes.get(parent_inode) {
                for &child_inode in dir.children {
                    let child_node = &self.nodes[child_inode];
                    let child_name = match child_node {
                        Node::Directory(child_dir) => &child_dir.name,
                        Node::File(child_file) => &child_file.name,
                    };
                    if child_name == name {
                        return Some(child_inode);
                    }
                }
            }
            None
        }

        #[tracing::instrument(skip(self))]
        pub fn getattr(&self, inode: Inode) -> Option<NodeAttributes> {
            self.nodes.get(inode).map(|node| match node {
                Node::File(file) => NodeAttributes {
                    inode,
                    kind: NodeType::File,
                    size: file.accumulated_bytes,
                },
                Node::Directory(_) => NodeAttributes {
                    inode,
                    kind: NodeType::Directory,
                    size: 0,
                },
            })
        }

        // TODO honestly i'm not sure how I want to handle this. I need to think
        // about how the block mechanism works.

        // #[tracing::instrument(skip(self))]
        // // TODO modify to use a Block so that we can simulate waiting for data. Also unclear if the block size mechanism is suitable given the way reads happen but h
        // pub fn read(
        //     &mut self,
        //     inode: Inode,
        //     offset: usize,
        //     size: usize,
        //     tick: Tick,
        // ) -> Option<&[u8]> {
        //     if let Some(Node::File(file)) = self.nodes.get_mut(inode) {
        //         // Update accumulated bytes as per the current tick
        //         let elapsed_ticks = tick.saturating_sub(file.last_tick);
        //         if elapsed_ticks > 0 {
        //             let additional_bytes = self.bytes_per_tick * elapsed_ticks;
        //             let new_accumulated = file.accumulated_bytes + additional_bytes;

        //             // Update accumulated bytes and last_tick
        //             file.accumulated_bytes = new_accumulated;
        //             file.last_tick = tick;
        //         }

        //         // Simulate infinite data by repeating the content
        //         let available_bytes = file.accumulated_bytes;
        //         if offset as u64 >= available_bytes {
        //             // No data available at this offset
        //             return Some(&[]);
        //         }

        //         // Calculate how many bytes we can return
        //         let bytes_to_read =
        //             std::cmp::min(size as u64, available_bytes - offset as u64) as usize;

        //         // Calculate the position in the content buffer
        //         let content_len = file.content.len();
        //         let start = offset % content_len;
        //         let end = start + bytes_to_read.min(content_len - start);

        //         Some(&file.content[start..end])
        //     } else {
        //         None
        //     }
        // }

        #[tracing::instrument(skip(self))]
        pub fn readdir(&self, inode: Inode) -> Option<&[Inode]> {
            if let Some(Node::Directory(dir)) = self.nodes.get(inode) {
                Some(dir.children)
            } else {
                None
            }
        }

        #[tracing::instrument(skip(self))]
        pub fn get_name(&self, inode: Inode) -> &str {
            match &self.nodes[inode] {
                Node::Directory(dir) => &dir.name,
                Node::File(file) => &file.name,
            }
        }

        #[tracing::instrument(skip(self))]
        pub fn get_file_type(&self, inode: Inode) -> fuser::FileType {
            match &self.nodes[inode] {
                Node::Directory(_) => fuser::FileType::Directory,
                Node::File(_) => fuser::FileType::RegularFile,
            }
        }

        #[tracing::instrument(skip(self))]
        pub fn get_parent_inode(&self, inode: Inode) -> Inode {
            if inode == self.root_inode {
                self.root_inode
            } else {
                match &self.nodes[inode] {
                    Node::Directory(dir) => dir.parent.unwrap_or(self.root_inode),
                    Node::File(file) => file.parent,
                }
            }
        }

        #[tracing::instrument(skip(self))]
        pub fn update_state(&mut self, current_tick: Tick) {
            for node in self.nodes.iter_mut() {
                if let Node::File(file) = node {
                    let elapsed_ticks = current_tick.saturating_sub(file.last_tick);

                    if elapsed_ticks > 0 {
                        let additional_bytes = self.bytes_per_tick * elapsed_ticks;
                        let new_accumulated = file.accumulated_bytes + additional_bytes;

                        // Update accumulated bytes and last_tick
                        file.accumulated_bytes = new_accumulated;
                        file.last_tick = current_tick;
                    }
                }
            }
        }
    }
}

const TTL: Duration = Duration::from_secs(1); // Attribute cache timeout

#[derive(Debug)]
struct LogrotateFS<'a> {
    state: model::State<'a>,
    start_time: std::time::Instant,
}

impl<'a> LogrotateFS<'a> {
    #[tracing::instrument(skip(self))]
    fn get_current_tick(&self) -> model::Tick {
        self.start_time.elapsed().as_secs()
    }

    #[tracing::instrument(skip(self))]
    fn getattr_helper(&mut self, inode: usize) -> Option<FileAttr> {
        self.state.getattr(inode).map(|attr| FileAttr {
            ino: attr.inode as u64,
            size: attr.size,
            blocks: (attr.size + 511) / 512,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: match attr.kind {
                NodeType::File => fuser::FileType::RegularFile,
                NodeType::Directory => fuser::FileType::Directory,
            },
            perm: if matches!(attr.kind, NodeType::Directory) {
                0o755
            } else {
                0o644
            },
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        })
    }
}

impl<'a> Filesystem for LogrotateFS<'a> {
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
        self.state.update_state(tick);

        let name_str = name.to_str().unwrap_or("");
        if let Some(ino) = self.state.lookup(parent as usize, name_str) {
            if let Some(attr) = self.getattr_helper(ino) {
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }
        reply.error(ENOENT);
    }

    #[tracing::instrument(skip(self, _req, reply))]
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let tick = self.get_current_tick();
        self.state.update_state(tick);

        if let Some(attr) = self.getattr_helper(ino as usize) {
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
        self.state.update_state(tick);

        if let Some(data) = self
            .state
            .read(ino as usize, offset as usize, size as usize, tick)
        {
            reply.data(data);
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
        self.state.update_state(tick);

        if offset != 0 {
            reply.ok();
            return;
        }

        if let Some(entries) = self.state.readdir(ino as usize) {
            let mut index = 1;
            // TODO fix
            let _ = reply.add(ino, index, fuser::FileType::Directory, ".");
            index += 1;
            let root_inode = self.state.root_inode;
            if ino != root_inode as u64 {
                let parent_ino = self.state.get_parent_inode(ino as usize);
                // TODO fix
                let _ = reply.add(parent_ino as u64, index, fuser::FileType::Directory, "..");
                index += 1;
            }

            for &child_ino in entries {
                let name = self.state.get_name(child_ino);
                let file_type = self.state.get_file_type(child_ino);
                // TODO fix
                let _ = reply.add(child_ino as u64, index, file_type, name);
                index += 1;
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
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

    let content = "\
Call me Ishmael.\n\
Some years ago—never mind how long precisely—having little or no money in my purse, and nothing particular to interest me on shore, I thought I would sail about a little and see the watery part of the world. It is a way I have of driving off the spleen and regulating the circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing up the rear of every funeral I meet; and especially whenever my hypos get such an upper hand of me, that it requires a strong moral principle to prevent me from deliberately stepping into the street, and methodically knocking people’s hats off—then, I account it high time to get to sea as soon as I can. This is my substitute for pistol and ball. With a philosophical flourish Cato throws himself upon his sword; I quietly take to the ship. There is nothing surprising in this. If they but knew it, almost all men in their degree, some time or other, cherish very nearly the same feelings towards the ocean with me.\n\
\n\
There now is your insular city of the Manhattoes, belted round by wharves as Indian isles by coral reefs—commerce surrounds it with her surf. Right and left, the streets take you waterward. Its extreme downtown is the battery, where that noble mole is washed by waves, and cooled by breezes, which a few hours previous were out of sight of land. Look at the crowds of water-gazers there.\n".as_bytes();

    // Initialize the model state
    let state = model::State::new(
        args.bytes_per_second.get_bytes() as u64, // Adjust units accordingly
        content,
    );

    // Initialize the FUSE filesystem
    let fs = LogrotateFS {
        state,
        start_time: std::time::Instant::now(),
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
