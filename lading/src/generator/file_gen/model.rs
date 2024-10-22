//! Model the internal logic of a logrotate filesystem.

//use lading_payload::block;

use std::collections::HashMap;

use bytes::Bytes;
use lading_payload::block;

/// Time representation of the model
pub type Tick = u64;
/// The identification node number
pub type Inode = usize;

/// Model representation of a `File`. Does not actually contain any bytes but
/// stores sufficient metadata to determine access patterns over time.
#[derive(Debug, Clone, Copy)]
pub struct File {
    /// The parent `Node` of this `File`.
    parent: Inode,

    /// The number of bytes written over the lifetime of this
    /// `File`. Monotonically increasing.
    ///
    /// Property: `bytes_written` >= `bytes_read`.
    bytes_written: u64,
    /// The number of bytes read over the lifetime of this
    /// `File`. Monotonically increasing.
    ///
    /// Property: `bytes_written` >= `bytes_read`.
    bytes_read: u64,

    /// The `Tick` on which the `File` was last accessed. Updated on reads,
    /// opens for reading.
    access_tick: Tick,
    /// The `Tick` on which the `File` was last modified. Updated on writes,
    /// truncations or opens for writing.
    modified_tick: Tick,
    /// The `Tick` on which the `File` last had its status updated. Updated
    /// whenever `access_tick` or `modified_tick` are updated.
    ///
    /// Property: `status_tick` == `modified_tick` || `status_tick` == `access_tick`
    status_tick: Tick,

    /// The number of bytes that accumulate in this `File` per tick.
    bytes_per_tick: u64,

    /// Whether the file is read-only -- that is, no more "writes" will ever
    /// happen -- or not.
    read_only: bool,

    /// The ordinal number of this File. If the file is foo.log the ordinal
    /// number is 0, if foo.log.1 then 1 etc.
    ordinal: u8,

    /// The peer of this file, the next in line in rotation. So, if this file is
    /// foo.log the peer will be foo.log.1 and its peer foo.log.2 etc.
    peer: Option<Inode>,
}

impl File {
    /// Register a read.
    ///
    /// This function is pair to [`File::available_to_read`]. It's possible that
    /// while `available_to_read` to read may signal some value X as being the
    /// total bytes available the pool of entropy or caller will not read up to
    /// X. As such we have to register how much is actually read. That's what
    /// this function does.
    ///
    /// Updates `access_tick` to `now` and adds `request` to `bytes_read`. Time
    /// will be advanced, meaning `modified_tick` may update.
    pub fn read(&mut self, request: u64, now: Tick) {
        self.advance_time(now);

        self.bytes_read = self.bytes_read.saturating_add(request);
        self.access_tick = now;
        self.status_tick = now;
    }

    /// Run the clock forward in the `File`.
    ///
    /// This function runs the clock forward to `now`, updating `modified_tick`
    /// and `status_tick` as bytes are continuously "written" to the `File`.
    ///
    /// Will have no result if `now` <= `modified_tick`. Will have no result if
    /// the file is read-only.
    fn advance_time(&mut self, now: Tick) {
        if now <= self.modified_tick || self.read_only {
            return;
        }

        let diff = now.saturating_sub(self.modified_tick);
        let bytes_accum = diff.saturating_mul(self.bytes_per_tick);

        self.bytes_written = self.bytes_written.saturating_add(bytes_accum);
        self.modified_tick = now;
        self.status_tick = now;
    }

    /// Set this file to read-only
    ///
    /// This function flips the internal bool on this `File` stopping any future
    /// byte accumulations.
    pub fn set_read_only(&mut self) {
        self.read_only = true;
    }

    /// Return whether the file is read-only or not
    #[must_use]
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Return the ordinal number of this File
    #[must_use]
    pub fn ordinal(&self) -> u8 {
        self.ordinal
    }

    /// Increment the ordinal number of this File
    pub fn incr_ordinal(&mut self) {
        self.ordinal = self.ordinal.saturating_add(1);
    }

    /// Returns the current size in bytes of the File
    ///
    /// This function does not advance time.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.bytes_written
    }
}

/// Model representation of a `Directory`. Contains children are `Directory`
/// instances or `File` instances. Root directory will not have a `parent`.
#[derive(Debug)]
pub struct Directory {
    children: HashMap<String, Inode>,
    parent: Option<Inode>,
}

/// A filesystem object, either a `File` or a `Directory`.
#[derive(Debug)]
pub enum Node {
    /// A [`File`]
    File {
        /// The name of this file. If the full path is /logs/foo.log then this is "foo.log".
        name: String,
        /// The `File` instance.
        file: File,
    },
    /// A [`Directory`]
    Directory {
        /// the name of this directory. If the full path is /logs then this is "logs".
        name: String,
        /// The `Directory` instance.
        dir: Directory,
    },
}

impl Node {
    /// Run the clock forward on this node
    pub fn advance_time(&mut self, now: Tick) {
        match self {
            Node::Directory { .. } => { /* nothing, intentionally */ }
            Node::File { file, .. } => file.advance_time(now),
        }
    }
}

/// The state of the filesystem
///
/// This structure is responsible for maintenance of the structure of the
/// filesystem. It does not contain any bytes, the caller must maintain this
/// themselves.
#[derive(Debug)]
pub struct State {
    nodes: HashMap<Inode, Node>,
    root_inode: Inode,
    now: Tick,
    block_cache: block::Cache,
    max_rotations: u8,
    max_bytes_per_file: u64,
}

/// The attributes of a `Node`.
#[derive(Debug, Clone, Copy)]
pub struct NodeAttributes {
    /// The id of the node.
    pub inode: Inode,
    /// The kind, whether a file or directory.
    pub kind: NodeType,
    /// The size in bytes.
    pub size: u64,
    /// The last access time in ticks.
    pub access_tick: Tick,
    /// The last modified time in ticks.
    pub modified_tick: Tick,
    /// The last status change time in ticks.
    pub status_tick: Tick,
}

/// Describe whether the Node is a File or Directory.
#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    /// A [`File`]
    File,
    /// A [`Directory`]
    Directory,
}

impl State {
    /// Create a new instance of `State`.
    #[tracing::instrument(skip(block_cache))]
    pub fn new(
        bytes_per_tick: u64,
        max_rotations: u8,
        max_bytes_per_file: u64,
        block_cache: block::Cache,
    ) -> State {
        let root_inode: Inode = 1; // `/`
        let logs_inode: Inode = 2; // `/logs`
        let foo_log_inode: Inode = 3; // `/logs/foo.log`

        let mut nodes = HashMap::new();

        let mut root_dir = Directory {
            children: HashMap::new(),
            parent: None,
        };
        root_dir.children.insert("logs".to_string(), logs_inode);
        nodes.insert(
            root_inode,
            Node::Directory {
                name: "/".to_string(),
                dir: root_dir,
            },
        );

        let mut logs_dir = Directory {
            children: HashMap::new(),
            parent: Some(root_inode),
        };
        logs_dir
            .children
            .insert("foo.log".to_string(), foo_log_inode);
        nodes.insert(
            logs_inode,
            Node::Directory {
                name: "logs".to_string(),
                dir: logs_dir,
            },
        );

        let foo_log = File {
            parent: logs_inode,

            bytes_written: 0,
            bytes_read: 0,

            access_tick: 0,
            modified_tick: 0,
            status_tick: 0,

            bytes_per_tick,

            read_only: false,
            peer: None,
            ordinal: 0,
        };
        nodes.insert(
            foo_log_inode,
            Node::File {
                name: "foo.log".to_string(),
                file: foo_log,
            },
        );

        // NOTE this structure is going to be a problem when I include rotating
        // files. Specifically the inodes will need to change so there might
        // need to be a concept of a SuperFile that holds inodes or something
        // for its rotating children? Dunno. An array with a current pointer?

        State {
            nodes,
            root_inode,
            now: 0,
            block_cache,
            max_rotations,
            max_bytes_per_file,
        }
    }

    /// Advance time in the model.
    ///
    /// # Panics
    ///
    /// Will panic if passed `now` is less than recorded `now`. Time can only
    /// advance.
    pub fn advance_time(&mut self, now: Tick) {
        // Okay so here's the idea.
        //
        // 1. I introduce a read-only File via boolean flag
        // 2. A File has a "peer" Option<Inode> that allows for lookup of the next in line
        // 3. The names are held here. We traverse the linked list of peers and
        // then delete anything past max_rotations.
        //
        // The State holds all notion of when a File should rotate and also be
        // deleted. The File has no say in that at all.

        // nothing yet beyond updating the clock, rotations to come
        assert!(now >= self.now);

        for (inode, node) in &mut self.nodes {
            match node {
                Node::File { file, .. } => {
                    file.advance_time(now);
                    if file.read_only() {
                        continue;
                    };
                    if file.size() >= self.max_bytes_per_file {
                        file.set_read_only();
                        // Add rotation logic here. What I want to do is add a
                        // new File with the name of the current `File` bump the
                        // ordinal number of `file` and make the new File have
                        // `file` as its peer. I need to adjust `self.nodes` to
                        // include the new File and also modify the name of the
                        // current `file`. Or I should just make a pre-defined
                        // array of names and index into them with the ordinal.
                        //
                        // If the ordinal of the `file` is past
                        // self.max_rotations we delete it.
                    }
                }
                Node::Directory { .. } => {
                    // directories are immutable though time flows around them
                }
            }
        }

        self.now = now;
    }

    /// Look up the Inode for a given `name`.
    ///
    /// This function searches under `parent_inode` for a match to `name`,
    /// returning any inode that happens to match. Time will be advanced to
    /// `now`.
    #[tracing::instrument(skip(self))]
    pub fn lookup(&mut self, now: Tick, parent_inode: Inode, name: &str) -> Option<Inode> {
        self.advance_time(now);

        if let Some(Node::Directory { dir, .. }) = self.nodes.get(&parent_inode) {
            dir.children.get(name).copied()
        } else {
            None
        }
    }

    /// Look up the attributes for an `Inode`.
    ///
    /// Time will be advanced to `now`.
    #[tracing::instrument(skip(self))]
    pub fn getattr(&mut self, now: Tick, inode: Inode) -> Option<NodeAttributes> {
        self.advance_time(now);

        self.nodes.get(&inode).map(|node| match node {
            Node::File { file, .. } => NodeAttributes {
                inode,
                kind: NodeType::File,
                size: file.bytes_written,
                access_tick: file.access_tick,
                modified_tick: file.modified_tick,
                status_tick: file.status_tick,
            },
            Node::Directory { .. } => NodeAttributes {
                inode,
                kind: NodeType::Directory,
                size: 0,
                access_tick: self.now,
                modified_tick: self.now,
                status_tick: self.now,
            },
        })
    }

    /// Read `size` bytes from `inode`.
    ///
    /// We do not model a position in files, meaning that `offset` is
    /// ignored. An attempt will be made to read `size` bytes at time `tick` --
    /// time will be advanced -- and a slice up to `size` bytes will be returned
    /// or `None` if no bytes are available to be read.
    #[tracing::instrument(skip(self))]
    pub fn read(&mut self, inode: Inode, offset: usize, size: usize, now: Tick) -> Option<Bytes> {
        self.advance_time(now);

        match self.nodes.get_mut(&inode) {
            Some(Node::File {
                name: _,
                ref mut file,
            }) => {
                let bytes_written = usize::try_from(file.bytes_written)
                    .expect("more bytes written than machine word");

                if offset >= bytes_written {
                    // Offset beyond EOF
                    return Some(Bytes::new());
                }

                let available = bytes_written.saturating_sub(offset);
                let to_read = available.min(size);

                // Get data from block_cache without worrying about blocks
                let data = self.block_cache.read_at(offset as u64, to_read);

                file.read(to_read as u64, now);

                Some(data)
            }
            Some(Node::Directory { .. }) | None => None,
        }
    }

    /// Read inodes from a directory
    ///
    /// Returns None if the inode is a `File`, else returns the hashset of
    /// children inodes.
    ///
    /// Function does not advance time in the model.
    #[tracing::instrument(skip(self))]
    pub fn readdir(&self, inode: Inode) -> Option<&HashMap<String, Inode>> {
        if let Some(Node::Directory { dir, .. }) = self.nodes.get(&inode) {
            Some(&dir.children)
        } else {
            None
        }
    }

    /// Get the fuser file type of an inode if it exists
    #[tracing::instrument(skip(self))]
    pub fn get_file_type(&self, inode: Inode) -> Option<fuser::FileType> {
        self.nodes.get(&inode).map(|node| match node {
            Node::Directory { .. } => fuser::FileType::Directory,
            Node::File { .. } => fuser::FileType::RegularFile,
        })
    }

    /// Return the parent inode of an inode, if it exists
    #[tracing::instrument(skip(self))]
    pub fn get_parent_inode(&self, inode: Inode) -> Option<Inode> {
        if inode == self.root_inode {
            Some(self.root_inode)
        } else {
            self.nodes.get(&inode).map(|node| match node {
                Node::Directory { dir, .. } => dir.parent.unwrap_or(self.root_inode),
                Node::File { file, .. } => file.parent,
            })
        }
    }

    /// Return the root inode of this state
    #[must_use]
    pub fn root_inode(&self) -> Inode {
        self.root_inode
    }

    /// Return the number of links for the inode.
    #[must_use]
    pub fn nlink(&self, inode: Inode) -> usize {
        if let Some(Node::Directory { dir, .. }) = self.nodes.get(&inode) {
            let subdirectory_count = dir
                .children
                .iter()
                .filter(|(_, child_inode)| {
                    matches!(self.nodes.get(child_inode), Some(Node::Directory { .. }))
                })
                .count();
            // nlink is 2 (for "." and "..") plus the number of subdirectories
            2 + subdirectory_count
        } else {
            1
        }
    }
}
