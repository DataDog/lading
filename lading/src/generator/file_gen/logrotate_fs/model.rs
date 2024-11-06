//! Model the internal logic of a logrotate filesystem.

use bytes::Bytes;
use lading_payload::block;
use metrics::counter;
use rand::Rng;
use rustc_hash::FxHashMap;
use std::collections::BTreeSet;
use tracing::info;

/// Time representation of the model
pub(crate) type Tick = u64;
/// The identification node number
pub(crate) type Inode = usize;

/// Model representation of a `File`. Does not actually contain any bytes but
/// stores sufficient metadata to determine access patterns over time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct File {
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

    /// The `Tick` on which the `File` was created.
    created_tick: Tick,
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

    /// When the file became read-only. Will only be Some if `read_only` is false.
    read_only_since: Option<Tick>,

    /// The peer of this file, the next in line in rotation. So, if this file is
    /// foo.log the peer will be foo.log.1 and its peer foo.log.2 etc.
    peer: Option<Inode>,

    /// The ordinal number of this File. If the file is foo.log the ordinal
    /// number is 0, if foo.log.1 then 1 etc.
    ordinal: u8,

    /// The group ID of this File. So for instance all File instances that are
    /// called foo.log, foo.log.1 etc have the same group ID.
    group_id: u16,

    /// The number of open file handles for this `File`.
    open_handles: usize,

    /// Indicates that the `File` no longer has a name but is not removed from
    /// the filesystem.
    unlinked: bool,

    /// The maximual offset observed, maintained by `State`.
    max_offset_observed: u64,
}

/// Represents an open file handle.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FileHandle {
    id: u64,
    inode: Inode,
}

impl FileHandle {
    /// Return the ID of this file handle
    #[must_use]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Return the inode of this file handle
    #[must_use]
    pub(crate) fn inode(&self) -> Inode {
        self.inode
    }
}

impl File {
    /// Create a new instance of `File`
    pub(crate) fn new(
        parent: Inode,
        group_id: u16,
        bytes_per_tick: u64,
        now: Tick,
        peer: Option<Inode>,
    ) -> Self {
        Self {
            parent,
            bytes_written: 0,
            bytes_read: 0,
            access_tick: now,
            modified_tick: now,
            status_tick: now,
            created_tick: now,
            bytes_per_tick,
            read_only: false,
            read_only_since: None,
            ordinal: 0,
            peer,
            group_id,
            open_handles: 0,
            unlinked: false,
            max_offset_observed: 0,
        }
    }

    /// Open a new handle to this file.
    pub(crate) fn open(&mut self, now: Tick) {
        self.advance_time(now);
        if now > self.access_tick {
            self.access_tick = now;
            self.status_tick = now;
        }

        self.open_handles += 1;
    }

    /// Close a handle to this file.
    ///
    /// # Panics
    ///
    /// Function will panic if attempt is made to close file with no file
    /// handles outstanding.
    pub(crate) fn close(&mut self, now: Tick) {
        self.advance_time(now);

        assert!(
            self.open_handles > 0,
            "Attempted to close a file with no open handles"
        );
        self.open_handles -= 1;
    }

    /// Mark the file as unlinked (deleted).
    pub(crate) fn unlink(&mut self, now: Tick) {
        self.advance_time(now);

        self.unlinked = true;
    }

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
    pub(crate) fn read(&mut self, request: u64, now: Tick) {
        self.advance_time(now);
        if now > self.access_tick {
            self.access_tick = now;
            self.status_tick = now;
        }

        counter!("bytes_read").increment(request);
        self.bytes_read = self.bytes_read.saturating_add(request);
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

        counter!("bytes_written").increment(bytes_accum);
        self.bytes_written = self.bytes_written.saturating_add(bytes_accum);
        self.modified_tick = now;
        self.status_tick = now;
    }

    /// Set this file to read-only
    ///
    /// This function flips the internal bool on this `File` stopping any future
    /// byte accumulations.
    pub(crate) fn set_read_only(&mut self, now: Tick) {
        self.read_only = true;
        self.read_only_since = Some(now);
    }

    /// Return whether the file is read-only or not
    #[must_use]
    pub(crate) fn read_only(&self) -> bool {
        self.read_only
    }

    /// Return the ordinal number of this File
    #[must_use]
    pub(crate) fn ordinal(&self) -> u8 {
        self.ordinal
    }

    /// Increment the ordinal number of this File
    pub(crate) fn incr_ordinal(&mut self) {
        self.ordinal = self.ordinal.saturating_add(1);
    }
}

/// Model representation of a `Directory`. Contains children are `Directory`
/// instances or `File` instances. Root directory will not have a `parent`.
#[derive(Debug, Default)]
pub(crate) struct Directory {
    children: BTreeSet<Inode>,
    parent: Option<Inode>,
}

impl Directory {
    fn new(parent: Option<Inode>) -> Self {
        Self {
            children: BTreeSet::default(),
            parent,
        }
    }
}

/// A filesystem object, either a `File` or a `Directory`.
#[derive(Debug)]
pub(crate) enum Node {
    /// A [`File`]
    File {
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

/// Profile for load in this filesystem.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LoadProfile {
    /// Constant bytes per tick
    Constant(u64),
    /// Linear growth of bytes per tick
    Linear {
        /// Starting point for bytes per tick
        start: u64,
        /// Amount to increase per tick
        rate: u64,
    },
}

/// The state of the filesystem
///
/// This structure is responsible for maintenance of the structure of the
/// filesystem. It does not contain any bytes, the caller must maintain this
/// themselves.
pub(crate) struct State {
    nodes: FxHashMap<Inode, Node>,
    root_inode: Inode,
    now: Tick,
    initial_tick: Tick,
    block_cache: block::Cache,
    max_bytes_per_file: u64,
    max_rotations: u8,
    // [GroupID, [Names]]. The interior Vec have size `max_rotations`.
    group_names: Vec<Vec<String>>,
    next_inode: Inode,
    next_file_handle: u64,
    inode_scratch: Vec<Inode>,
    load_profile: LoadProfile,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("nodes", &self.nodes)
            .field("root_inode", &self.root_inode)
            .field("now", &self.now)
            // intentionally leaving out block_cache
            // intentionally leaving out inode_scratch
            .field("max_rotations", &self.max_rotations)
            .field("max_bytes_per_file", &self.max_bytes_per_file)
            .field("group_names", &self.group_names)
            .field("next_inode", &self.next_inode)
            .field("load_profile", &self.load_profile)
            .finish_non_exhaustive()
    }
}

/// The attributes of a `Node`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NodeAttributes {
    /// The id of the node.
    pub(crate) inode: Inode,
    /// The kind, whether a file or directory.
    pub(crate) kind: NodeType,
    /// The size in bytes.
    pub(crate) size: u64,
    /// The last access time in ticks.
    pub(crate) access_tick: Tick,
    /// The last modified time in ticks.
    pub(crate) modified_tick: Tick,
    /// The last status change time in ticks.
    pub(crate) status_tick: Tick,
    /// The tick on which the file was created.
    pub(crate) created_tick: Tick,
}

/// Describe whether the Node is a File or Directory.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NodeType {
    /// A [`File`]
    File,
    /// A [`Directory`]
    Directory,
}

impl State {
    /// Create a new instance of `State`.
    #[tracing::instrument(skip(rng, block_cache))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<R>(
        rng: &mut R,
        initial_tick: Tick,
        max_rotations: u8,
        max_bytes_per_file: u64,
        block_cache: block::Cache,
        max_depth: u8,
        concurrent_logs: u16,
        load_profile: LoadProfile,
    ) -> State
    where
        R: Rng,
    {
        let root_inode: Inode = 1; // `/`
        let mut nodes = FxHashMap::default();

        let root_dir = Directory::default();
        nodes.insert(
            root_inode,
            Node::Directory {
                name: "/".to_string(),
                dir: root_dir,
            },
        );

        let mut state = State {
            nodes,
            root_inode,
            initial_tick,
            now: initial_tick,
            block_cache,
            max_bytes_per_file,
            max_rotations,
            group_names: Vec::new(),
            next_inode: 2,
            next_file_handle: 0,
            inode_scratch: Vec::with_capacity(concurrent_logs as usize),
            load_profile,
        };

        if concurrent_logs == 0 {
            return state;
        }

        // Strategy:
        //
        // For 0 to `concurrent_logs` generate a directory path up to
        // `max_depth` from the root node and place a file in that
        // directory. Node that we must keep track of the group we're in, so we
        // loop over `concurrent_logs`.
        for group_id in 0..concurrent_logs {
            // First, generate the group name.
            let base: String = (0..8)
                .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
                .collect();
            let base_name = format!("{base}_{group_id}.log");
            let mut names = Vec::new();
            names.push(base_name.clone()); // Ordinal 0
            for i in 1..=max_rotations {
                names.push(format!("{base_name}.{i}")); // Ordinal i
            }
            state.group_names.push(names);

            // Now, build up the directory tree and put the file in it.
            let mut current_inode = state.root_inode;
            let depth = if max_depth == 0 {
                0
            } else {
                rng.gen_range(1..=max_depth as usize)
            };

            // Build the directory path
            for _ in 0..depth {
                let dir_name: String = (0..8)
                    .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
                    .collect();

                // Create the directory. If the name already exists under the current_inode we reuse it.
                let dir_inode = {
                    if let Some(Node::Directory { dir, .. }) = state.nodes.get(&current_inode) {
                        let mut found_inode = None;
                        for &child_inode in &dir.children {
                            if let Some(Node::Directory { name, .. }) =
                                state.nodes.get(&child_inode)
                            {
                                if name == &dir_name {
                                    found_inode = Some(child_inode);
                                    break;
                                }
                            }
                        }

                        if let Some(inode) = found_inode {
                            // Already exists, use it.
                            inode
                        } else {
                            // Does not exist, create it.
                            let new_inode = state.next_inode;
                            state.next_inode += 1;

                            let new_dir = Directory::new(Some(current_inode));
                            state.nodes.insert(
                                new_inode,
                                Node::Directory {
                                    name: dir_name.clone(),
                                    dir: new_dir,
                                },
                            );

                            if let Some(Node::Directory { dir, .. }) =
                                state.nodes.get_mut(&current_inode)
                            {
                                dir.children.insert(new_inode);
                            }

                            new_inode
                        }
                    } else {
                        panic!("current_inode {current_inode} is not a directory");
                    }
                };

                // Move to the next directory level
                current_inode = dir_inode;
            }

            // current_inode is the directory that'll be the parent for the new File.
            let file_inode = state.next_inode;
            state.next_inode += 1;

            let file = File::new(current_inode, group_id, 0, state.now, None);
            state.nodes.insert(file_inode, Node::File { file });

            // Add the file to the directory's children
            if let Some(Node::Directory { dir, .. }) = state.nodes.get_mut(&current_inode) {
                dir.children.insert(file_inode);
            }
        }

        state
    }

    /// Open a file and return a handle.
    ///
    /// This function advances time.
    pub(crate) fn open_file(&mut self, now: Tick, inode: Inode) -> Option<FileHandle> {
        self.advance_time(now);

        if let Some(Node::File { file, .. }) = self.nodes.get_mut(&inode) {
            file.open(now);
            let id = self.next_file_handle;
            self.next_file_handle = self.next_file_handle.wrapping_add(1);
            Some(FileHandle { id, inode })
        } else {
            None
        }
    }

    /// Close a file handle.
    ///
    /// This function advances time.
    ///
    /// # Panics
    ///
    /// Function will panic if `FileHandle` is not valid.
    pub(crate) fn close_file(&mut self, now: Tick, handle: FileHandle) {
        self.advance_time(now);

        if let Some(Node::File { file, .. }) = self.nodes.get_mut(&handle.inode) {
            file.close(now);
        } else {
            panic!("Invalid file handle");
        }
    }

    /// Advance time in the model.
    ///
    /// # Panics
    ///
    /// Will panic if passed `now` is less than recorded `now`. Time can only
    /// advance.
    pub(crate) fn advance_time(&mut self, now: Tick) {
        assert!(now >= self.now);
        // We have to simulate all ticks that happen between self.now and now,
        // else an observer will detect a sudden shift in the model. This is
        // still possible by careful observation of the metadata for files
        // updating only on tick boundaries, but it's better than shifts only
        // when observation happens.
        while self.now < now {
            self.now += 1;
            self.advance_time_inner(self.now);
        }
    }

    #[inline]
    fn advance_time_inner(&mut self, now: Tick) {
        assert!(now >= self.now);

        // Compute new global bytes_per_tick, at now - 1.
        let elapsed_ticks = now.saturating_sub(self.initial_tick).saturating_sub(1);
        let bytes_per_tick = match &self.load_profile {
            LoadProfile::Constant(bytes) => *bytes,
            LoadProfile::Linear { start, rate } => {
                start.saturating_add(rate.saturating_mul(elapsed_ticks))
            }
        };

        // Update each File's bytes_per_tick but do not advance time, as that is
        // done later.
        for node in self.nodes.values_mut() {
            if let Node::File { file } = node {
                if !file.read_only && !file.unlinked {
                    file.bytes_per_tick = bytes_per_tick;
                }
            }
        }

        for inode in self.nodes.keys() {
            self.inode_scratch.push(*inode);
        }

        for inode in self.inode_scratch.drain(..) {
            let (rotated_inode, parent_inode, group_id, ordinal) = {
                // If the node pointed to by inode doesn't exist, that's a
                // catastrophic programming error. We just copied all inode to node
                // pairs.
                let node = self
                    .nodes
                    .get_mut(&inode)
                    .expect("inode not associated with node");

                // Only process files.
                let file = match node {
                    Node::File { file } => file,
                    Node::Directory { .. } => continue,
                };

                // If the file is read-only we have no more work to do on this file
                // although it _may_ be touched if we process a peer chain below.
                if file.read_only() {
                    continue;
                }

                // If the file is available for writing we advance time for the file.
                file.advance_time(now);

                // Determine if the file pointed to by inode needs to be rotated. A
                // file is only rotated if it is linked, that is, it has a name in
                // the filesystem.
                if file.bytes_written < self.max_bytes_per_file {
                    continue;
                }
                assert!(
                    file.ordinal() == 0,
                    "Expected rotated file to be 0th ordinal, was {}",
                    file.ordinal()
                );
                file.set_read_only(now);

                // Rotation data needed below.
                (inode, file.parent, file.group_id, file.ordinal)
            };

            // Create our new file, called, well, `new_file`. This will
            // become the 0th ordinal in the `group_id` and may -- although
            // we don't know yet -- cause `rotated_inode` to be deleted.
            //
            // Set bytes_per_tick to current and now to now-1 else we'll never
            // ramp properly.
            let new_file_inode = self.next_inode;
            let mut new_file = File::new(
                parent_inode,
                group_id,
                bytes_per_tick,
                self.now.saturating_sub(1),
                Some(rotated_inode),
            );

            new_file.advance_time(now);
            self.next_inode = self.next_inode.saturating_add(1);

            // Insert `new_file` into the node list and make it a member of
            // its directory's children.
            self.nodes
                .insert(new_file_inode, Node::File { file: new_file });
            if let Some(Node::Directory { dir, .. }) = self.nodes.get_mut(&parent_inode) {
                dir.children.insert(new_file_inode);
            }

            // Now, we step through the list of peers beginning with
            // `rotated_inode` and increment the ordinal of each file we
            // find. This tells us if the file falls off the end of the peer
            // chain or not. If it does, the file will be unlinked. If the
            // file is unlinked and has no active file handles it is removed
            // from the node map.
            //
            // We do not preserve unlinked files in the peer chain. We
            // likewise do not preserve unlinked files in its parent
            // directory. We _do_ preserve unlinked files in the node map
            // until the last file handle is removed for that node.

            let mut current_inode = rotated_inode;
            assert!(ordinal == 0, "Expected ordinal 0, got {ordinal}");
            let mut prev_inode = new_file_inode;

            loop {
                // Increment the current_inode's ordinal and determine if
                // the ordinal is now past max_rotations and whether the
                // next peer needs to be followed.
                let node = self.nodes.get_mut(&current_inode).expect("Node must exist");
                let (remove_current, next_peer) = match node {
                    Node::File { file } => {
                        file.incr_ordinal();
                        counter!("log_file_rotated").increment(1);

                        let remove_current = file.ordinal() > self.max_rotations;
                        (remove_current, file.peer)
                    }
                    Node::Directory { .. } => panic!("Expected a File node"),
                };

                if remove_current {
                    // The only time a node is removed is when it's at the end
                    // of the peer chain. This means that next_peer is None and
                    // there are no further peers to explore.
                    assert!(
                        next_peer.is_none(),
                        "next_peer must be None when removing a node, else not end of peer chain"
                    );

                    // Because we are at the end of the peer chain we remove
                    // the node from its parent directory and unlink it.
                    if let Some(Node::Directory { dir, .. }) = self.nodes.get_mut(&parent_inode) {
                        dir.children.remove(&current_inode);
                    }
                    if let Some(Node::File { file }) = self.nodes.get_mut(&current_inode) {
                        file.unlink(now);
                    }

                    // Update the peer of the previous file to None
                    let node = self.nodes.get_mut(&prev_inode).expect("Node must exist");
                    if let Node::File { file } = node {
                        file.peer = None;
                    }

                    // Break the loop, as there are no further peers to process
                    break;
                }

                // Move to the next peer
                //
                // The next_peer is only None in the `remove_current`
                // branch, meaning that we only reach this point if the next
                // peer is Some.
                prev_inode = current_inode;
                if let Some(next_peer) = next_peer {
                    current_inode = next_peer;
                } else {
                    // We're at the end of the rotated files but not so many
                    // it's time to rotate off.
                    break;
                }
            }
        }

        self.gc();
    }

    // Garbage collect unlinked files with no open handles, calculating the bytes
    // lost from these files.
    #[tracing::instrument(skip(self))]
    fn gc(&mut self) {
        let mut to_remove = Vec::new();
        for (&inode, node) in &self.nodes {
            if let Node::File { file } = node {
                if file.unlinked && file.open_handles == 0 {
                    to_remove.push(inode);
                }
            }
        }
        for inode in to_remove {
            if let Some(Node::File { file }) = self.nodes.remove(&inode) {
                let lost_bytes = file.bytes_written.saturating_sub(file.max_offset_observed);
                info!("Log file deleted. Total bytes lost: {lost_bytes}. Total bytes written: {bytes_written}. Total bytes read: {bytes_read}. Group ID: {group_id}. Created: {created_tick}.",
                      group_id = file.group_id,
                      created_tick = file.created_tick,
                      bytes_written = file.bytes_written,
                      bytes_read = file.bytes_read);
                counter!("log_file_deleted").increment(1);
                counter!("lost_bytes", "group_id" => format!("{}", file.group_id))
                    .increment(lost_bytes);
            }
        }
    }

    /// Look up the Inode for a given `name`.
    ///
    /// This function searches under `parent_inode` for a match to `name`,
    /// returning any inode that happens to match. Time will be advanced to
    /// `now`.
    #[tracing::instrument(skip(self))]
    pub(crate) fn lookup(&mut self, now: Tick, parent_inode: Inode, name: &str) -> Option<Inode> {
        self.advance_time(now);

        if let Some(Node::Directory { dir, .. }) = self.nodes.get(&parent_inode) {
            for &child_inode in &dir.children {
                if let Some(node) = self.nodes.get(&child_inode) {
                    let child_name = match node {
                        Node::File { file } => {
                            &self.group_names[file.group_id as usize][file.ordinal as usize]
                        }
                        Node::Directory { name, .. } => name,
                    };
                    if child_name == name {
                        return Some(child_inode);
                    }
                }
            }
        }
        None
    }

    /// Look up the attributes for an `Inode`.
    ///
    /// Time will be advanced to `now`.
    #[tracing::instrument(skip(self))]
    pub(crate) fn getattr(&mut self, now: Tick, inode: Inode) -> Option<NodeAttributes> {
        self.advance_time(now);

        self.nodes.get(&inode).map(|node| match node {
            Node::File { file, .. } => NodeAttributes {
                inode,
                kind: NodeType::File,
                size: file.bytes_written,
                access_tick: file.access_tick,
                modified_tick: file.modified_tick,
                status_tick: file.status_tick,
                created_tick: file.created_tick,
            },
            Node::Directory { .. } => NodeAttributes {
                inode,
                kind: NodeType::Directory,
                size: 0,
                access_tick: self.now,
                modified_tick: self.now,
                status_tick: self.now,
                created_tick: self.now,
            },
        })
    }

    /// Read `size` bytes from `inode`.
    ///
    /// An attempt will be made to read `size` bytes at time `tick` -- time will
    /// be advanced -- and a slice up to `size` bytes will be returned or `None`
    /// if no bytes are available to be read.
    #[tracing::instrument(skip(self))]
    pub(crate) fn read(
        &mut self,
        file_handle: FileHandle,
        offset: usize,
        size: usize,
        now: Tick,
    ) -> Option<Bytes> {
        self.advance_time(now);

        let inode = file_handle.inode;
        match self.nodes.get_mut(&inode) {
            Some(Node::File { ref mut file }) => {
                let bytes_written = usize::try_from(file.bytes_written)
                    .expect("more bytes written than machine word");

                if offset >= bytes_written {
                    // Offset beyond EOF
                    return Some(Bytes::new());
                }

                let available = bytes_written.saturating_sub(offset);
                let to_read = available.min(size);

                let end_offset = offset as u64 + to_read as u64;
                file.max_offset_observed = file.max_offset_observed.max(end_offset);

                // Get data from block_cache without worrying about blocks
                let data = self.block_cache.read_at(offset as u64, to_read);
                assert!(data.len() == to_read, "Data returned from block_cache is distinct from the read size: {l} != {to_read}", l = data.len());

                file.read(to_read as u64, now);

                Some(data)
            }
            Some(Node::Directory { .. }) | None => None,
        }
    }

    /// Read inodes from a directory.
    ///
    /// Returns None if the inode is a `File`, else returns the hashset of
    /// children inodes. Guaranteed to be in the same order so long as time does
    /// not advance.
    ///
    /// Function does not advance time in the model.
    #[tracing::instrument(skip(self))]
    pub(crate) fn readdir(&self, inode: Inode) -> Option<&BTreeSet<Inode>> {
        if let Some(Node::Directory { dir, .. }) = self.nodes.get(&inode) {
            Some(&dir.children)
        } else {
            None
        }
    }

    /// Get the fuser file type of an inode if it exists
    #[tracing::instrument(skip(self))]
    pub(crate) fn get_file_type(&self, inode: Inode) -> Option<fuser::FileType> {
        self.nodes.get(&inode).map(|node| match node {
            Node::Directory { .. } => fuser::FileType::Directory,
            Node::File { .. } => fuser::FileType::RegularFile,
        })
    }

    /// Return the name of the inode if it exists
    #[tracing::instrument(skip(self))]
    pub(crate) fn get_name(&self, inode: Inode) -> Option<&str> {
        self.nodes
            .get(&inode)
            .map(|node| match node {
                Node::Directory { name, .. } => name,
                Node::File { file } => {
                    &self.group_names[file.group_id as usize][file.ordinal as usize]
                }
            })
            .map(String::as_str)
    }

    /// Return the parent inode of an inode, if it exists
    #[tracing::instrument(skip(self))]
    pub(crate) fn get_parent_inode(&self, inode: Inode) -> Option<Inode> {
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
    pub(crate) fn root_inode(&self) -> Inode {
        self.root_inode
    }

    /// Return the number of links for the inode.
    #[must_use]
    pub(crate) fn nlink(&self, inode: Inode) -> usize {
        if let Some(Node::Directory { dir, .. }) = self.nodes.get(&inode) {
            let subdirectory_count = dir
                .children
                .iter()
                .filter(|child_inode| {
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

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroU32,
    };

    use super::{FileHandle, Inode, LoadProfile, Node, State, Tick};
    use lading_payload::block;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};

    /// Our testing strategy is to drive the State as if in a filesystem. The
    /// crux is the Operation enum that defines which parts of the State are
    /// exercised and how. Invariant properties are tested after each operation,
    /// meaning we drive the model forward normally and assure at every step
    /// that it's in good order.

    #[derive(Debug, Clone)]
    enum Operation {
        Open,
        Close,
        Read { offset: usize, size: usize },
        Lookup { name: Option<String> },
        GetAttr,
        Wait { ticks: u64 },
    }

    impl Arbitrary for Operation {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            let open_op = Just(Operation::Open);

            let close_op = Just(Operation::Close);

            let read_op = (0usize..1024usize, 1usize..1024usize)
                .prop_map(|(offset, size)| Operation::Read { offset, size });

            let lookup_op = (any::<Option<String>>()).prop_map(|name| Operation::Lookup { name });

            let getattr_op = Just(Operation::GetAttr);

            let wait_op = (0u64..=100u64).prop_map(|ticks| Operation::Wait { ticks });

            prop_oneof![wait_op, getattr_op, lookup_op, read_op, open_op, close_op].boxed()
        }
    }

    impl Arbitrary for LoadProfile {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            let constant_strategy = (1u64..=10_000u64).prop_map(LoadProfile::Constant);
            let linear_strategy = (1u64..=1_000u64, 1u64..=100u64)
                .prop_map(|(start, rate)| LoadProfile::Linear { start, rate });
            prop_oneof![constant_strategy, linear_strategy].boxed()
        }
    }

    impl Arbitrary for State {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (
                any::<u64>(),         // seed
                1u8..=8u8,            // max_rotations
                1u64..=100_000u64,    // max_bytes_per_file
                1u8..=4u8,            // max_depth
                1u16..=16u16,         // concurrent_logs
                1u64..=1000u64,       // initial_tick
                any::<LoadProfile>(), // load_profile
            )
                .prop_map(
                    |(
                        seed,
                        max_rotations,
                        max_bytes_per_file,
                        max_depth,
                        concurrent_logs,
                        initial_tick,
                        load_profile,
                    )| {
                        let mut rng = StdRng::seed_from_u64(seed);
                        let block_cache = block::Cache::fixed(
                            &mut rng,
                            NonZeroU32::new(1_000_000).expect("zero value"),
                            10_000,
                            &lading_payload::Config::Ascii,
                        )
                        .expect("block construction");

                        State::new(
                            &mut rng,
                            initial_tick,
                            max_rotations,
                            max_bytes_per_file,
                            block_cache,
                            max_depth,
                            concurrent_logs,
                            load_profile,
                        )
                    },
                )
                .boxed()
        }
    }

    fn random_inode<R>(rng: &mut R, state: &State) -> Inode
    where
        R: Rng,
    {
        if state.nodes.is_empty() {
            state.root_inode
        } else {
            *state.nodes.keys().choose(rng).unwrap_or(&state.root_inode)
        }
    }

    fn random_name<R>(rng: &mut R, state: &State) -> String
    where
        R: Rng,
    {
        let names: Vec<String> = state
            .nodes
            .values()
            .filter_map(|node| match node {
                Node::Directory { name, .. } => Some(name.clone()),
                Node::File { file } => {
                    let group_names = state.group_names.get(file.group_id as usize)?;
                    group_names.get(file.ordinal() as usize).cloned()
                }
            })
            .collect();

        names.into_iter().choose(rng).unwrap()
    }

    fn assert_state_properties(state: &State) {
        // Property 1: bytes_written >= max_offset_observed
        //
        // While a caller can read the same bytes multiple times they cannot
        // read past the maximum bytes available.
        for node in state.nodes.values() {
            if let Node::File { file } = node {
                assert!(
                    file.bytes_written >= file.max_offset_observed,
                    "bytes_written ({}) < max_offset_observed ({})",
                    file.bytes_written,
                    file.max_offset_observed,
                );
            }
        }

        // Property 2: status_tick == modified_tick || status_tick == access_tick
        for node in state.nodes.values() {
            if let Node::File { file } = node {
                assert!(
                    file.status_tick == file.modified_tick || file.status_tick == file.access_tick,
                    "status_tick ({}) != modified_tick ({}) or access_tick ({})",
                    file.status_tick,
                    file.modified_tick,
                    file.access_tick
                );
            }
        }

        // Property 3: Correct peer chain
        //
        // A peer chain should always begin with the 0th member of a group and
        // proceed until the last member of that group. No peer should be
        // unlinked and the ordinal of each peer should increase as we move down
        // the chain.
        for node in state.nodes.values() {
            if let Node::File { file } = node {
                if file.ordinal == 0 {
                    // Start of a peer chain
                    let mut current_file = file;
                    let mut expected_ordinal = current_file.ordinal;
                    let mut seen_inodes = HashSet::new();

                    while let Some(peer_inode) = current_file.peer {
                        if !seen_inodes.insert(peer_inode) {
                            panic!("Cycle detected in peer chain at inode {peer_inode}",);
                        }

                        if let Some(Node::File { file: peer_file }) = state.nodes.get(&peer_inode) {
                            assert!(!peer_file.unlinked, "File was found in peer chain unlinked");
                            expected_ordinal += 1;
                            assert_eq!(
                                peer_file.ordinal,
                                expected_ordinal,
                                "Expected ordinal {expected_ordinal}, got {peer_file_ordinal}",
                                peer_file_ordinal = peer_file.ordinal
                            );
                            current_file = peer_file;
                        } else {
                            panic!("Peer inode {peer_inode} does not exist or is not a file");
                        }
                    }
                }
            }
        }

        // Property 4: Ordinal values within bounds
        //
        // No ordinal should exeed max_rotations, so long as the file is linked.
        for node in state.nodes.values() {
            if let Node::File { file } = node {
                if file.unlinked {
                    continue;
                }
                assert!(
                    file.ordinal <= state.max_rotations,
                    "Ordinal {ordinal} exceeds max_rotations {max_rotations}: {state:#?}",
                    ordinal = file.ordinal,
                    max_rotations = state.max_rotations
                );
            }
        }

        // Property 5: No orphaned files
        //
        // Every node held by the filesystem is either a linked file, an
        // unlinked file or a directory. A linked file appears in peer chains
        // and is subject to rotation. An unlinked file has no name, is not
        // subject to rotation and does not appear in a peer chain. An unlinked
        // file is orphaned only when it has no open file handles. A directory
        // holds linked and unlinked files.
        for (&inode, node) in &state.nodes {
            if let Node::File { file } = node {
                if file.unlinked && file.open_handles == 0 {
                    panic!(
                        "Found orphaned file inode {} (unlinked with zero open handles)",
                        inode
                    );
                }
            }
        }

        // Property 6: Correct names corresponding to ordinals in linked files
        //
        // For every linked file the ordinal of said file should correspond to
        // the correct name.
        for (&inode, node) in &state.nodes {
            if let Node::File { file } = node {
                if file.unlinked {
                    continue;
                }
                if let Some(names) = state.group_names.get(file.group_id as usize) {
                    if let Some(expected_name) = names.get(file.ordinal as usize) {
                        let actual_name = state.get_name(inode).unwrap_or("");
                        assert_eq!(
                            actual_name,
                            expected_name.as_str(),
                            "Inode {inode} name mismatch: expected {expected_name}, got {actual_name}",
                        );
                    } else {
                        panic!(
                            "Ordinal {ordinal} is out of bounds in group_names",
                            ordinal = file.ordinal
                        );
                    }
                } else {
                    panic!(
                        "Group ID {group_id} is not present in group_names",
                        group_id = file.group_id
                    );
                }
            }
        }

        // Property 7: bytes_written are tick accurate
        for (&inode, node) in &state.nodes {
            if let Node::File { file } = node {
                let end_tick = file.read_only_since.unwrap_or(state.now);
                let expected_bytes = compute_expected_bytes_written(
                    &state.load_profile,
                    state.initial_tick,
                    file.created_tick,
                    end_tick,
                );
                assert_eq!(
                    file.bytes_written,
                    expected_bytes,
                    "bytes_written ({}) does not match expected_bytes_written ({expected_bytes}) for file with inode {inode}",
                    file.bytes_written,
                );
            }
        }

        // Property 8: Rotated files have bytes_written within acceptable range
        //
        // For a rotated file (read_only == true), bytes_written should be
        // within max_bytes_per_file <= bytes_written < (max_bytes_per_file + 2
        // * bytes_per_tick). It's possible because of when rotation is done
        // that a full tick will elapse, allowing an additional tick worth of
        // bytes to be written, hence the 2x.
        for node in state.nodes.values() {
            if let Node::File { file } = node {
                if !file.read_only {
                    continue;
                }
                let min_size = state.max_bytes_per_file;
                let max_size = state
                    .max_bytes_per_file
                    .saturating_add(2 * file.bytes_per_tick);
                assert!(
                    file.bytes_written >= min_size && file.bytes_written <= max_size,
                    "Rotated file size {actual} not in expected range [{min_size}, {max_size}]",
                    actual = file.bytes_written
                );
            }
        }
    }

    fn compute_expected_bytes_written(
        load_profile: &LoadProfile,
        initial_tick: Tick,
        created_tick: Tick,
        end_tick: Tick,
    ) -> u64 {
        let start_tick = created_tick.max(initial_tick);
        let end_tick = end_tick.max(start_tick);
        let duration = end_tick - start_tick;

        match load_profile {
            LoadProfile::Constant(bytes_per_tick) => bytes_per_tick.saturating_mul(duration),
            LoadProfile::Linear { start, rate } => {
                // bytes_per_tick at time t is start + rate * (t - initial_tick)
                // total_bytes = sum_{t = start_tick}^{end_tick - 1} bytes_per_tick at t
                // Simplify the sum:
                // total_bytes = duration * start + rate * sum_{t = start_tick}^{end_tick - 1} (t - initial_tick)
                // sum_{t = start_tick}^{end_tick - 1} (t - initial_tick) = sum_{k = start_tick - initial_tick}^{end_tick - 1 - initial_tick} k
                let a = start_tick.saturating_sub(initial_tick);
                let b = end_tick.saturating_sub(1).saturating_sub(initial_tick);
                let num_terms = b.saturating_sub(a).saturating_add(1);
                let sum_of_terms = num_terms.saturating_mul(a + b) / 2;

                let total_bytes = duration
                    .saturating_mul(*start)
                    .saturating_add(rate.saturating_mul(sum_of_terms));
                total_bytes
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            // Increase the number of generated cases (default is 256)
            cases: 1_024,
            // Allow more shrink iterations (default is 4096)
            max_shrink_iters: 1_000_000,
            max_shrink_time: 300_000, // five minutes
            .. ProptestConfig::default()
        })]

        #[test]
        fn test_state_operations(seed in any::<u64>(),
                                 state in any::<State>(),
                                 operations in vec(any::<Operation>(), 1..100)) {
            test_state_operations_inner(seed, state, operations)
        }
    }

    #[inline]
    fn test_state_operations_inner(seed: u64, mut state: State, operations: Vec<Operation>) {
        let mut rng = StdRng::seed_from_u64(seed);
        // Assert that the state is well-formed before we begin
        assert_state_properties(&state);

        let mut now = state.now;
        let mut open_handles: HashMap<Inode, FileHandle> = HashMap::new();

        for op in operations {
            match op {
                Operation::Open => {
                    let inode = random_inode(&mut rng, &state);
                    if let Some(handle) = state.open_file(now, inode) {
                        assert!(handle.inode == inode);
                        open_handles.insert(handle.inode, handle);
                    }
                }
                Operation::Close => {
                    // Only attempt to close if there's an open handle available
                    if !open_handles.is_empty() {
                        let inode = *open_handles
                            .keys()
                            .choose(&mut rng)
                            .expect("open_handles should not be empty");
                        state.close_file(
                            now,
                            open_handles
                                .remove(&inode)
                                .expect("[close] file handle must exist"),
                        );
                    }
                }
                Operation::Read { offset, size } => {
                    let inode = random_inode(&mut rng, &state);
                    // Read from an open file
                    if let Some(handle) = open_handles.get(&inode) {
                        if state.nodes.contains_key(&handle.inode) {
                            let _ = state.read(*handle, offset, size, now);
                        } else {
                            panic!("Attempted to read from an invalid or already removed inode: {inode}", inode = handle.inode);
                        }
                    }
                }
                Operation::Lookup { name: op_name } => {
                    let parent_inode = random_inode(&mut rng, &state);
                    if state.nodes.contains_key(&parent_inode) {
                        let name = op_name.unwrap_or_else(|| random_name(&mut rng, &state));
                        let _ = state.lookup(now, parent_inode, &name);
                    } else {
                        panic!("Attempted to look up a name in an invalid or already removed parent inode: {parent_inode}");
                    }
                }
                Operation::GetAttr => {
                    let inode = random_inode(&mut rng, &state);
                    if state.nodes.contains_key(&inode) {
                        let _ = state.getattr(now, inode);
                    } else {
                        panic!("Attempted to get attributes of an invalid or already removed inode: {inode}");
                    }
                }
                Operation::Wait { ticks } => {
                    now += ticks;
                    state.advance_time(now);
                }
            }

            // After each operation, assert that the properties hold. Note that
            // because GC is lazy -- happens only when time advances -- we force
            // it to run.
            state.gc();
            assert_state_properties(&state);
        }
    }
}
