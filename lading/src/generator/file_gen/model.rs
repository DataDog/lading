//! Model the internal logic of a logrotate filesystem.

//use lading_payload::block;

use std::collections::{HashMap, HashSet};

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

    /// The group ID of this File. So for instance all File instances that are
    /// called foo.log, foo.log.1 etc have the same group ID.
    group_id: u8,
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
    children: HashSet<Inode>,
    parent: Option<Inode>,
}

/// A filesystem object, either a `File` or a `Directory`.
#[derive(Debug)]
pub enum Node {
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
pub struct State {
    nodes: HashMap<Inode, Node>,
    root_inode: Inode,
    now: Tick,
    block_cache: block::Cache,
    max_bytes_per_file: u64,
    // [GroupID, [Names]]. The interior Vec have size `max_rotations`.
    group_names: Vec<Vec<String>>,
    next_inode: Inode,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("nodes", &self.nodes)
            .field("root_inode", &self.root_inode)
            .field("now", &self.now)
            // intentionally leaving out block_cache
            .field("max_rotations", &self.max_rotations)
            .field("max_bytes_per_file", &self.max_bytes_per_file)
            .field("group_names", &self.group_names)
            .field("next_inode", &self.next_inode)
            .finish()
    }
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
            children: HashSet::new(),
            parent: None,
        };
        root_dir.children.insert(logs_inode);
        nodes.insert(
            root_inode,
            Node::Directory {
                name: "/".to_string(),
                dir: root_dir,
            },
        );

        let mut logs_dir = Directory {
            children: HashSet::new(),
            parent: Some(root_inode),
        };
        logs_dir.children.insert(foo_log_inode);
        nodes.insert(
            logs_inode,
            Node::Directory {
                name: "logs".to_string(),
                dir: logs_dir,
            },
        );

        let mut group_names = Vec::new();

        // Create names for the rotation group
        let base_name = "foo.log".to_string();
        let mut names = Vec::new();
        names.push(base_name.clone()); // Ordinal 0
        for i in 1..=max_rotations {
            names.push(format!("foo.log.{i}")); // Ordinal i
        }
        group_names.push(names);

        let foo_log = File {
            parent: logs_inode,

            bytes_written: 0,
            bytes_read: 0,

            access_tick: 0,
            modified_tick: 0,
            status_tick: 0,

            bytes_per_tick,

            read_only: false,
            ordinal: 0,
            group_id: 0,
        };
        nodes.insert(foo_log_inode, Node::File { file: foo_log });

        // NOTE this structure is going to be a problem when I include rotating
        // files. Specifically the inodes will need to change so there might
        // need to be a concept of a SuperFile that holds inodes or something
        // for its rotating children? Dunno. An array with a current pointer?

        State {
            nodes,
            root_inode,
            now: 0,
            block_cache,
            max_bytes_per_file,
            group_names,
            next_inode: 4,
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

        assert!(now >= self.now);
        let mut inodes: Vec<Inode> = self.nodes.keys().copied().collect();

        for inode in inodes.drain(..) {
            let rotation_data = {
                println!("{nodes:?}", nodes = self.nodes);
                if let Some(node) = self.nodes.get_mut(&inode) {
                    match node {
                        Node::File { file } => {
                            file.advance_time(now);
                            if file.read_only() {
                                None
                            } else if file.size() >= self.max_bytes_per_file {
                                // File has exceeded its size, meaning it will be
                                // rotated. This starts a process that may end in a
                                // member of the file's group being deleted and the
                                // creation, certainly, of a new File instance in
                                // the group.
                                file.set_read_only();

                                Some((
                                    inode,
                                    file.parent,
                                    file.bytes_per_tick,
                                    file.group_id,
                                    file.ordinal(),
                                ))
                            } else {
                                None
                            }
                        }
                        Node::Directory { .. } => None,
                    }
                } else {
                    // Node has been removed, skip
                    continue;
                }
            };

            if let Some((rotated_inode, parent_inode, bytes_per_tick, group_id, ordinal)) =
                rotation_data
            {
                // Create our new File instance, using data from the now rotated file.
                let new_file = File {
                    parent: parent_inode,
                    bytes_written: 0,
                    bytes_read: 0,
                    access_tick: now,
                    modified_tick: now,
                    status_tick: now,
                    bytes_per_tick,
                    read_only: false,
                    ordinal: 0,
                    peer: Some(rotated_inode),
                    group_id,
                };

                // Insert `new_file` into the node list and make it a member of
                // its directory's children.
                self.nodes
                    .insert(self.next_inode, Node::File { file: new_file });
                if let Some(Node::Directory { dir, .. }) = self.nodes.get_mut(&parent_inode) {
                    dir.children.insert(self.next_inode);
                }

                // Bump the Inode index
                self.next_inode = self.next_inode.saturating_add(1);

                // Now, search through the peers of this File and rotate them,
                // keeping track of the last one which will need to be
                // deleted. There is no previous node as the rotated_inode is of
                // the 0th ordinal.
                let mut current_inode = rotated_inode;
                assert!(ordinal == 0);
                let mut prev_inode = None;

                loop {
                    let (remove_current, next_peer) = {
                        let node = self.nodes.get_mut(&current_inode).expect("Node must exist");
                        match node {
                            Node::File { file } => {
                                file.incr_ordinal();

                                let remove_current = file.ordinal() > self.max_rotations;
                                let next_peer = file.peer;
                                (remove_current, next_peer)
                            }
                            Node::Directory { .. } => panic!("Expected a File node"),
                        }
                    };

                    if remove_current {
                        // The only time a node is removed is when it's at the
                        // end of the line. This means that next_peer is None
                        // and there are no further peers to explore.
                        assert!(next_peer.is_none());

                        self.nodes.remove(&current_inode);
                        if let Some(Node::Directory { dir, .. }) = self.nodes.get_mut(&parent_inode)
                        {
                            dir.children.remove(&current_inode);
                        }

                        // Update the peer of the previous file to None
                        if let Some(prev_inode) = prev_inode {
                            let node = self.nodes.get_mut(&prev_inode).expect("Node must exist");
                            if let Node::File { file } = node {
                                file.peer = None;
                            }
                        }

                        break;
                    }

                    // Move to the next peer
                    //
                    // SAFETY: The next_peer is only None in the
                    // `remove_current` branch, meaning that we only reach this
                    // point if the next peer is Some.
                    prev_inode = Some(inode);
                    if next_peer.is_none() {
                        // We're at the end of the rotated files but not so many
                        // it's time to rotate off.
                        break;
                    }
                    current_inode = next_peer.expect("next peer must not be none");
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
    /// An attempt will be made to read `size` bytes at time `tick` -- time will
    /// be advanced -- and a slice up to `size` bytes will be returned or `None`
    /// if no bytes are available to be read.
    #[tracing::instrument(skip(self))]
    pub fn read(&mut self, inode: Inode, offset: usize, size: usize, now: Tick) -> Option<Bytes> {
        self.advance_time(now);

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
    pub fn readdir(&self, inode: Inode) -> Option<&HashSet<Inode>> {
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

    /// Return the name of the inode if it exists
    #[tracing::instrument(skip(self))]
    pub fn get_name(&self, inode: Inode) -> Option<&str> {
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

    use super::{Directory, File, Inode, Node, State};
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
        Read { offset: usize, size: usize },
        Lookup { name: Option<String> },
        GetAttr,
        Wait { ticks: u64 },
    }

    impl Arbitrary for Operation {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            let read_op = (any::<usize>(), 1usize..1024usize)
                .prop_map(|(offset, size)| Operation::Read { offset, size });

            let lookup_op = (any::<Option<String>>()).prop_map(|name| Operation::Lookup { name });

            let getattr_op = Just(Operation::GetAttr);

            let wait_op = (0u64..=1_000u64).prop_map(|ticks| Operation::Wait { ticks });

            prop_oneof![wait_op].boxed()
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
        // nothing yet
    }

    impl Arbitrary for State {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        // TODO maybe I get rid of this Arbitrary eventually and have everything driven by State::new
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            // We'll generate a valid initial State and a sequence of times to advance
            (
                1u64..=1000u64,          // initial now
                1u8..=10u8,              // max_rotations
                1024u64..=10_000_000u64, // max_bytes_per_file
                4usize..=100usize,       // next_inode (ensure it's at least 4)
            )
                .prop_map(
                    move |(now, max_rotations, max_bytes_per_file, next_inode)| {
                        let root_inode: Inode = 1;
                        let logs_inode: Inode = 2;
                        let foo_log_inode: Inode = 3;

                        let mut nodes = HashMap::new();

                        let mut root_dir = Directory {
                            children: HashSet::new(),
                            parent: None,
                        };
                        root_dir.children.insert(logs_inode);
                        nodes.insert(
                            root_inode,
                            Node::Directory {
                                name: "/".to_string(),
                                dir: root_dir,
                            },
                        );

                        let mut logs_dir = Directory {
                            children: HashSet::new(),
                            parent: Some(root_inode),
                        };
                        logs_dir.children.insert(foo_log_inode);
                        nodes.insert(
                            logs_inode,
                            Node::Directory {
                                name: "logs".to_string(),
                                dir: logs_dir,
                            },
                        );

                        let base_name = "foo.log".to_string();
                        let mut names = Vec::new();
                        names.push(base_name.clone()); // Ordinal 0
                        for i in 1..=max_rotations {
                            names.push(format!("foo.log.{i}")); // Ordinal i
                        }
                        let group_names = vec![names];

                        let foo_log = File {
                            parent: logs_inode,

                            bytes_written: 0,
                            bytes_read: 0,

                            access_tick: now,
                            modified_tick: now,
                            status_tick: now,

                            bytes_per_tick: 1024, // 1 KiB per tick
                            read_only: false,
                            peer: None,
                            ordinal: 0,
                            group_id: 0,
                        };
                        nodes.insert(foo_log_inode, Node::File { file: foo_log });

                        let mut rng = StdRng::seed_from_u64(1024);
                        let block_cache = block::Cache::fixed(
                            &mut rng,
                            NonZeroU32::new(1_000_000).expect("zero value"), // TODO make this an Error
                            10_000,                                          // 10 KiB
                            &lading_payload::Config::Ascii,
                        )
                        .expect("block construction"); // TODO make this an Error

                        State {
                            nodes,
                            root_inode,
                            now,
                            block_cache,
                            max_rotations,
                            max_bytes_per_file,
                            group_names,
                            next_inode,
                        }
                    },
                )
                .boxed()
        }
    }

    proptest! {
        #[test]
        fn test_state_operations(seed in any::<u64>(), mut state in any::<State>(), operations in vec(any::<Operation>(), 1..100)) {
            // Assert that the state is well-formed before we begin
            assert_state_properties(&state);

            let mut rng = StdRng::seed_from_u64(seed);
            let mut now = state.now;
            for op in operations {
                match op {
                    Operation::Read { offset, size } => {
                        let inode = random_inode(&mut rng, &state);
                        now += 1;
                        let _ = state.read(inode, offset, size, now);
                    },
                    Operation::Lookup { name: op_name } => {
                        let parent_inode = random_inode(&mut rng, &state);
                        let name = if let Some(n) = op_name {
                            n
                        } else {
                            random_name(&mut rng, &state)
                        };
                        now += 1;
                        let _ = state.lookup(now, parent_inode, &name);
                    },
                    Operation::GetAttr => {
                        let inode = random_inode(&mut rng, &state);
                        now += 1;
                        let _ = state.getattr(now, inode);
                    },
                    Operation::Wait { ticks } => {
                        now += ticks;
                        state.advance_time(now);
                    },
                }

                // After each operation, assert that the properties hold
                assert_state_properties(&state);
            }
        }
    }
}
