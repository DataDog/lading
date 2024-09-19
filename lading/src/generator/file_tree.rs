//! The file tree generator.
//!
//! Unlike the other generators the file generator does not "connect" however
//! loosely to the target but instead, without coordination, merely generates
//! a file tree and generates random access/rename operations.
//!
//! ## Metrics
//!
//! This generator does not emit any metrics. Some metrics may be emitted by the
//! configured [throttle].
//!

use lading_throttle::Throttle;
use rand::{
    distributions::{Alphanumeric, DistString},
    seq::SliceRandom,
};
use std::{
    collections::VecDeque,
    num::{NonZeroU32, NonZeroUsize},
    path,
    path::Path,
    path::PathBuf,
    str,
};

use rand::{prelude::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::{fs::create_dir, fs::rename, fs::File};
use tracing::info;

static FILE_EXTENSION: &str = "txt";

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`FileTree`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("Io error: {0}")]
    Io(::std::io::Error),
    /// Folder has no parent error
    #[error("Folder has no parent")]
    Parent,
    /// Error converting path to string
    #[error("Error converting path to string")]
    ToStr,
}

impl From<::std::io::Error> for Error {
    fn from(error: ::std::io::Error) -> Self {
        Error::Io(error)
    }
}

fn default_max_depth() -> NonZeroUsize {
    NonZeroUsize::new(10).expect("default max depth given was 0")
}

fn default_max_sub_folders() -> NonZeroU32 {
    NonZeroU32::new(5).expect("default max sub folders given was 0")
}

fn default_max_files() -> NonZeroU32 {
    NonZeroU32::new(5).expect("default max files given was 0")
}

fn default_max_nodes() -> NonZeroUsize {
    NonZeroUsize::new(100).expect("default max nodes given was 0")
}

fn default_name_len() -> NonZeroUsize {
    NonZeroUsize::new(8).expect("default name len given was 0")
}

fn default_open_per_second() -> NonZeroU32 {
    NonZeroU32::new(8).expect("default open per second given was 0")
}

fn default_rename_per_name() -> NonZeroU32 {
    NonZeroU32::new(1).expect("default rename per second given was 0")
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`FileTree`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The maximum depth of the file tree
    #[serde(default = "default_max_depth")]
    pub max_depth: NonZeroUsize,
    /// The maximum number of sub folders
    #[serde(default = "default_max_sub_folders")]
    pub max_sub_folders: NonZeroU32,
    /// The maximum number of files per folder
    #[serde(default = "default_max_files")]
    pub max_files: NonZeroU32,
    /// The maximum number of nodes (folder/file) created
    #[serde(default = "default_max_nodes")]
    pub max_nodes: NonZeroUsize,
    /// The name length of the nodes (folder/file) without the extension
    #[serde(default = "default_name_len")]
    pub name_len: NonZeroUsize,
    /// The root folder where the nodes will be created
    pub root: String,
    /// The number of nodes opened per second
    #[serde(default = "default_open_per_second")]
    pub open_per_second: NonZeroU32,
    #[serde(default = "default_rename_per_name")]
    /// The number of rename per second
    pub rename_per_second: NonZeroU32,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

#[derive(Debug)]
/// The file tree generator.
///
/// This generator generates a file tree and generates random access/rename operations,
/// this without coordination to the target.
pub struct FileTree {
    name_len: NonZeroUsize,
    open_throttle: Throttle,
    rename_throttle: Throttle,
    total_folder: usize,
    nodes: VecDeque<PathBuf>,
    rng: StdRng,
    shutdown: lading_signal::Watcher,
}

impl FileTree {
    /// Create a new [`FileTree`]
    ///
    /// # Errors
    ///
    /// Creation will fail if the target file/folder cannot be opened for writing.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: &Config, shutdown: lading_signal::Watcher) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let (nodes, _total_files, total_folder) = generate_tree(&mut rng, config)?;

        let _labels = [
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "file_tree".to_string()),
        ];

        let open_throttle = Throttle::new_with_config(config.throttle, config.open_per_second);
        let rename_throttle = Throttle::new_with_config(config.throttle, config.rename_per_second);
        Ok(Self {
            name_len: config.name_len,
            open_throttle,
            rename_throttle,
            total_folder,
            nodes,
            rng,
            shutdown,
        })
    }

    /// Run [`FileTree`] to completion or until a shutdown signal is received.
    ///
    /// In this loop the file tree will be generated and accessed.
    ///
    /// # Errors
    ///
    /// This function will terminate with an error if file permissions are not
    /// correct, if the file cannot be written to etc. Any error from
    /// `std::io::Error` is possible.
    ///
    /// # Panics
    ///
    /// Function will panic if one node is not path is not populated properly
    pub async fn spin(mut self) -> Result<(), Error> {
        let mut iter = self.nodes.iter().cycle();
        let mut folders = Vec::with_capacity(self.total_folder);

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                _ = self.open_throttle.wait() => {
                    let node = iter.next().expect("node is not populated properly");
                    if node.exists() {
                        File::open(node.as_path()).await?;
                    } else {
                        create_node(node).await?;

                        if is_folder(node) {
                            folders.push(node);
                        }
                    }
                },
                _ = self.rename_throttle.wait() => {
                    if let Some(folder) = folders.choose_mut(&mut self.rng) {
                        rename_folder(&mut self.rng, folder, self.name_len.get()).await?;
                    }
                }
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    break;
                },
            }
        }
        Ok(())
    }
}

fn generate_tree(
    rng: &mut StdRng,
    config: &Config,
) -> Result<(VecDeque<PathBuf>, usize, usize), Error> {
    let mut nodes = VecDeque::new();

    let root_depth = config.root.matches(path::MAIN_SEPARATOR).count();

    let mut total_file: usize = 0;
    let mut total_folder: usize = 0;

    let mut stack = Vec::new();
    stack.push(PathBuf::from(config.root.clone()));

    loop {
        if let Some(node) = stack.pop() {
            if is_folder(&node) {
                // generate files
                for _n in 0..config.max_files.get() {
                    if total_file + total_folder < config.max_nodes.get() {
                        let file = rnd_file_name(rng, &node, config.name_len.get());
                        stack.push(file);
                        total_file += 1;
                    }
                }

                let curr_depth = node
                    .to_str()
                    .ok_or(Error::ToStr)?
                    .matches(path::MAIN_SEPARATOR)
                    .count()
                    - root_depth;

                // generate sub folders
                if curr_depth < config.max_depth.get() {
                    for _n in 0..config.max_sub_folders.get() {
                        if total_file + total_folder < config.max_nodes.get() {
                            let dir = rnd_node_name(rng, &node, config.name_len.get());
                            stack.push(dir);
                            total_folder += 1;
                        }
                    }
                }
            }

            nodes.push_back(node);
        } else {
            return Ok((nodes, total_file, total_folder));
        }
    }
}

#[inline]
async fn rename_folder(rng: &mut StdRng, folder: &Path, len: usize) -> Result<(), Error> {
    let parent = PathBuf::from(folder.parent().ok_or(Error::Parent)?);
    let dir = rnd_node_name(rng, &parent, len);

    // rename twice to keep the original folder name so that future file access
    // in the folder will still work
    rename(&folder, &dir).await?;
    rename(&dir, &folder).await?;
    Ok(())
}

#[inline]
fn is_folder(node: &Path) -> bool {
    node.extension().is_none()
}

#[inline]
async fn create_node(node: &Path) -> Result<(), Error> {
    if is_folder(node) {
        create_dir(node).await?;
    } else {
        File::create(node).await?;
    }
    Ok(())
}

#[inline]
fn rnd_node_name(rng: &mut StdRng, dir: &Path, len: usize) -> PathBuf {
    let name = Alphanumeric.sample_string(rng, len);
    dir.join(name)
}

#[inline]
fn rnd_file_name(rng: &mut StdRng, dir: &Path, len: usize) -> PathBuf {
    let mut node = rnd_node_name(rng, dir, len);
    node.set_extension(FILE_EXTENSION);
    node
}
