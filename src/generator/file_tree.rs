//! The file tree generator.
//!
//! Unlike the other generators the file generator does not "connect" however
//! losely to the target but instead, without coordination, merely generates
//! a file tree and generates random acess/rename operations.

use rand::{
    distributions::{Alphanumeric, DistString},
    seq::SliceRandom,
};
use std::{
    num::{NonZeroU32, NonZeroUsize},
    path,
    path::Path,
    path::PathBuf,
    str,
};

use governor::{Quota, RateLimiter};
use rand::{prelude::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::{fs::create_dir, fs::rename, fs::File};
use tracing::{debug, info};

use crate::signals::Shutdown;

static FILE_EXTENSION: &str = "txt";

#[derive(Debug)]
/// Errors produced by [`FileTree`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    Io(::std::io::Error),
}

impl From<::std::io::Error> for Error {
    fn from(error: ::std::io::Error) -> Self {
        Error::Io(error)
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
/// Configuration of [`FileTree`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The maximum depth of the file tree
    pub max_depth: NonZeroUsize,
    /// The maximum number of sub folders
    pub max_sub_folders: NonZeroU32,
    /// The maximum number of files per folder
    pub max_files: NonZeroU32,
    /// The maximum number of nodes (folder/file) created
    pub max_nodes: NonZeroU32,
    /// The name length of the nodes (folder/file) without the extension
    pub name_len: NonZeroUsize,
    /// The root folder where the nodes will be created
    pub root: String,
    /// The number of nodes created/accessed per second
    pub node_per_second: NonZeroU32,
    /// The number of rename per second
    pub rename_per_second: NonZeroU32,
}

#[derive(Debug)]
/// The file tree generator.
///
/// This generator generates a file tree and generates random access/rename operations,
/// this without coordination to the target.
pub struct FileTree {
    max_depth: NonZeroUsize,

    max_sub_folders: NonZeroU32,
    max_files: NonZeroU32,
    max_nodes: NonZeroU32,
    name_len: NonZeroUsize,
    root: String,
    node_per_second: NonZeroU32,
    rename_per_second: NonZeroU32,
    rng: StdRng,
    shutdown: Shutdown,
}

impl FileTree {
    /// Create a new [`FileTree`]
    ///
    /// # Errors
    ///
    /// Creation will fail if the target file/folder cannot be opened for writing.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let rng = StdRng::from_seed(config.seed);

        Ok(Self {
            max_depth: config.max_depth,
            max_files: config.max_files,
            max_nodes: config.max_nodes,
            max_sub_folders: config.max_sub_folders,
            name_len: config.name_len,
            node_per_second: config.node_per_second,
            rename_per_second: config.rename_per_second,
            root: config.root,
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
        let node_rate_limiter = RateLimiter::direct(Quota::per_second(self.node_per_second));
        let rename_rate_limiter = RateLimiter::direct(Quota::per_second(self.rename_per_second));

        // store files and folders once generated for further access
        let mut files = Vec::new();
        let mut folders = Vec::new();

        let mut total_node = 0;
        let root_depth = self.root.matches(path::MAIN_SEPARATOR).count();

        // init the stack with the root node
        let mut stack = Vec::new();
        stack.push(PathBuf::from(self.root));

        loop {
            tokio::select! {
                _ = node_rate_limiter.until_ready() => {
                    match stack.pop() {
                        Some(node) => {
                            debug!("create {}", node.display());

                            // create the node. can be either a file or a folder
                            create_node(&node).await?;

                            if is_file(&node) {
                                files.push(node);
                            } else {
                                // generate files
                                for _n in 0..self.max_files.get() {
                                    if total_node < self.max_nodes.get() {
                                        let file = rnd_file_name(&mut self.rng, &node, self.name_len.get());
                                        stack.push(file);
                                        total_node += 1;
                                    }
                                }

                                let curr_depth = node.to_str().unwrap().matches(path::MAIN_SEPARATOR).count()-root_depth;

                                // generate sub folders
                                if curr_depth < self.max_depth.get() {
                                    for _n in 0..self.max_sub_folders.get() {
                                        if total_node < self.max_nodes.get() {
                                            let dir = rnd_node_name(&mut self.rng, &node, self.name_len.get());
                                            stack.push(dir);
                                            total_node += 1;
                                        }
                                    }
                                }
                                folders.push(node);
                            }
                        },
                        _ => {
                            // randomly generate an access
                            if let Some(node) = files.choose(&mut self.rng) {
                                debug!("access {}", node.display());

                                File::open(node.as_path()).await?;
                            }
                        }
                    }
                },
                _ = rename_rate_limiter.until_ready() => {
                    if let Some(folder) = folders.choose_mut(&mut self.rng) {
                        let parent = PathBuf::from(folder.parent().unwrap());
                        let dir = rnd_node_name(&mut self.rng, &parent, self.name_len.get());

                        debug!("rename {}", folder.display());
                        rename(&folder, &dir).await?;
                        rename(&dir, &folder).await?;
                    }
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    break;
                },
            }
        }
        Ok(())
    }
}

#[inline]
fn is_file(node: &Path) -> bool {
    node.extension().is_some()
}

#[inline]
async fn create_node(node: &Path) -> Result<(), Error> {
    if !node.exists() {
        if is_file(node) {
            File::create(node).await?;
        } else {
            create_dir(node).await?;
        }
    }
    Ok(())
}

#[inline]
fn rnd_node_name(rng: &mut StdRng, dir: &Path, len: usize) -> PathBuf {
    let name = Alphanumeric.sample_string(rng, len);
    let mut node = dir.to_path_buf();
    node.push(name);
    node
}

#[inline]
fn rnd_file_name(rng: &mut StdRng, dir: &Path, len: usize) -> PathBuf {
    let mut node = rnd_node_name(rng, dir, len);
    node.set_extension(FILE_EXTENSION);
    node
}
