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
    collections::VecDeque,
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
use tracing::info;

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
    /// The number of nodes opened per second
    pub open_per_second: NonZeroU32,
    /// The number of rename per second
    pub rename_per_second: NonZeroU32,
}

#[derive(Debug)]
/// The file tree generator.
///
/// This generator generates a file tree and generates random access/rename operations,
/// this without coordination to the target.
pub struct FileTree {
    name_len: NonZeroUsize,
    open_per_second: NonZeroU32,
    rename_per_second: NonZeroU32,
    nodes: VecDeque<PathBuf>,
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
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let nodes = generate_tree(&mut rng, config);

        Ok(Self {
            name_len: config.name_len,
            open_per_second: config.open_per_second,
            rename_per_second: config.rename_per_second,
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
        let open_rate_limiter = RateLimiter::direct(Quota::per_second(self.open_per_second));
        let rename_rate_limiter = RateLimiter::direct(Quota::per_second(self.rename_per_second));

        // store files and folders once generated for further access
        let mut files = Vec::new();
        let mut folders = Vec::new();

        loop {
            tokio::select! {
                _ = open_rate_limiter.until_ready() => {
                    match self.nodes.pop_front() {
                        Some(node) => {
                            // create the node. can be either a file or a folder
                            create_node(&node).await?;

                            if is_file(&node) {
                                files.push(node);
                            } else {
                                folders.push(node);
                            }
                        },
                        _ => {
                            // randomly generate an access
                            if let Some(node) = files.choose(&mut self.rng) {
                                File::open(node.as_path()).await?;
                            }
                        }
                    }
                },
                _ = rename_rate_limiter.until_ready() => {
                    if let Some(folder) = folders.choose_mut(&mut self.rng) {
                        let parent = PathBuf::from(folder.parent().unwrap());
                        let dir = rnd_node_name(&mut self.rng, &parent, self.name_len.get());

                        // rename twice to keep the original folder name so that future file access
                        // in the folder will still work
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

fn generate_tree(rng: &mut StdRng, config: &Config) -> VecDeque<PathBuf> {
    let mut nodes = VecDeque::new();

    let mut total_node = 0;
    let root_depth = config.root.matches(path::MAIN_SEPARATOR).count();

    let mut stack = Vec::new();
    stack.push(PathBuf::from(config.root.clone()));

    loop {
        if let Some(node) = stack.pop() {
            if !is_file(&node) {
                // generate files
                for _n in 0..config.max_files.get() {
                    if total_node < config.max_nodes.get() {
                        let file = rnd_file_name(rng, &node, config.name_len.get());
                        stack.push(file);
                        total_node += 1;
                    }
                }

                let curr_depth =
                    node.to_str().unwrap().matches(path::MAIN_SEPARATOR).count() - root_depth;

                // generate sub folders
                if curr_depth < config.max_depth.get() {
                    for _n in 0..config.max_sub_folders.get() {
                        if total_node < config.max_nodes.get() {
                            let dir = rnd_node_name(rng, &node, config.name_len.get());
                            stack.push(dir);
                            total_node += 1;
                        }
                    }
                }
            }

            nodes.push_back(node);
        } else {
            return nodes;
        }
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
    dir.join(name)
}

#[inline]
fn rnd_file_name(rng: &mut StdRng, dir: &Path, len: usize) -> PathBuf {
    let mut node = rnd_node_name(rng, dir, len);
    node.set_extension(FILE_EXTENSION);
    node
}
