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

use crate::{signals::Shutdown, target};

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

fn default_max_depth() -> NonZeroUsize {
    NonZeroUsize::new(10).unwrap()
}

fn default_max_sub_folders() -> NonZeroU32 {
    NonZeroU32::new(5).unwrap()
}

fn default_max_files() -> NonZeroU32 {
    NonZeroU32::new(5).unwrap()
}

fn default_max_nodes() -> NonZeroUsize {
    NonZeroUsize::new(100).unwrap()
}

fn default_name_len() -> NonZeroUsize {
    NonZeroUsize::new(8).unwrap()
}

fn default_open_per_second() -> NonZeroU32 {
    NonZeroU32::new(8).unwrap()
}

fn default_rename_per_name() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
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
    total_folder: usize,
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
        let (nodes, _total_files, total_folder) = generate_tree(&mut rng, config);

        Ok(Self {
            name_len: config.name_len,
            open_per_second: config.open_per_second,
            rename_per_second: config.rename_per_second,
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
        let open_rate_limiter = RateLimiter::direct(Quota::per_second(self.open_per_second));
        let rename_rate_limiter = RateLimiter::direct(Quota::per_second(self.rename_per_second));

        let mut iter = self.nodes.iter().cycle();
        let mut folders = Vec::with_capacity(self.total_folder);

        loop {
            tokio::select! {
                _ = open_rate_limiter.until_ready(), if
                    !target::Meta::rss_bytes_limit_exceeded()
                => {
                    let node = iter.next().unwrap();
                    if node.exists() {
                        File::open(node.as_path()).await?;
                    } else {
                        create_node(node).await?;

                        if is_folder(node) {
                            folders.push(node);
                        }
                    }
                },
                _ = rename_rate_limiter.until_ready() => {
                    if target::Meta::rss_bytes_limit_exceeded() {
                        info!("RSS byte limit exceeded, backing off...");
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    if let Some(folder) = folders.choose_mut(&mut self.rng) {
                        rename_folder(&mut self.rng, folder, self.name_len.get()).await?;
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

fn generate_tree(rng: &mut StdRng, config: &Config) -> (VecDeque<PathBuf>, usize, usize) {
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

                let curr_depth =
                    node.to_str().unwrap().matches(path::MAIN_SEPARATOR).count() - root_depth;

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
            return (nodes, total_file, total_folder);
        }
    }
}

#[inline]
async fn rename_folder(rng: &mut StdRng, folder: &Path, len: usize) -> Result<(), Error> {
    let parent = PathBuf::from(folder.parent().unwrap());
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
