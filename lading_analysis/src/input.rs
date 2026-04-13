//! FUSE capture parsing and input line reconstruction.
//!
//! Reads the FUSE capture JSONL, reconstructs an identical block cache from the
//! lading config, and replays each read to extract the actual lines that were
//! served to the reader.

use std::{
    io::{BufRead, BufReader},
    num::NonZeroU32,
    path::Path,
};

use rustc_hash::FxHashMap;
use serde::Deserialize;

use crate::Error;
use crate::context::InputLine;

/// A parsed FUSE capture record (mirrors the enum in logrotate_fs.rs).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(tag = "type")]
pub enum FuseEvent {
    /// Block cache metadata, emitted once at startup.
    #[serde(rename = "block_cache_meta")]
    BlockCacheMeta {
        /// Total size of the block cache in bytes.
        total_cache_size: u64,
        /// Number of blocks in the cache.
        num_blocks: usize,
    },
    /// A file was created.
    #[serde(rename = "file_created")]
    FileCreated {
        /// Inode of the created file.
        inode: usize,
        /// Group ID of the file.
        group_id: u16,
        /// Offset into the block cache for this file.
        cache_offset: u64,
        /// Tick at which the file was created.
        created_tick: u64,
        /// Bytes written per tick.
        bytes_per_tick: u64,
        /// Parent inode.
        parent_inode: usize,
    },
    /// A file was rotated.
    #[serde(rename = "file_rotated")]
    FileRotated {
        /// Tick at which rotation occurred.
        tick: u64,
        /// Group ID of the rotated file.
        group_id: u16,
        /// Inode of the old file.
        old_inode: usize,
        /// Inode of the new file.
        new_inode: usize,
        /// Cache offset of the new file.
        new_cache_offset: u64,
    },
    /// A file was deleted.
    #[serde(rename = "file_deleted")]
    FileDeleted {
        /// Tick at which deletion occurred.
        tick: u64,
        /// Inode of the deleted file.
        inode: usize,
        /// Group ID of the deleted file.
        group_id: u16,
        /// Total bytes written to this file.
        bytes_written: u64,
        /// Total bytes read from this file.
        bytes_read: u64,
        /// Maximum read offset observed.
        max_offset_observed: u64,
    },
    /// A FUSE read operation.
    #[serde(rename = "read")]
    Read {
        /// Milliseconds since lading epoch.
        relative_ms: u64,
        /// Inode of the file read.
        inode: usize,
        /// Group ID of the file.
        group_id: u16,
        /// Byte offset within the file.
        offset: u64,
        /// Number of bytes read.
        size: u64,
    },
}

/// File metadata tracked during replay.
#[derive(Debug, Clone, Copy)]
struct FileInfo {
    cache_offset: u64,
}

/// Minimal subset of the lading config needed for block cache reconstruction.
#[derive(Debug, Deserialize)]
struct LadingConfig {
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    generator: Vec<GeneratorEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GeneratorEntry {
    FileGen(FileGenConfig),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FileGenConfig {
    LogrotateFs(LogrotateFsConfig),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct LogrotateFsConfig {
    seed: [u8; 32],
    variant: lading_payload::Config,
    maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    #[serde(default = "default_max_block_size")]
    maximum_block_size: byte_unit::Byte,
}

fn default_max_block_size() -> byte_unit::Byte {
    lading_payload::block::default_maximum_block_size()
}

/// Simple hash function for line content.
fn hash_line(line: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    line.hash(&mut hasher);
    hasher.finish()
}

/// Extract the logrotate_fs config from the lading config file.
fn extract_logrotate_config(lading_config_path: &Path) -> Result<LogrotateFsConfig, Error> {
    let contents = std::fs::read_to_string(lading_config_path)?;
    let config: LadingConfig =
        serde_yaml::from_str(&contents).map_err(|e| Error::Config(format!("lading config: {e}")))?;

    for entry in config.generator {
        if let GeneratorEntry::FileGen(FileGenConfig::LogrotateFs(cfg)) = entry {
            return Ok(cfg);
        }
    }

    Err(Error::Config(
        "no logrotate_fs generator found in lading config".into(),
    ))
}

/// Reconstruct the block cache and replay FUSE reads to extract input lines.
///
/// Returns `(input_lines_by_hash, all_fuse_events)`.
///
/// # Errors
///
/// Returns an error if files cannot be read or parsed, or if block cache
/// reconstruction fails.
pub fn reconstruct(
    fuse_capture_path: &Path,
    lading_config_path: &Path,
) -> Result<(FxHashMap<u64, InputLine>, Vec<FuseEvent>), Error> {
    // 1. Reconstruct the block cache from the lading config
    let lr_config = extract_logrotate_config(lading_config_path)?;

    let mut rng = <rand::rngs::SmallRng as rand::SeedableRng>::from_seed(lr_config.seed);
    let total_bytes = NonZeroU32::new(lr_config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
        .ok_or_else(|| Error::Config("maximum_prebuild_cache_size_bytes is zero".into()))?;

    let block_cache = lading_payload::block::Cache::fixed_with_max_overhead(
        &mut rng,
        total_bytes,
        lr_config.maximum_block_size.as_u128(),
        &lr_config.variant,
        total_bytes.get() as usize,
    )?;

    // 2. Parse FUSE capture JSONL — first pass collects metadata and reads
    let file = std::fs::File::open(fuse_capture_path)?;
    let reader = BufReader::new(file);

    let mut files: FxHashMap<usize, FileInfo> = FxHashMap::default();
    let mut events: Vec<FuseEvent> = Vec::new();

    // Per-file accumulator: (max_offset_seen, group_id, first_read_ms)
    let mut file_reads: FxHashMap<usize, (u64, u16, u64)> = FxHashMap::default();

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let event: FuseEvent = serde_json::from_str(&line)?;

        match &event {
            FuseEvent::FileCreated {
                inode,
                cache_offset,
                ..
            } => {
                files.insert(
                    *inode,
                    FileInfo {
                        cache_offset: *cache_offset,
                    },
                );
            }
            FuseEvent::FileRotated {
                new_inode,
                new_cache_offset,
                ..
            } => {
                files.insert(
                    *new_inode,
                    FileInfo {
                        cache_offset: *new_cache_offset,
                    },
                );
            }
            FuseEvent::Read {
                inode,
                offset,
                size,
                relative_ms,
                group_id,
                ..
            } => {
                let end = offset + size;
                file_reads
                    .entry(*inode)
                    .and_modify(|(max_end, _, first_ms)| {
                        *max_end = (*max_end).max(end);
                        *first_ms = (*first_ms).min(*relative_ms);
                    })
                    .or_insert((end, *group_id, *relative_ms));
            }
            FuseEvent::BlockCacheMeta { .. } | FuseEvent::FileDeleted { .. } => {}
        }

        events.push(event);
    }

    // 3. Reconstruct full content per file and split into lines
    let mut input_lines: FxHashMap<u64, InputLine> = FxHashMap::default();

    for (inode, (total_bytes_read, group_id, first_ms)) in &file_reads {
        let Some(file_info) = files.get(inode) else {
            continue;
        };

        // Read the entire range that was read from this file
        let data = block_cache.read_at(file_info.cache_offset, *total_bytes_read as usize);

        for line_bytes in data.split(|&b| b == b'\n') {
            if line_bytes.is_empty() {
                continue;
            }
            let h = hash_line(line_bytes);
            input_lines
                .entry(h)
                .and_modify(|il| il.count += 1)
                .or_insert_with(|| InputLine {
                    hash: h,
                    text: String::from_utf8_lossy(line_bytes).into_owned(),
                    count: 1,
                    group_id: *group_id,
                    first_seen_ms: *first_ms,
                });
        }
    }

    Ok((input_lines, events))
}
