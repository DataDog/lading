//! FUSE capture parsing and input reconstruction.
//!
//! Supports two modes:
//! - **Raw**: one entry per FUSE read with SHA-256 hash and exact timestamp
//! - **Newline-delimited**: lines reconstructed across read boundaries, each
//!   annotated with the reads that contributed bytes

use std::{
    io::{BufRead, BufReader},
    num::NonZeroU32,
    path::Path,
};

use rustc_hash::FxHashMap;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::Error;
use crate::context::{ContentHash, RawRead, ReadContribution, ReconstructedLine};

/// A parsed FUSE capture record.
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

/// A read event with its file context, used for per-file sorting.
#[derive(Debug, Clone, Copy)]
struct ReadRecord {
    offset: u64,
    size: u64,
    relative_ms: u64,
    group_id: u16,
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

/// SHA-256 hash of a byte slice.
fn sha256(data: &[u8]) -> ContentHash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
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

/// Reconstruct inputs from FUSE capture and lading config. Always produces
/// both raw reads (per-FUSE-read with hash and timestamp) and newline-delimited
/// lines (stitched across read boundaries).
///
/// # Errors
///
/// Returns an error if files cannot be read or parsed, or if block cache
/// reconstruction fails.
pub fn reconstruct(
    fuse_capture_path: &Path,
    lading_config_path: &Path,
) -> Result<(Vec<RawRead>, Vec<ReconstructedLine>, Vec<FuseEvent>), Error> {
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

    // Parse FUSE capture JSONL
    let file = std::fs::File::open(fuse_capture_path)?;
    let reader = BufReader::new(file);

    let mut files: FxHashMap<usize, FileInfo> = FxHashMap::default();
    let mut events: Vec<FuseEvent> = Vec::new();
    let mut reads_by_inode: FxHashMap<usize, Vec<ReadRecord>> = FxHashMap::default();
    let mut raw_reads: Vec<RawRead> = Vec::new();

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
                let Some(file_info) = files.get(inode) else {
                    events.push(event);
                    continue;
                };

                // Always collect raw reads
                let data = block_cache.read_at(
                    file_info.cache_offset + offset,
                    *size as usize,
                );
                raw_reads.push(RawRead {
                    inode: *inode,
                    group_id: *group_id,
                    offset: *offset,
                    size: *size,
                    relative_ms: *relative_ms,
                    content: String::from_utf8_lossy(&data).into_owned(),
                });

                // Always collect per-inode reads for line reconstruction
                reads_by_inode
                    .entry(*inode)
                    .or_default()
                    .push(ReadRecord {
                        offset: *offset,
                        size: *size,
                        relative_ms: *relative_ms,
                        group_id: *group_id,
                        cache_offset: file_info.cache_offset,
                    });
            }
            FuseEvent::BlockCacheMeta { .. } | FuseEvent::FileDeleted { .. } => {}
        }

        events.push(event);
    }

    let lines = reconstruct_lines(&block_cache, reads_by_inode);

    Ok((raw_reads, lines, events))
}

/// Reconstruct newline-delimited lines from per-file reads.
fn reconstruct_lines(
    block_cache: &lading_payload::block::Cache,
    reads_by_inode: FxHashMap<usize, Vec<ReadRecord>>,
) -> Vec<ReconstructedLine> {
    let mut all_lines: Vec<ReconstructedLine> = Vec::new();

    for (_inode, mut reads) in reads_by_inode {
        // Sort reads by offset for sequential replay
        reads.sort_by_key(|r| r.offset);

        let group_id = reads.first().map_or(0, |r| r.group_id);
        let mut line_buffer: Vec<u8> = Vec::new();
        let mut contributions: Vec<ReadContribution> = Vec::new();

        for read in &reads {
            let data = block_cache.read_at(
                read.cache_offset + read.offset,
                read.size as usize,
            );

            // Process byte by byte looking for newlines
            let mut start = 0;
            for (i, &byte) in data.iter().enumerate() {
                if byte == b'\n' {
                    // Complete line found
                    line_buffer.extend_from_slice(&data[start..i]);
                    contributions.push(ReadContribution {
                        offset: read.offset + start as u64,
                        size: (i - start) as u64,
                        relative_ms: read.relative_ms,
                    });

                    if !line_buffer.is_empty() {
                        all_lines.push(ReconstructedLine {
                            hash: sha256(&line_buffer),
                            text: String::from_utf8_lossy(&line_buffer).into_owned(),
                            group_id,
                            contributions: std::mem::take(&mut contributions),
                        });
                    }
                    line_buffer.clear();
                    start = i + 1;
                }
            }

            // Remaining bytes after last newline — carry forward
            if start < data.len() {
                line_buffer.extend_from_slice(&data[start..]);
                contributions.push(ReadContribution {
                    offset: read.offset + start as u64,
                    size: (data.len() - start) as u64,
                    relative_ms: read.relative_ms,
                });
            }
        }

        // Final partial line — discard (agent framer won't emit it)
        // line_buffer and contributions are dropped
    }

    all_lines
}
