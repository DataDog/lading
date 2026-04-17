//! Integration test: writes synthetic JSONL capture files and a lading config,
//! runs the full analysis pipeline, and verifies results.

use std::io::Write;

use lading_analysis::config::{
    AnalysisConfig, CheckConfig, CompletenessParams, DuplicationParams, FabricationParams,
    Inputs, LatencyParams,
};

/// Write a minimal lading config that the analysis tool can parse to rebuild
/// the block cache.
fn write_lading_config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("lading.yaml");
    let mut f = std::fs::File::create(&path).unwrap();
    write!(
        f,
        r#"generator:
  - file_gen:
      logrotate_fs:
        seed: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]
        concurrent_logs: 1
        maximum_bytes_per_log: "10 MiB"
        total_rotations: 3
        max_depth: 0
        variant: ascii
        maximum_prebuild_cache_size_bytes: "4 MiB"
        mount_point: /tmp/unused
        load_profile:
          constant: "10 KiB"
blackhole:
  - http:
      binding_addr: "127.0.0.1:9091"
"#
    )
    .unwrap();
    path
}

/// Build a block cache from the test config and extract lines from the first
/// file's content, returning (lines, cache_offset).
fn rebuild_cache_and_extract() -> (Vec<String>, u64) {
    use lading_payload::block;
    use rand::{SeedableRng, rngs::SmallRng};
    use std::num::NonZeroU32;

    let seed = [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 1,
    ];
    let mut rng = SmallRng::from_seed(seed);
    let total_bytes = NonZeroU32::new(4 * 1024 * 1024).unwrap();
    let cache = block::Cache::fixed_with_max_overhead(
        &mut rng,
        total_bytes,
        block::default_maximum_block_size().as_u128(),
        &lading_payload::Config::Ascii,
        total_bytes.get() as usize,
    )
    .unwrap();

    // The first file gets a cache_offset from the same RNG sequence.
    // We need to match what logrotate_fs::model would produce.
    // For this test, we'll just pick offset 0 and read some bytes.
    let cache_offset: u64 = 0;
    let data = cache.read_at(cache_offset, 20480); // 20 KiB
    let text = String::from_utf8_lossy(&data);
    let lines: Vec<String> = text
        .split('\n')
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();
    (lines, cache_offset)
}

/// Write FUSE capture JSONL with block_cache_meta + file_created + read events.
fn write_fuse_capture(
    dir: &std::path::Path,
    cache_offset: u64,
    read_size: u64,
    num_reads: usize,
) -> std::path::PathBuf {
    let path = dir.join("fuse_reads.jsonl");
    let mut f = std::fs::File::create(&path).unwrap();

    // Block cache meta
    writeln!(
        f,
        r#"{{"type":"block_cache_meta","total_cache_size":4194304,"num_blocks":7}}"#
    )
    .unwrap();

    // File created
    writeln!(
        f,
        r#"{{"type":"file_created","inode":2,"group_id":0,"cache_offset":{cache_offset},"created_tick":0,"bytes_per_tick":10240,"parent_inode":1}}"#
    )
    .unwrap();

    // Sequential reads
    for i in 0..num_reads {
        let offset = (i as u64) * read_size;
        let ms = 1000 + (i as u64) * 1000; // 1s apart
        writeln!(
            f,
            r#"{{"type":"read","relative_ms":{ms},"inode":2,"group_id":0,"offset":{offset},"size":{read_size}}}"#
        )
        .unwrap();
    }

    path
}

/// Write blackhole capture JSONL with payloads containing the given messages.
fn write_blackhole_capture(
    dir: &std::path::Path,
    messages: &[(u64, Vec<String>)], // (relative_ms, messages)
) -> std::path::PathBuf {
    let path = dir.join("blackhole.jsonl");
    let mut f = std::fs::File::create(&path).unwrap();

    for (ms, msgs) in messages {
        let entries: Vec<String> = msgs
            .iter()
            .map(|m| {
                let escaped = m.replace('\\', "\\\\").replace('"', "\\\"");
                format!(r#"{{"message":"{escaped}"}}"#)
            })
            .collect();
        let payload = format!("[{}]", entries.join(","));
        let payload_escaped = payload.replace('\\', "\\\\").replace('"', "\\\"");
        writeln!(
            f,
            r#"{{"relative_ms":{ms},"compressed_bytes":0,"payload":"{payload_escaped}"}}"#
        )
        .unwrap();
    }

    path
}

#[test]
fn integration_perfect_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let lading_config = write_lading_config(dir.path());

    // Rebuild cache and get the actual lines
    let (all_lines, cache_offset) = rebuild_cache_and_extract();
    assert!(all_lines.len() >= 5, "need at least 5 lines for test");

    // Use all lines that the reads produce — the FUSE reads define the input set
    let fuse_path = write_fuse_capture(dir.path(), cache_offset, 4096, 5);

    // Reconstruct what those reads produce to get the exact input lines
    let (_, lines, _) = lading_analysis::input::reconstruct(
        &fuse_path,
        &lading_config,
    )
    .unwrap();
    let test_lines: Vec<String> = lines.iter().map(|l| l.text.clone()).collect();
    assert!(!test_lines.is_empty(), "should have reconstructed lines");

    // Write blackhole capture with ALL reconstructed lines
    let blackhole_path =
        write_blackhole_capture(dir.path(), &[(5000, test_lines.clone())]);

    let config = AnalysisConfig {
        inputs: Inputs {
            fuse_capture: fuse_path,
            blackhole_capture: blackhole_path,
            lading_config,
        },
        output_dir: None,
        checks: vec![
            CheckConfig::Completeness(CompletenessParams { min_ratio: 1.0 }),
            CheckConfig::Fabrication(FabricationParams { max_count: 0 }),
            CheckConfig::Duplication(DuplicationParams { max_ratio: 0.0 }),
            CheckConfig::Latency(LatencyParams {
                max_p99_ms: Some(10000),
            }),
        ],
    };

    let results = lading_analysis::run(&config).unwrap();

    for r in &results {
        assert!(
            r.passed,
            "check '{}' failed: {} {:?}",
            r.name, r.summary, r.details
        );
    }
}

#[test]
fn integration_detects_missing_and_fabricated() {
    let dir = tempfile::tempdir().unwrap();
    let lading_config = write_lading_config(dir.path());

    let (all_lines, cache_offset) = rebuild_cache_and_extract();
    assert!(all_lines.len() >= 5);

    let test_lines: Vec<String> = all_lines[..5].to_vec();

    let fuse_path = write_fuse_capture(dir.path(), cache_offset, 4096, 5);

    // Only send 2 of 5 lines + add a fabricated one
    let mut output_msgs = vec![test_lines[0].clone(), test_lines[1].clone()];
    output_msgs.push("THIS_LINE_WAS_FABRICATED".to_string());

    let blackhole_path =
        write_blackhole_capture(dir.path(), &[(5000, output_msgs)]);

    let config = AnalysisConfig {
        inputs: Inputs {
            fuse_capture: fuse_path,
            blackhole_capture: blackhole_path,
            lading_config,
        },
        output_dir: None,
        checks: vec![
            CheckConfig::Completeness(CompletenessParams { min_ratio: 0.9 }),
            CheckConfig::Fabrication(FabricationParams { max_count: 0 }),
        ],
    };

    let results = lading_analysis::run(&config).unwrap();

    // Completeness should fail (2/N < 0.9)
    assert!(!results[0].passed, "completeness should fail: {}", results[0].summary);

    // Fabrication should fail (1 fabricated line)
    assert!(!results[1].passed, "fabrication should fail: {}", results[1].summary);
}
