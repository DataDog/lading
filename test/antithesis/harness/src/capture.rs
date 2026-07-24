//! Crash-consistency checking for lading capture (JSONL) files.
//!
//! Under node faults lading is hard-killed (SIGKILL), possibly mid-write. A safe
//! termination leaves the capture file a valid parseable prefix: a partial final
//! line (the interrupted write) is tolerated, but any unparseable line before the
//! final one, or any `fetch_index`/time invariant violation among the parsed
//! records, is real corruption. Parsing and the invariant check reuse
//! lading's own [`lading_capture`] `Line` and canonical `validate_lines`, so the
//! oracle cannot drift from lading's real capture contract.

use std::path::Path;

use lading_capture::line::Line;
use lading_capture::validate::jsonl::validate_lines;
use lading_capture::validate::parquet::validate_parquet;

/// Outcome of checking a capture file's crash-consistency.
#[derive(Debug, Clone, Copy)]
pub struct ConsistencyReport {
    /// Complete lines that parsed as capture records.
    pub parsed: usize,
    /// Non-final lines that failed to parse. A torn final line is tolerated and
    /// not counted here; anything here is real mid-file corruption.
    pub torn_before_final: usize,
    /// Whether the parsed records satisfy lading's capture invariants.
    pub invariants_hold: bool,
    /// `fetch_index`/time mapping violations among parsed records.
    pub fetch_index_errors: u64,
    /// Per-series (time / `fetch_index` monotonicity) violations.
    pub per_series_errors: u64,
}

/// Check a capture file's contents for crash-consistency.
#[must_use]
pub fn check_consistency(content: &str) -> ConsistencyReport {
    let raw: Vec<&str> = content.lines().collect();
    let line_count = raw.len();
    let mut parsed: Vec<Line> = Vec::new();
    let mut torn_before_final = 0_usize;

    for (idx, line) in raw.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Line>(line) {
            Ok(parsed_line) => parsed.push(parsed_line),
            Err(_) => {
                // A parse failure is real corruption only if it is not the final
                // line; a torn final line is the tolerated interrupted write.
                if idx + 1 != line_count {
                    torn_before_final += 1;
                }
            }
        }
    }

    let result = validate_lines(&parsed, None);
    ConsistencyReport {
        parsed: parsed.len(),
        torn_before_final,
        invariants_hold: result.is_valid(),
        fetch_index_errors: u64::try_from(result.fetch_index_errors).unwrap_or(u64::MAX),
        per_series_errors: u64::try_from(result.per_series_errors).unwrap_or(u64::MAX),
    }
}

/// Outcome of checking a Parquet capture file.
#[derive(Debug, Clone, Copy)]
pub struct ParquetReport {
    /// Whether the file was readable as Parquet (its footer is present). Parquet
    /// writes the footer only on a clean close, so a hard kill mid-write leaves it
    /// unreadable -- that is expected, not corruption.
    pub readable: bool,
    /// If readable, whether its capture invariants hold.
    pub invariants_hold: bool,
    /// If readable, the record count.
    pub records: usize,
}

/// Validate a Parquet capture file. An unreadable file (missing footer) is
/// reported via `readable: false` rather than an error, because an abrupt kill
/// legitimately leaves Parquet without its footer; a *readable* Parquet, however,
/// must satisfy the capture invariants.
#[must_use]
pub fn check_parquet(path: &Path) -> ParquetReport {
    match validate_parquet(path, None) {
        Ok(result) => ParquetReport {
            readable: true,
            invariants_hold: result.is_valid(),
            records: usize::try_from(result.line_count).unwrap_or(usize::MAX),
        },
        Err(_) => ParquetReport {
            readable: false,
            invariants_hold: false,
            records: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{check_consistency, check_parquet};
    use lading_capture::line::{Line, LineValue, MetricKind};
    use rustc_hash::FxHashMap;
    use uuid::Uuid;

    fn line(run_id: Uuid, time: u128, fetch_index: u64, metric: &str) -> Line {
        Line {
            run_id,
            time,
            fetch_index,
            metric_name: metric.to_string(),
            metric_kind: MetricKind::Counter,
            value: LineValue::Int(fetch_index),
            labels: FxHashMap::default(),
            value_histogram: Vec::new(),
        }
    }

    fn jsonl(lines: &[Line]) -> String {
        lines
            .iter()
            .map(|l| serde_json::to_string(l).expect("serialize line"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn valid_lines() -> Vec<Line> {
        let run = Uuid::new_v4();
        vec![
            line(run, 1000, 0, "m.a"),
            line(run, 2000, 1, "m.a"),
            line(run, 3000, 2, "m.a"),
        ]
    }

    #[test]
    fn valid_capture_passes() {
        let r = check_consistency(&jsonl(&valid_lines()));
        assert_eq!(r.parsed, 3);
        assert_eq!(r.torn_before_final, 0);
        assert!(r.invariants_hold);
    }

    #[test]
    fn torn_final_line_is_tolerated() {
        let content = format!("{}\n{{\"run_id\":\"partial", jsonl(&valid_lines()));
        let r = check_consistency(&content);
        assert_eq!(r.parsed, 3, "the three complete lines still parse");
        assert_eq!(r.torn_before_final, 0, "a partial final line is tolerated");
        assert!(r.invariants_hold);
    }

    #[test]
    fn torn_middle_line_is_corruption() {
        let good = jsonl(&valid_lines());
        let mut parts: Vec<&str> = good.lines().collect();
        parts.insert(1, "{not valid json");
        let content = parts.join("\n");
        let r = check_consistency(&content);
        assert!(
            r.torn_before_final >= 1,
            "a broken non-final line is flagged"
        );
    }

    #[test]
    fn non_monotonic_fetch_index_fails_invariants() {
        let run = Uuid::new_v4();
        // Same series, fetch_index goes backwards -> invariant violation.
        let lines = vec![line(run, 1000, 5, "m.a"), line(run, 2000, 2, "m.a")];
        let r = check_consistency(&jsonl(&lines));
        assert!(!r.invariants_hold, "backwards fetch_index must fail");
        assert!(r.per_series_errors >= 1 || r.fetch_index_errors >= 1);
    }

    #[test]
    fn unreadable_parquet_is_reported_not_errored() {
        use std::io::Write;
        // A hard kill can leave a Parquet file without its footer. That is not a
        // crash-consistency violation; check_parquet must report it as unreadable
        // rather than error. (A well-formed Parquet's validity is covered by
        // lading_capture's own validate::parquet tests.)
        let mut f = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("temp file");
        f.write_all(b"not a parquet footer").expect("write");
        let r = check_parquet(f.path());
        assert!(
            !r.readable,
            "a footer-less parquet is unreadable, not a crash"
        );
        assert!(!r.invariants_hold);
    }
}
