use heck::ToSnakeCase;
use metrics::gauge;
use tokio::fs;

use super::{BYTES_PER_KIBIBYTE, next_token};

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Integer parsing error.
    #[error("Number Parsing: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    /// Parsing error.
    #[error("Parsing: {0}")]
    Parsing(String),
}

#[derive(Debug, Clone, Copy, Default)]
/// Aggregator for memory metrics from smaps_rollup.
pub struct Aggregator {
    /// Resident Set Size in bytes.
    pub rss: u64,
    /// Proportional Set Size in bytes.
    pub pss: u64,
}

/// Read `/proc/{pid}/smaps_rollup` and parse it directly into metrics.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub async fn poll(
    pid: i32,
    labels: &[(&'static str, String)],
    aggr: &mut Aggregator,
) -> Result<(), Error> {
    let path = format!("/proc/{pid}/smaps_rollup");
    // NOTE `read_to_string` uses as few IO operations as possible in its
    // implementation, so we might get the contents here in one go.
    let contents: String = fs::read_to_string(path).await?;
    let mut lines = contents.lines();

    lines.next(); // skip header, doesn't have any useful information
    // looks like this:
    // 00400000-7fff03d61000 ---p 00000000 00:00 0                              [rollup]

    for line in lines {
        let mut chars = line.char_indices().peekable();
        let Some(name) = next_token(line, &mut chars) else {
            // if there is no token on the line, that means empty line, that's fine
            continue;
        };

        let value_bytes = {
            let value_token = next_token(line, &mut chars).ok_or(Error::Parsing(format!(
                "Could not parse numeric value from line: {line}"
            )))?;
            let unit = next_token(line, &mut chars).ok_or(Error::Parsing(format!(
                "Could not parse unit from line: {line}"
            )))?;
            let numeric = value_token.parse::<u64>()?;

            match unit {
                "kB" => Ok(numeric.saturating_mul(BYTES_PER_KIBIBYTE)),
                unknown => Err(Error::Parsing(format!(
                    "Unknown unit: {unknown} in line: {line}"
                ))),
            }
        }?;

        let name_len = name.len();
        // Last character is a :, skip it.
        let field = name[..name_len - 1].to_snake_case();
        match field.as_str() {
            "rss" => aggr.rss = aggr.rss.saturating_add(value_bytes),
            "pss" => aggr.pss = aggr.pss.saturating_add(value_bytes),
            _ => { /* ignore other fields */ }
        }
        let metric_name = format!("smaps_rollup.{field}");
        gauge!(metric_name, labels).set(value_bytes as f64);
    }

    Ok(())
}
