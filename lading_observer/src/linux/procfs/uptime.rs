#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Unable to parse /proc/uptime
    #[error("/proc/uptime malformed: {0}")]
    Malformed(&'static str),
    /// Unable to parse floating point
    #[error("Float Parsing: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
}

/// Read `/proc/uptime`
///
/// Only the first field is used, which is the total uptime in seconds.
pub(crate) async fn poll() -> Result<f64, Error> {
    let buf = tokio::fs::read_to_string("/proc/uptime").await?;
    let uptime_secs = proc_uptime_inner(&buf)?;
    Ok(uptime_secs)
}

/// Parse `/proc/uptime` to extract total uptime in seconds.
///
/// # Errors
///
/// Function errors if the file is malformed.
#[inline]
fn proc_uptime_inner(contents: &str) -> Result<f64, Error> {
    // TODO this should probably be scooted up to procfs.rs. Implies the
    // `proc_*` functions there need a test component, making this an inner
    // function eventually.

    let fields: Vec<&str> = contents.split_whitespace().collect();
    if fields.is_empty() {
        return Err(Error::Malformed("/proc/uptime empty"));
    }
    let uptime_secs = fields[0].parse::<f64>()?;
    Ok(uptime_secs)
}

#[cfg(test)]
mod test {
    use super::proc_uptime_inner;

    #[test]
    fn parse_uptime_basic() {
        let line = "12345.67 4321.00\n";
        let uptime = proc_uptime_inner(line).unwrap();
        assert!((uptime - 12345.67).abs() < f64::EPSILON);
    }
}
