use metrics::gauge;
use rustc_hash::{FxBuildHasher, FxHashMap};

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Unable to parse /proc/vmstat
    #[error("/proc/vmstat malformed: {0}")]
    Malformed(&'static str),
    /// Unable to parse integer
    #[error("Integer Parsing: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

/// Read and publish metrics from `/proc/vmstat`
///
/// This function reads the kernel's virtual memory statistics and publishes
/// them as metrics. The vmstat file contains key-value pairs separated by
/// whitespace, providing insights into memory usage, allocation, and pressure.
pub(crate) async fn poll() -> Result<(), Error> {
    let buf = tokio::fs::read_to_string("/proc/vmstat").await?;
    let vmstat_data = proc_vmstat_inner(&buf)?;

    // Publish metrics for all vmstat fields
    for (field, value) in vmstat_data {
        gauge!(format!("vmstat.{}", field)).set(value as f64);
    }

    Ok(())
}

/// Parse `/proc/vmstat` to extract virtual memory statistics.
///
/// The vmstat file contains one field per line in the format:
/// `field_name value`
///
/// # Errors
///
/// Function errors if the file is malformed or contains unparseable values.
#[inline]
fn proc_vmstat_inner(contents: &str) -> Result<FxHashMap<&str, u64>, Error> {
    let mut vmstat_data = FxHashMap::with_capacity_and_hasher(128, FxBuildHasher);

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let field_name = parts
            .next()
            .ok_or(Error::Malformed("line does not contain field name"))?;
        let value_str = parts
            .next()
            .ok_or(Error::Malformed("line does not contain value"))?;

        if parts.next().is_some() {
            return Err(Error::Malformed("line contains more than two fields"));
        }

        let value = value_str.parse::<u64>()?;
        vmstat_data.insert(field_name, value);
    }

    Ok(vmstat_data)
}

#[cfg(test)]
mod test {
    use super::proc_vmstat_inner;

    #[test]
    fn parse_vmstat_basic() {
        let vmstat_content = "nr_free_pages 123456\nnr_alloc_batch 789\nnr_inactive_anon 456789\n";
        let vmstat_data = proc_vmstat_inner(vmstat_content).unwrap();

        assert_eq!(vmstat_data.get("nr_free_pages"), Some(&123456));
        assert_eq!(vmstat_data.get("nr_alloc_batch"), Some(&789));
        assert_eq!(vmstat_data.get("nr_inactive_anon"), Some(&456789));
    }

    #[test]
    fn parse_vmstat_empty_lines() {
        let vmstat_content = "nr_free_pages 123456\n\nnr_alloc_batch 789\n\n";
        let vmstat_data = proc_vmstat_inner(vmstat_content).unwrap();

        assert_eq!(vmstat_data.len(), 2);
        assert_eq!(vmstat_data.get("nr_free_pages"), Some(&123456));
        assert_eq!(vmstat_data.get("nr_alloc_batch"), Some(&789));
    }

    #[test]
    fn parse_vmstat_malformed_line() {
        let vmstat_content = "nr_free_pages 123456\ninvalid_line\n";
        let result = proc_vmstat_inner(vmstat_content);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("malformed"));
    }

    #[test]
    fn parse_vmstat_invalid_number() {
        let vmstat_content = "nr_free_pages not_a_number\n";
        let result = proc_vmstat_inner(vmstat_content);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Integer Parsing"));
    }

    #[test]
    fn parse_vmstat_real_sample() {
        let vmstat_content = r#"nr_free_pages 1234567
nr_alloc_batch 63
nr_inactive_anon 456789
nr_active_anon 123456
nr_inactive_file 789012
nr_active_file 345678
nr_unevictable 0
nr_mlock 0
nr_anon_pages 567890
nr_mapped 234567
nr_file_pages 1134690
nr_dirty 456
nr_writeback 0
nr_slab_reclaimable 89012
nr_slab_unreclaimable 45678
pgpgin 12345678
pgpgout 9876543
pswpin 123
pswpout 456
pgalloc_dma 0
pgalloc_dma32 1234567
pgalloc_normal 9876543
pgalloc_high 0
pgalloc_movable 0
pgfree 11111111
pgactivate 222222
pgdeactivate 333333
pgfault 44444444
pgmajfault 55555
"#;
        let vmstat_data = proc_vmstat_inner(vmstat_content).unwrap();

        assert_eq!(vmstat_data.len(), 29);
        assert_eq!(vmstat_data.get("nr_free_pages"), Some(&1234567));
        assert_eq!(vmstat_data.get("pgpgin"), Some(&12345678));
        assert_eq!(vmstat_data.get("pgpgout"), Some(&9876543));
        assert_eq!(vmstat_data.get("pgfault"), Some(&44444444));
        assert_eq!(vmstat_data.get("pgmajfault"), Some(&55555));
    }
}
