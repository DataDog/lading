use std::io::Read;

use super::{next_token, BYTES_PER_KIBIBYTE};

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub(crate) enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Number Parsing: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Parsing: {0}")]
    Parsing(String),
}

pub(crate) struct Rollup {
    pub(crate) rss: u64,
    pub(crate) pss: u64,
    pub(crate) pss_dirty: Option<u64>,
    pub(crate) pss_anon: Option<u64>,
    pub(crate) pss_file: Option<u64>,
    pub(crate) pss_shmem: Option<u64>,
}

impl Rollup {
    pub(crate) fn from_pid(pid: i32) -> Result<Self, Error> {
        Self::from_file(&format!("/proc/{pid}/smaps_rollup"))
    }

    pub(crate) fn from_file(path: &str) -> Result<Self, Error> {
        let mut file: std::fs::File = std::fs::OpenOptions::new().read(true).open(path)?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        Self::from_str(&contents)
    }

    #[allow(clippy::similar_names)]
    pub(crate) fn from_str(contents: &str) -> Result<Self, Error> {
        let mut lines = contents.lines();
        lines.next(); // skip header, doesn't have any useful information
                      // looks like this:
                      // 00400000-7fff03d61000 ---p 00000000 00:00 0                              [rollup]
        let mut rss = None;
        let mut pss = None;
        let mut pss_dirty = None;
        let mut pss_anon = None;
        let mut pss_file = None;
        let mut pss_shmem = None;

        for line in lines {
            let mut chars = line.char_indices().peekable();
            let Some(name) = next_token(line, &mut chars) else {
                // if there is no token on the line, that means empty line, that's fine
                continue;
            };

            let mut value_in_kibibytes = || -> Result<u64, Error> {
                let value_token = next_token(line, &mut chars).ok_or(Error::Parsing(format!(
                    "Could not parse numeric value from line: {line}"
                )))?;
                let unit = next_token(line, &mut chars).ok_or(Error::Parsing(format!(
                    "Could not parse unit from line: {line}"
                )))?;
                let numeric = value_token.parse::<u64>()?;

                match unit {
                    "kB" => Ok(numeric * BYTES_PER_KIBIBYTE),
                    unknown => Err(Error::Parsing(format!(
                        "Unknown unit: {unknown} in line: {line}"
                    ))),
                }
            };

            match name {
                "Rss:" => {
                    rss = Some(value_in_kibibytes()?);
                }
                "Pss:" => {
                    pss = Some(value_in_kibibytes()?);
                }
                "Pss_Dirty:" => {
                    pss_dirty = Some(value_in_kibibytes()?);
                }
                "Pss_Anon:" => {
                    pss_anon = Some(value_in_kibibytes()?);
                }
                "Pss_File:" => {
                    pss_file = Some(value_in_kibibytes()?);
                }
                "Pss_Shmem:" => {
                    pss_shmem = Some(value_in_kibibytes()?);
                }
                _ => {}
            }
        }

        let (Some(rss), Some(pss)) = (rss, pss) else {
            return Err(Error::Parsing(format!(
                "Could not parse all value fields from rollup: '{contents}'"
            )));
        };

        Ok(Rollup {
            rss,
            pss,
            pss_dirty,
            pss_anon,
            pss_file,
            pss_shmem,
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_rollup() {
        let rollup =
            "00400000-7fff03d61000 ---p 00000000 00:00 0                              [rollup]
        Rss:              312048 kB
        Pss:              312044 kB
        Pss_Dirty:        310508 kB
        Pss_Anon:         310508 kB
        Pss_File:           1536 kB
        Pss_Shmem:             0 kB
        Shared_Clean:          4 kB
        Shared_Dirty:          0 kB
        Private_Clean:      1536 kB
        Private_Dirty:    310508 kB
        Referenced:       312048 kB
        Anonymous:        310508 kB
        LazyFree:              0 kB
        AnonHugePages:         0 kB
        ShmemPmdMapped:        0 kB
        FilePmdMapped:         0 kB
        Shared_Hugetlb:        0 kB
        Private_Hugetlb:       0 kB
        Swap:                  0 kB
        SwapPss:               0 kB
        Locked:                0 kB";
        let rollup = Rollup::from_str(rollup).expect("Parsing failed");
        assert_eq!(rollup.pss, 312044 * BYTES_PER_KIBIBYTE);
        assert_eq!(rollup.rss, 312048 * BYTES_PER_KIBIBYTE);
        assert_eq!(rollup.pss_dirty, Some(310508 * BYTES_PER_KIBIBYTE));
        assert_eq!(rollup.pss_anon, Some(310508 * BYTES_PER_KIBIBYTE));
        assert_eq!(rollup.pss_file, Some(1536 * BYTES_PER_KIBIBYTE));
        assert_eq!(rollup.pss_shmem, Some(0));
    }

    #[test]
    fn test_rollup_missing_data() {
        let rollup =
            "00400000-7fff03d61000 ---p 00000000 00:00 0                              [rollup]
        Rss:              312048 kB
        Pss:              312044 kB
        ShmemPmdMapped:        0 kB
        FilePmdMapped:         0 kB
        Shared_Hugetlb:        0 kB
        Private_Hugetlb:       0 kB
        Swap:                  0 kB
        SwapPss:               0 kB
        Locked:                0 kB";
        let rollup = Rollup::from_str(rollup).expect("Parsing failed");
        assert_eq!(rollup.pss, 312044 * BYTES_PER_KIBIBYTE);
        assert_eq!(rollup.rss, 312048 * BYTES_PER_KIBIBYTE);
        assert_eq!(rollup.pss_dirty, None);
        assert_eq!(rollup.pss_anon, None);
        assert_eq!(rollup.pss_file, None);
        assert_eq!(rollup.pss_shmem, None);
    }
}
