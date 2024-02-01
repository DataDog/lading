use regex::Regex;
use std::{collections::HashMap, io::Read};

const SMAP_SIZE_HINT: usize = 128 * 1024; // 128 kB
const BYTES_PER_KIBIBYTE: u64 = 1024;

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

fn next_token<'a>(
    source: &'a str,
    iter: &'_ mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> Option<&'a str> {
    while let Some((_, c)) = iter.peek() {
        if !c.is_whitespace() {
            break;
        }
        iter.next();
    }
    let start = iter.peek()?.0;
    while let Some((_, c)) = iter.peek() {
        if c.is_whitespace() {
            break;
        }
        iter.next();
    }
    let end = iter.peek().map_or_else(|| source.len(), |&(idx, _)| idx);
    Some(&source[start..end])
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

        let mut contents = String::with_capacity(SMAP_SIZE_HINT);
        file.read_to_string(&mut contents).unwrap();

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

pub(crate) struct Regions(pub(crate) Vec<Region>);

#[allow(dead_code)]
pub(crate) struct Region {
    // Metadata
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) perms: String,
    pub(crate) offset: u64,
    // major:minor
    pub(crate) dev: String,
    // 0 indicates no inode
    pub(crate) inode: u64,
    // empty string indicates no pathname
    pub(crate) pathname: String,

    // Values (all in bytes)
    pub(crate) size: u64,
    pub(crate) pss: u64,
    pub(crate) swap: u64,
    pub(crate) rss: u64,
    pub(crate) pss_dirty: Option<u64>, // only present in 6.0+
}
// Best docs ref I have:
// https://docs.kernel.org/filesystems/proc.html
//
// Future improvement: clean up the naming to match man proc
// The /proc/PID/smaps is an extension based on maps, showing
// the memory consumption for each of the process's mappings.
// For each mapping (aka Virtual Memory Area, or VMA) there is a series of lines such as the following:
//
// So what I'm calling "Region" is more accurately referred to as a Virtual Memory Area or VMA

impl Region {
    #[allow(clippy::similar_names)]
    #[allow(clippy::too_many_lines)]
    pub(crate) fn from_str(contents: &str) -> Result<Self, Error> {
        let mut lines = contents.lines();

        // parse metadata
        // 7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
        let Some(metadata_line) = lines.next() else {
            return Err(Error::Parsing(format!(
                "No metadata line found in given region '{contents}'"
            )));
        };

        let mut start: Option<u64> = None;
        let mut end: Option<u64> = None;
        let mut perms: Option<String> = None;
        let mut offset: Option<u64> = None;
        let mut dev: Option<String> = None;
        let mut inode: Option<u64> = None;
        let mut pathname: Option<String> = None;

        let mut chars = metadata_line.char_indices().peekable();

        loop {
            let token = next_token(metadata_line, &mut chars);
            let Some(token) = token else {
                break;
            };

            if let (None, None) = (start, end) {
                let start_str = &token[0..12];
                let end_str = &token[13..25];
                start = Some(u64::from_str_radix(start_str, 16)?);
                end = Some(u64::from_str_radix(end_str, 16)?);
            } else if perms.is_none() {
                perms = Some(token.to_string());
            } else if offset.is_none() {
                offset = Some(u64::from_str_radix(token, 16)?);
            } else if dev.is_none() {
                dev = Some(token.to_string());
            } else if inode.is_none() {
                inode = Some(token.parse::<u64>()?);
            } else if pathname.is_none() {
                pathname = Some(token.to_string());
            } else {
                break;
            }
        }

        let (Some(start), Some(end), Some(perms), Some(offset), Some(dev), Some(inode)) =
            (start, end, perms, offset, dev, inode)
        else {
            return Err(Error::Parsing(format!(
                "Could not parse all metadata fields from line: {metadata_line}"
            )));
        };

        let mut size: Option<u64> = None;
        let mut pss: Option<u64> = None;
        let mut rss: Option<u64> = None;
        let mut swap: Option<u64> = None;
        let mut pss_dirty: Option<u64> = None;

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
                "Size:" => {
                    size = Some(value_in_kibibytes()?);
                }
                "Swap:" => {
                    swap = Some(value_in_kibibytes()?);
                }
                "Pss_Dirty:" => {
                    pss_dirty = Some(value_in_kibibytes()?);
                }
                _ => {}
            }
        }

        let (Some(size), Some(pss), Some(rss), Some(swap)) = (size, pss, rss, swap) else {
            return Err(Error::Parsing(format!(
                "Could not parse all value fields from region: '{contents}'"
            )));
        };

        Ok(Region {
            start,
            end,
            perms,
            offset,
            dev,
            inode,
            pathname: pathname.unwrap_or_default(),
            size,
            pss,
            swap,
            rss,
            pss_dirty,
        })
    }
}

#[derive(Default)]
pub(crate) struct AggrMeasure {
    pub(crate) size: u64,
    pub(crate) pss: u64,
    pub(crate) swap: u64,
    pub(crate) rss: u64,
}

impl Regions {
    pub(crate) fn from_pid(pid: i32) -> Result<Self, Error> {
        Regions::from_file(&format!("/proc/{pid}/smaps"))
    }

    /// Returns a sum of all the fields from each region within this Regions
    pub(crate) fn aggregate(&self) -> AggrMeasure {
        let mut aggr = AggrMeasure::default();

        for region in &self.0 {
            aggr.size = aggr.size.saturating_add(region.size);
            aggr.pss = aggr.pss.saturating_add(region.pss);
            aggr.swap = aggr.swap.saturating_add(aggr.swap);
            aggr.rss = aggr.rss.saturating_add(region.rss);
        }

        aggr
    }

    pub(crate) fn aggregate_by_pathname(&self) -> Vec<(String, AggrMeasure)> {
        let mut map: HashMap<String, AggrMeasure> = HashMap::new();

        for region in &self.0 {
            let pathname = region.pathname.clone();

            let entry = map.entry(pathname).or_insert(AggrMeasure {
                size: 0,
                pss: 0,
                swap: 0,
                rss: 0,
            });
            entry.size += region.size;
            entry.pss += region.pss;
            entry.swap += region.swap;
            entry.rss += region.rss;
        }

        map.into_iter().collect()
    }

    fn from_file(path: &str) -> Result<Self, Error> {
        let mut file: std::fs::File = std::fs::OpenOptions::new().read(true).open(path)?;

        let mut contents = String::with_capacity(SMAP_SIZE_HINT);
        file.read_to_string(&mut contents).unwrap();

        Self::from_str(&contents)
    }

    fn from_str(contents: &str) -> Result<Self, Error> {
        let mut str_regions = Vec::new();
        // split this smaps file into regions
        // regions are separated by a line like this:
        // 7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
        let region_start_regex =
            Regex::new("[[:xdigit:]]{12}-[[:xdigit:]]{12}").expect("Regex to be valid");
        let mut start_indices = region_start_regex.find_iter(contents).map(|m| m.start());

        if let Some(mut start_index) = start_indices.next() {
            for end_match in start_indices {
                str_regions.push(&contents[start_index..end_match]);
                start_index = end_match;
            }

            str_regions.push(&contents[start_index..]);
        };

        let regions = str_regions
            .iter()
            .map(|s| Region::from_str(s))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Regions(regions))
    }
}

#[cfg(test)]
#[allow(clippy::identity_op)]
#[allow(clippy::erasing_op)]
#[allow(clippy::unreadable_literal)]
mod tests {
    use super::*;

    const KERNEL_6_TWO_REGIONS: &str = "
7fffa9f35000-7fffa9f39000 r--p 00000000 12:11 0                          [vvar]
Size:                 16 kB
KernelPageSize:        4 kB
MMUPageSize:           4 kB
Rss:                   0 kB
Pss:                   0 kB
Pss_Dirty:             0 kB
Shared_Clean:          0 kB
Shared_Dirty:          0 kB
Private_Clean:         0 kB
Private_Dirty:         0 kB
Referenced:            0 kB
Anonymous:             0 kB
LazyFree:              0 kB
AnonHugePages:         0 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:         0 kB
Shared_Hugetlb:        0 kB
Private_Hugetlb:       0 kB
Swap:                  7 kB
SwapPss:               0 kB
Locked:                0 kB
THPeligible:    0
ProtectionKey:         0
VmFlags: rd mr pf io de dd sd
7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
Size:                  8 kB
KernelPageSize:        4 kB
MMUPageSize:           4 kB
Rss:                   8 kB
Pss:                   2 kB
Pss_Dirty:             0 kB
Shared_Clean:          8 kB
Shared_Dirty:          0 kB
Private_Clean:         0 kB
Private_Dirty:         0 kB
Referenced:            8 kB
Anonymous:             0 kB
LazyFree:              0 kB
AnonHugePages:         0 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:         0 kB
Shared_Hugetlb:        0 kB
Private_Hugetlb:       0 kB
Swap:                  0 kB
SwapPss:               0 kB
Locked:                0 kB
THPeligible:    0
ProtectionKey:         0
VmFlags: rd ex mr mw me de sd";

    #[test]
    fn test_basic_case() {
        let regions = Regions::from_str(KERNEL_6_TWO_REGIONS).unwrap();
        assert_eq!(regions.0.len(), 2);

        let region_one = &regions.0[0];
        assert_eq!(region_one.start, 0x7fffa9f35000);
        assert_eq!(region_one.end, 0x7fffa9f39000);
        assert_eq!(region_one.perms, "r--p");
        assert_eq!(region_one.offset, 0);
        assert_eq!(region_one.dev, "12:11");
        assert_eq!(region_one.inode, 0);
        assert_eq!(region_one.pathname, "[vvar]");
        assert_eq!(region_one.size, 16 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss, 0);
        assert_eq!(region_one.swap, 7 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.rss, 0);
        assert_eq!(region_one.pss_dirty, Some(0));

        let region_two = &regions.0[1];
        assert_eq!(region_two.start, 0x7fffa9f39000);
        assert_eq!(region_two.end, 0x7fffa9f3b000);
        assert_eq!(region_two.perms, "r-xp");
        assert_eq!(region_two.offset, 0);
        assert_eq!(region_two.dev, "00:00");
        assert_eq!(region_two.inode, 0);
        assert_eq!(region_two.pathname, "[vdso]");
        assert_eq!(region_two.size, 8 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.pss, 2 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.swap, 0 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.rss, 8 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.pss_dirty, Some(0));
    }

    #[test]
    fn test_empty_pathname() {
        let smap_region = "
abcdefabcfed-abdcef123450 r-xp 10101010 12:34 0
Size:                  80000000 kB
KernelPageSize:        400 kB
MMUPageSize:           4 kB
Rss:                   0 kB
Pss:                   1 kB
Pss_Dirty:             2 kB
Shared_Clean:          3 kB
Shared_Dirty:          4 kB
Private_Clean:         5 kB
Private_Dirty:         6 kB
Referenced:            7 kB
Anonymous:             8 kB
LazyFree:              9 kB
AnonHugePages:         10 kB
ShmemPmdMapped:        110 kB
FilePmdMapped:         120 kB
Shared_Hugetlb:        130 kB
Private_Hugetlb:       140140140140 kB
Swap:                  100000000000 kB
SwapPss:               10000000000000000 kB
Locked:                1000000000 kB
THPeligible:    0
ProtectionKey:         0
VmFlags: rd ex mr mw me de sd";
        let regions = Regions::from_str(smap_region).unwrap();
        assert_eq!(regions.0.len(), 1);

        let region_one = &regions.0[0];
        assert_eq!(region_one.start, 0xabcdefabcfed);
        assert_eq!(region_one.end, 0xabdcef123450);
        assert_eq!(region_one.perms, "r-xp");
        assert_eq!(region_one.offset, 0x10101010);
        assert_eq!(region_one.dev, "12:34");
        assert_eq!(region_one.inode, 0);
        assert_eq!(region_one.pathname, "");
        assert_eq!(region_one.size, 80000000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss, 1 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.swap, 100000000000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.rss, 0);
        assert_eq!(region_one.pss_dirty, Some(2 * BYTES_PER_KIBIBYTE));
    }

    #[test]
    fn test_no_pss_dirty() {
        let smap_region = "
7ffeb825c000-7ffeb827d000 rw-p 00000000 00:00 0                          [stack]
Size:                  80000000 kB
KernelPageSize:        400 kB
MMUPageSize:           4 kB
Rss:                   0 kB
Pss:                   1 kB
Shared_Clean:          3 kB
Shared_Dirty:          4 kB
Private_Clean:         5 kB
Private_Dirty:         6 kB
Referenced:            7 kB
Anonymous:             8 kB
LazyFree:              9 kB
AnonHugePages:         10 kB
ShmemPmdMapped:        110 kB
FilePmdMapped:         120 kB
Shared_Hugetlb:        130 kB
Private_Hugetlb:       140140140140 kB
Swap:                  100000000000 kB
SwapPss:               10000000000000000 kB
Locked:                1000000000 kB
THPeligible:    0
ProtectionKey:         0
VmFlags: rd ex mr mw me de sd";
        let regions = Regions::from_str(smap_region).unwrap();
        assert_eq!(regions.0.len(), 1);

        let region_one = &regions.0[0];
        assert_eq!(region_one.start, 0x7ffeb825c000);
        assert_eq!(region_one.end, 0x7ffeb827d000);
        assert_eq!(region_one.perms, "rw-p");
        assert_eq!(region_one.offset, 0);
        assert_eq!(region_one.dev, "00:00");
        assert_eq!(region_one.inode, 0);
        assert_eq!(region_one.pathname, "[stack]");
        assert_eq!(region_one.size, 80000000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss, 1 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.swap, 100000000000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.rss, 0 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss_dirty, None);
    }

    #[test]
    fn test_agent_regions() {
        let region =
            " 7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
        Size:                  8 kB
        KernelPageSize:        4 kB
        MMUPageSize:           4 kB
        Rss:                   8 kB
        Pss:                   2 kB
        Pss_Dirty:             0 kB
        Shared_Clean:          8 kB
        Shared_Dirty:          0 kB
        Private_Clean:         0 kB
        Private_Dirty:         0 kB
        Referenced:            8 kB
        Anonymous:             0 kB
        LazyFree:              0 kB
        AnonHugePages:         0 kB
        ShmemPmdMapped:        0 kB
        FilePmdMapped:         0 kB
        Shared_Hugetlb:        0 kB
        Private_Hugetlb:       0 kB
        Swap:                  0 kB
        SwapPss:               0 kB
        Locked:                0 kB
        THPeligible:    0
        ProtectionKey:         0
        VmFlags: rd ex mr mw me de sd";
        let region = Region::from_str(region).unwrap();

        assert_eq!(region.pathname, "[vdso]");
        assert_eq!(region.size, 8 * BYTES_PER_KIBIBYTE);

        let region = "ffff3fddf000-ffff3fde4000 rw-p 0037f000 fe:01 9339677                    /opt/datadog-agent/embedded/lib/python3.9/site-packages/pydantic_core/_pydantic_core.cpython-39-aarch64-linux-gnu.so
        Size:                 20 kB
        KernelPageSize:        4 kB
        MMUPageSize:           4 kB
        Rss:                  20 kB
        Pss:                  20 kB
        Shared_Clean:          0 kB
        Shared_Dirty:          0 kB
        Private_Clean:         0 kB
        Private_Dirty:        20 kB
        Referenced:           20 kB
        Anonymous:            20 kB
        LazyFree:              0 kB
        AnonHugePages:         0 kB
        ShmemPmdMapped:        0 kB
        FilePmdMapped:         0 kB
        Shared_Hugetlb:        0 kB
        Private_Hugetlb:       0 kB
        Swap:                  0 kB
        SwapPss:               0 kB
        Locked:                0 kB
        THPeligible:    0
        VmFlags: rd wr mr mw me ac";

        let region = Region::from_str(region).unwrap();
        assert_eq!(region.pathname, "/opt/datadog-agent/embedded/lib/python3.9/site-packages/pydantic_core/_pydantic_core.cpython-39-aarch64-linux-gnu.so");
        assert_eq!(region.size, 20 * BYTES_PER_KIBIBYTE);
    }

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
        let rollup = Rollup::from_str(rollup).unwrap();
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
        let rollup = Rollup::from_str(rollup).unwrap();
        assert_eq!(rollup.pss, 312044 * BYTES_PER_KIBIBYTE);
        assert_eq!(rollup.rss, 312048 * BYTES_PER_KIBIBYTE);
        assert_eq!(rollup.pss_dirty, None);
        assert_eq!(rollup.pss_anon, None);
        assert_eq!(rollup.pss_file, None);
        assert_eq!(rollup.pss_shmem, None);
    }
}
