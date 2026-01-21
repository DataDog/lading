use std::{fs, io::Read};

use regex::Regex;
use rustc_hash::FxHashMap;

use crate::linux::procfs::BYTES_PER_KIBIBYTE;

use super::next_token;

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

struct Region {
    // empty string indicates no pathname
    pub(crate) pathname: String,

    // Values (all in bytes)
    pub(crate) size: u64,
    pub(crate) pss: u64,
    pub(crate) swap: u64,
    pub(crate) rss: u64,
    pub(crate) pss_dirty: Option<u64>, // only present in 6.0+
    pub(crate) shared_clean: Option<u64>,
    pub(crate) shared_dirty: Option<u64>,
    pub(crate) private_clean: Option<u64>,
    pub(crate) private_dirty: Option<u64>,
    pub(crate) referenced: Option<u64>,
    pub(crate) anonymous: Option<u64>,
    pub(crate) lazy_free: Option<u64>,
    pub(crate) anon_huge_pages: Option<u64>,
    pub(crate) shmem_pmd_mapped: Option<u64>,
    pub(crate) file_pmd_mapped: Option<u64>,
    pub(crate) shared_hugetlb: Option<u64>,
    pub(crate) private_hugetlb: Option<u64>,
    pub(crate) swap_pss: Option<u64>,
    pub(crate) locked: Option<u64>,
}

impl Region {
    #[allow(clippy::similar_names)]
    #[allow(clippy::too_many_lines)]
    pub(crate) fn from_str(contents: &str) -> Result<Self, Error> {
        let mut lines = contents.lines();

        // Parse metadata line
        let Some(metadata_line) = lines.next() else {
            return Err(Error::Parsing(format!(
                "No metadata line found in given region '{contents}'"
            )));
        };

        let pathname = {
            let mut start: Option<u64> = None;
            let mut end: Option<u64> = None;
            let mut perms: Option<String> = None;
            let mut offset: Option<u64> = None;
            let mut dev: Option<String> = None;
            let mut inode: Option<u64> = None;
            let pathname: Option<String> = None;
            let mut pathname_tokens = vec![];

            let mut chars = metadata_line.char_indices().peekable();
            let mut entered_pathname = false;

            loop {
                let token = next_token(metadata_line, &mut chars);
                let Some(token) = token else {
                    break;
                };

                if let (None, None) = (start, end) {
                    let dash_loc = token.find('-').ok_or(Error::Parsing(format!(
                        "Could not find dash in addr: {token}"
                    )))?;
                    let start_str = &token[0..dash_loc];
                    let end_str = &token[dash_loc + 1..];
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
                } else if pathname.is_none() || entered_pathname {
                    entered_pathname = true;
                    pathname_tokens.push(token);
                } else {
                    break;
                }
            }

            pathname_tokens.join(" ")
        };

        let mut size: Option<u64> = None;
        let mut pss: Option<u64> = None;
        let mut rss: Option<u64> = None;
        let mut swap: Option<u64> = None;
        let mut pss_dirty: Option<u64> = None;
        let mut shared_clean: Option<u64> = None;
        let mut shared_dirty: Option<u64> = None;
        let mut private_clean: Option<u64> = None;
        let mut private_dirty: Option<u64> = None;
        let mut referenced: Option<u64> = None;
        let mut anonymous: Option<u64> = None;
        let mut lazy_free: Option<u64> = None;
        let mut anon_huge_pages: Option<u64> = None;
        let mut shmem_pmd_mapped: Option<u64> = None;
        let mut file_pmd_mapped: Option<u64> = None;
        let mut shared_hugetlb: Option<u64> = None;
        let mut private_hugetlb: Option<u64> = None;
        let mut swap_pss: Option<u64> = None;
        let mut locked: Option<u64> = None;

        for line in lines {
            let mut chars = line.char_indices().peekable();
            let Some(name) = next_token(line, &mut chars) else {
                // Empty line, skip
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
                "Shared_Clean:" => {
                    shared_clean = Some(value_in_kibibytes()?);
                }
                "Shared_Dirty:" => {
                    shared_dirty = Some(value_in_kibibytes()?);
                }
                "Private_Clean:" => {
                    private_clean = Some(value_in_kibibytes()?);
                }
                "Private_Dirty:" => {
                    private_dirty = Some(value_in_kibibytes()?);
                }
                "Referenced:" => {
                    referenced = Some(value_in_kibibytes()?);
                }
                "Anonymous:" => {
                    anonymous = Some(value_in_kibibytes()?);
                }
                "LazyFree:" => {
                    lazy_free = Some(value_in_kibibytes()?);
                }
                "AnonHugePages:" => {
                    anon_huge_pages = Some(value_in_kibibytes()?);
                }
                "ShmemPmdMapped:" => {
                    shmem_pmd_mapped = Some(value_in_kibibytes()?);
                }
                "FilePmdMapped:" => {
                    file_pmd_mapped = Some(value_in_kibibytes()?);
                }
                "Shared_Hugetlb:" => {
                    shared_hugetlb = Some(value_in_kibibytes()?);
                }
                "Private_Hugetlb:" => {
                    private_hugetlb = Some(value_in_kibibytes()?);
                }
                "SwapPss:" => {
                    swap_pss = Some(value_in_kibibytes()?);
                }
                "Locked:" => {
                    locked = Some(value_in_kibibytes()?);
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
            pathname,
            size,
            pss,
            swap,
            rss,
            pss_dirty,
            shared_clean,
            shared_dirty,
            private_clean,
            private_dirty,
            referenced,
            anonymous,
            lazy_free,
            anon_huge_pages,
            shmem_pmd_mapped,
            file_pmd_mapped,
            shared_hugetlb,
            private_hugetlb,
            swap_pss,
            locked,
        })
    }
}

#[derive(Default)]
pub(crate) struct AggrMeasure {
    pub(crate) size: u64,
    pub(crate) pss: u64,
    pub(crate) swap: u64,
    pub(crate) rss: u64,
    pub(crate) pss_dirty: Option<u64>,
    pub(crate) shared_clean: Option<u64>,
    pub(crate) shared_dirty: Option<u64>,
    pub(crate) private_clean: Option<u64>,
    pub(crate) private_dirty: Option<u64>,
    pub(crate) referenced: Option<u64>,
    pub(crate) anonymous: Option<u64>,
    pub(crate) lazy_free: Option<u64>,
    pub(crate) anon_huge_pages: Option<u64>,
    pub(crate) shmem_pmd_mapped: Option<u64>,
    pub(crate) file_pmd_mapped: Option<u64>,
    pub(crate) shared_hugetlb: Option<u64>,
    pub(crate) private_hugetlb: Option<u64>,
    pub(crate) swap_pss: Option<u64>,
    pub(crate) locked: Option<u64>,
}

pub(crate) struct Regions(Vec<Region>);

impl Regions {
    pub(crate) fn from_pid(pid: i32) -> Result<Self, Error> {
        let path = format!("/proc/{pid}/smaps");
        let mut file: fs::File = fs::OpenOptions::new().read(true).open(path)?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        Self::from_str(&contents)
    }

    fn from_str(contents: &str) -> Result<Self, Error> {
        let str_regions = Self::into_region_strs(contents);
        let regions = str_regions
            .iter()
            .map(|s| Region::from_str(s))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Regions(regions))
    }

    pub(crate) fn aggregate_by_pathname(&self) -> Vec<(String, AggrMeasure)> {
        let mut map: FxHashMap<String, AggrMeasure> = FxHashMap::default();

        for region in &self.0 {
            let pathname = region.pathname.clone();

            let entry = map.entry(pathname).or_default();
            entry.size += region.size;
            entry.pss += region.pss;
            entry.swap += region.swap;
            entry.rss += region.rss;
            aggregate_option(&mut entry.pss_dirty, region.pss_dirty);
            aggregate_option(&mut entry.shared_clean, region.shared_clean);
            aggregate_option(&mut entry.shared_dirty, region.shared_dirty);
            aggregate_option(&mut entry.private_clean, region.private_clean);
            aggregate_option(&mut entry.private_dirty, region.private_dirty);
            aggregate_option(&mut entry.referenced, region.referenced);
            aggregate_option(&mut entry.anonymous, region.anonymous);
            aggregate_option(&mut entry.lazy_free, region.lazy_free);
            aggregate_option(&mut entry.anon_huge_pages, region.anon_huge_pages);
            aggregate_option(&mut entry.shmem_pmd_mapped, region.shmem_pmd_mapped);
            aggregate_option(&mut entry.file_pmd_mapped, region.file_pmd_mapped);
            aggregate_option(&mut entry.shared_hugetlb, region.shared_hugetlb);
            aggregate_option(&mut entry.private_hugetlb, region.private_hugetlb);
            aggregate_option(&mut entry.swap_pss, region.swap_pss);
            aggregate_option(&mut entry.locked, region.locked);
        }

        map.into_iter().collect()
    }

    fn into_region_strs(contents: &str) -> Vec<&str> {
        let mut str_regions = Vec::new();
        // Split the smaps file into regions
        // Regions are separated by a line like this:
        // 7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
        let region_start_regex =
            Regex::new("(?m)^[[:xdigit:]]+-[[:xdigit:]]+").expect("Regex to be valid");
        let mut start_indices = region_start_regex.find_iter(contents).map(|m| m.start());

        if let Some(mut start_index) = start_indices.next() {
            for end_match in start_indices {
                str_regions.push(&contents[start_index..end_match]);
                start_index = end_match;
            }

            str_regions.push(&contents[start_index..]);
        }

        str_regions
    }
}

#[inline]
fn aggregate_option(aggregate: &mut Option<u64>, value: Option<u64>) {
    if let Some(v) = value {
        match aggregate {
            Some(a) => *a += v,
            None => *aggregate = Some(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KERNEL_6_TWO_REGIONS: &str =
        "7fffa9f35000-7fffa9f39000 r--p 00000000 12:11 0                          [vvar]
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
SwapPss:               1 kB
Locked:                0 kB
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd mr pf io de dd sd
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";

    #[test]
    fn test_basic_case() {
        let regions = Regions::from_str(KERNEL_6_TWO_REGIONS).expect("Parsing failed");
        assert_eq!(regions.0.len(), 2);

        let region_one = &regions.0[0];

        assert_eq!(region_one.pathname, "[vvar]");
        assert_eq!(region_one.size, 16 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss, 0);
        assert_eq!(region_one.swap, 7 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.rss, 0);
        assert_eq!(region_one.pss_dirty, Some(0)); // pss_dirty is optional
        assert_eq!(region_one.shared_clean, Some(0));
        assert_eq!(region_one.shared_dirty, Some(0));
        assert_eq!(region_one.private_clean, Some(0));
        assert_eq!(region_one.private_dirty, Some(0));
        assert_eq!(region_one.referenced, Some(0));
        assert_eq!(region_one.anonymous, Some(0));
        assert_eq!(region_one.lazy_free, Some(0));
        assert_eq!(region_one.anon_huge_pages, Some(0));
        assert_eq!(region_one.shmem_pmd_mapped, Some(0));
        assert_eq!(region_one.file_pmd_mapped, Some(0));
        assert_eq!(region_one.shared_hugetlb, Some(0));
        assert_eq!(region_one.private_hugetlb, Some(0));
        assert_eq!(region_one.swap_pss, Some(BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.locked, Some(0));

        let region_two = &regions.0[1];
        assert_eq!(region_two.pathname, "[vdso]");
        assert_eq!(region_two.size, 8 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.pss, 2 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.swap, 0);
        assert_eq!(region_two.rss, 8 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_two.pss_dirty, Some(0));
        assert_eq!(region_two.shared_clean, Some(8 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_two.shared_dirty, Some(0));
        assert_eq!(region_two.private_clean, Some(0));
        assert_eq!(region_two.private_dirty, Some(0));
        assert_eq!(region_two.referenced, Some(8 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_two.anonymous, Some(0));
        assert_eq!(region_two.lazy_free, Some(0));
        assert_eq!(region_two.anon_huge_pages, Some(0));
        assert_eq!(region_two.shmem_pmd_mapped, Some(0));
        assert_eq!(region_two.file_pmd_mapped, Some(0));
        assert_eq!(region_two.shared_hugetlb, Some(0));
        assert_eq!(region_two.private_hugetlb, Some(0));
        assert_eq!(region_two.swap_pss, Some(0));
        assert_eq!(region_two.locked, Some(0));
    }

    #[test]
    fn test_empty_pathname() {
        let smap_region = "abcdefabcfed-abdcef123450 r-xp 10101010 12:34 0
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";
        let regions = Regions::from_str(smap_region).expect("Parsing failed");
        assert_eq!(regions.0.len(), 1);

        let region_one = &regions.0[0];

        assert_eq!(region_one.pathname, "");
        assert_eq!(region_one.size, 80_000_000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss, BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.swap, 100_000_000_000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.rss, 0);
        assert_eq!(region_one.pss_dirty, Some(2 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shared_clean, Some(3 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shared_dirty, Some(4 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.private_clean, Some(5 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.private_dirty, Some(6 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.referenced, Some(7 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.anonymous, Some(8 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.lazy_free, Some(9 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.anon_huge_pages, Some(10 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shmem_pmd_mapped, Some(110 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.file_pmd_mapped, Some(120 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shared_hugetlb, Some(130 * BYTES_PER_KIBIBYTE));
        assert_eq!(
            region_one.private_hugetlb,
            Some(140_140_140_140 * BYTES_PER_KIBIBYTE)
        );
        assert_eq!(
            region_one.swap_pss,
            Some(10_000_000_000_000_000 * BYTES_PER_KIBIBYTE)
        );
        assert_eq!(region_one.locked, Some(1_000_000_000 * BYTES_PER_KIBIBYTE));
    }

    #[test]
    fn test_no_pss_dirty() {
        let smap_region =
            "7ffeb825c000-7ffeb827d000 rw-p 00000000 00:00 0                          [stack]
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";

        let regions = Regions::from_str(smap_region).expect("Parsing failed");
        assert_eq!(regions.0.len(), 1);

        let region_one = &regions.0[0];

        assert_eq!(region_one.pathname, "[stack]");
        assert_eq!(region_one.size, 80_000_000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.pss, BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.swap, 100_000_000_000 * BYTES_PER_KIBIBYTE);
        assert_eq!(region_one.rss, 0);
        assert_eq!(region_one.pss_dirty, None); // Still optional and missing
        assert_eq!(region_one.shared_clean, Some(3 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shared_dirty, Some(4 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.private_clean, Some(5 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.private_dirty, Some(6 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.referenced, Some(7 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.anonymous, Some(8 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.lazy_free, Some(9 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.anon_huge_pages, Some(10 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shmem_pmd_mapped, Some(110 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.file_pmd_mapped, Some(120 * BYTES_PER_KIBIBYTE));
        assert_eq!(region_one.shared_hugetlb, Some(130 * BYTES_PER_KIBIBYTE));
        assert_eq!(
            region_one.private_hugetlb,
            Some(140_140_140_140 * BYTES_PER_KIBIBYTE)
        );
        assert_eq!(
            region_one.swap_pss,
            Some(10_000_000_000_000_000 * BYTES_PER_KIBIBYTE)
        );
        assert_eq!(region_one.locked, Some(1_000_000_000 * BYTES_PER_KIBIBYTE));
    }

    #[test]
    fn test_agent_regions() {
        let region =
            "7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";
        let region = Region::from_str(region).expect("Parsing failed");

        assert_eq!(region.pathname, "[vdso]");
        assert_eq!(region.size, 8 * BYTES_PER_KIBIBYTE);
        assert_eq!(region.shared_clean.unwrap(), 8 * BYTES_PER_KIBIBYTE);

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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd wr mr mw me ac";

        let region = Region::from_str(region).expect("Parsing failed");
        assert_eq!(
            region.pathname,
            "/opt/datadog-agent/embedded/lib/python3.9/site-packages/pydantic_core/_pydantic_core.cpython-39-aarch64-linux-gnu.so"
        );
        assert_eq!(region.size, 20 * BYTES_PER_KIBIBYTE);
        assert_eq!(region.private_dirty.unwrap(), 20 * BYTES_PER_KIBIBYTE);
    }

    #[test]
    fn test_varying_hex_len_mappings() {
        let region =
            "7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";
        let _region = Region::from_str(region).expect("Parsing failed");

        let region = "00400000-0e8dd000 r-xp 00000000 00:00 0                          [vdso]
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";

        let _region = Region::from_str(region).expect("Parsing failed");

        let region = "ffffffffff600000-ffffffffff601000 r-xp 00000000 00:00 0                          [vdso]
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
THPeligible:           0
ProtectionKey:         0
VmFlags:               rd ex mr mw me de sd";

        let _region = Region::from_str(region).expect("Parsing failed");
    }

    #[test]
    fn test_pathnames_with_whitespace() {
        let region =
            "7514356d1000-7514356d3000 rw-p 00000000 00:00 0                          [anon: glibc: loader malloc]
Size:                  8 kB
KernelPageSize:        4 kB
MMUPageSize:           4 kB
Rss:                   8 kB
Pss:                   8 kB
Pss_Dirty:             8 kB
Shared_Clean:          0 kB
Shared_Dirty:          0 kB
Private_Clean:         0 kB
Private_Dirty:         8 kB
Referenced:            8 kB
Anonymous:             8 kB
KSM:                   0 kB
LazyFree:              0 kB
AnonHugePages:         0 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:         0 kB
Shared_Hugetlb:        0 kB
Private_Hugetlb:       0 kB
Swap:                  0 kB
SwapPss:               0 kB
Locked:                0 kB
THPeligible:           0
ProtectionKey:         0
VmFlags: rd wr mr mw me ac sd";
        let parsed_region = Region::from_str(region).expect("Parsing failed");

        assert_eq!(parsed_region.pathname, ("[anon: glibc: loader malloc]"));
    }
}
