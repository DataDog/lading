use regex::Regex;
use std::io::Read;

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub(crate) enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Number Parsing: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

pub(crate) struct Regions(pub(crate) Vec<Region>);

pub(crate) struct Region {
    // Metadata
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) perms: String,
    pub(crate) offset: u64,
    pub(crate) dev: String,
    pub(crate) inode: u64,
    pub(crate) pathname: String,
    // Values
    pub(crate) size: u64,
    pub(crate) pss: u64,
    pub(crate) swap: u64,
    pub(crate) rss: u64,
    pub(crate) pss_dirty: u64,
}

impl Region {
    #[allow(clippy::similar_names)]
    pub(crate) fn from_str(contents: &str) -> Result<Self, Error> {
        let mut lines = contents.lines();

        let metadata_line = lines.next().unwrap();
        let metadata = metadata_line.split_whitespace().collect::<Vec<_>>();

        let values = lines
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        // lol this is all gpt-generated, I don't think it'll be correct
        // but lets try it out maybe it works
        // okay update, gpt got it pretty much entirely right
        // not very efficient to allocate all these Vecs, but w/e

        let start = u64::from_str_radix(&metadata[0][..12], 16)?;
        let end = u64::from_str_radix(&metadata[0][13..], 16)?;
        let perms = metadata[1].to_string();
        let offset = u64::from_str_radix(&metadata[2], 16)?;
        let dev = metadata[3].to_string();
        let inode = u64::from_str_radix(&metadata[4], 10)?;
        let pathname = metadata[5].to_string();

        let size = u64::from_str_radix(&values[0][1], 10)?;
        let rss = u64::from_str_radix(&values[3][1], 10)?;
        let pss = u64::from_str_radix(&values[4][1], 10)?;
        let pss_dirty = u64::from_str_radix(&values[5][1], 10)?;
        let swap = u64::from_str_radix(&values[18][1], 10)?;

        // todo no units taken into account right now
        // everything in is kB, but implicitly
        Ok(Region {
            start,
            end,
            perms,
            offset,
            dev,
            inode,
            pathname,
            size,
            pss,
            swap,
            rss,
            pss_dirty,
        })
    }
}

impl Regions {
    pub(crate) fn from_pid(pid: i32) -> Result<Self, Error> {
        Regions::from_file(&format!("/proc/{pid}/smaps"))
    }

    fn from_file(path: &str) -> Result<Self, Error> {
        let mut file: std::fs::File = std::fs::OpenOptions::new().read(true).open(path)?;

        let mut contents = String::new(); // todo add pre-set capacity probs
        file.read_to_string(&mut contents).unwrap();

        Self::from_str(&contents)
    }

    fn from_str(contents: &str) -> Result<Self, Error> {
        let mut str_regions = Vec::new();
        // split this smaps file into regions
        // regions are separated by a line like this:
        // 7fffa9f39000-7fffa9f3b000 r-xp 00000000 00:00 0                          [vdso]
        let mut start_index = 0;
        let region_start_regex =
            Regex::new("[0-9a-fA-F]{12}-[0-9a-fA-F]{12}").expect("Regex to be valid"); // todo can I use char classes here?
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
mod tests {
    use super::*;

    const TWO_REGION: &str = "
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

    #[allow(clippy::unreadable_literal)]
    #[test]
    fn test_basic_case() {
        let regions = Regions::from_str(TWO_REGION).unwrap();
        assert_eq!(regions.0.len(), 2);

        let region_one = &regions.0[0];
        assert_eq!(region_one.start, 0x7fffa9f35000);
        assert_eq!(region_one.end, 0x7fffa9f39000);
        assert_eq!(region_one.perms, "r--p");
        assert_eq!(region_one.offset, 0);
        assert_eq!(region_one.dev, "12:11");
        assert_eq!(region_one.inode, 0);
        assert_eq!(region_one.pathname, "[vvar]");
        assert_eq!(region_one.size, 16);
        assert_eq!(region_one.pss, 0);
        assert_eq!(region_one.swap, 7);
        assert_eq!(region_one.rss, 0);
        assert_eq!(region_one.pss_dirty, 0);

        let region_two = &regions.0[1];
        assert_eq!(region_two.start, 0x7fffa9f39000);
        assert_eq!(region_two.end, 0x7fffa9f3b000);
        assert_eq!(region_two.perms, "r-xp");
        assert_eq!(region_two.offset, 0);
        assert_eq!(region_two.dev, "00:00");
        assert_eq!(region_two.inode, 0);
        assert_eq!(region_two.pathname, "[vdso]");
        assert_eq!(region_two.size, 8);
        assert_eq!(region_two.pss, 2);
        assert_eq!(region_two.swap, 0);
        assert_eq!(region_two.rss, 8);
        assert_eq!(region_two.pss_dirty, 0);
    }
}
