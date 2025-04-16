use metrics::gauge;
use procfs::process::{MemoryMap, MemoryPageFlags, PageInfo};
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::debug;

mod process_descendents;
use process_descendents::ProcessDescendentsIterator;

mod pfnset;
use pfnset::PfnSet;

const PAGE_OFFSET: u64 = 0xffff_8800_0000_0000;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Wrapper for [`procfs::ProcError`]
    #[error("Unable to read procfs: {0}")]
    Proc(#[from] procfs::ProcError),
}

#[derive(Debug)]
pub(crate) struct Sampler {
    parent_pid: i32,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32) -> Result<Self, Error> {
        Ok(Self { parent_pid })
    }

    pub(crate) async fn poll(&mut self) -> Result<(), Error> {
        let page_size = page_size::get();
        let mut pfn_set = PfnSet::new();

        for process in ProcessDescendentsIterator::new(self.parent_pid) {
            debug!("Process PID: {}", process.pid());
            let mut pagemap = process.pagemap()?;
            for MemoryMap {
                address: (begin, end),
                ..
            } in process.maps()?
            {
                if begin > PAGE_OFFSET {
                    continue; // page idle tracking is user mem only
                }
                debug!("Memory region: {:#x} — {:#x}", begin, end);
                let begin = begin as usize / page_size;
                let end = end as usize / page_size;
                for page in pagemap.get_range_info(begin..end)? {
                    if let PageInfo::MemoryPage(memory_page_flags) = page {
                        if memory_page_flags.contains(MemoryPageFlags::PRESENT) {
                            pfn_set.insert(memory_page_flags.get_page_frame_number());
                        }
                    }
                }
            }
        }

        let mut nb_pages = 0;

        // See https://www.kernel.org/doc/html/latest/admin-guide/mm/idle_page_tracking.html
        let mut page_idle_bitmap = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/sys/kernel/mm/page_idle/bitmap")?;

        for (pfn_block, pfn_bitset) in pfn_set {
            page_idle_bitmap.seek(SeekFrom::Start(pfn_block * 8))?;

            let mut buffer = [0; 8];
            page_idle_bitmap.read_exact(&mut buffer)?;
            let bitset = u64::from_ne_bytes(buffer);

            nb_pages += (!bitset & pfn_bitset).count_ones() as usize;

            page_idle_bitmap.seek(SeekFrom::Start(pfn_block * 8))?;
            page_idle_bitmap.write_all(&pfn_bitset.to_ne_bytes())?;
        }

        gauge!("total_wss_bytes").set((nb_pages * page_size) as f64);

        Ok(())
    }
}
