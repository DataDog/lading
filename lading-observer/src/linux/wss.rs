use metrics::gauge;
use nix::unistd::AccessFlags;
use procfs::process::{MemoryMap, MemoryPageFlags, PageInfo};
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
};
use tracing::debug;

use crate::linux::utils::process_descendents::ProcessDescendantsIterator;

mod pfnset;
use pfnset::PfnSet;

pub(super) const PAGE_IDLE_BITMAP: &str = "/sys/kernel/mm/page_idle/bitmap";

#[cfg(target_arch = "x86_64")]
// From https://github.com/torvalds/linux/blob/c62f4b82d57155f35befb5c8bbae176614b87623/arch/x86/include/asm/page_64_types.h#L42
const PAGE_OFFSET: u64 = 0xffff_8800_0000_0000;
#[cfg(target_arch = "aarch64")]
// On arm64, the virtual address at which user space ends is more configurable
// See https://github.com/torvalds/linux/blob/c62f4b82d57155f35befb5c8bbae176614b87623/arch/arm64/include/asm/memory.h#L43-L45
// So, I checked one of our staging machine:
//        # grep VA_BITS /host/proc/1/root/boot/config-6.8.0-1027-aws
//        # CONFIG_ARM64_VA_BITS_39 is not set
//        CONFIG_ARM64_VA_BITS_48=y
//        CONFIG_ARM64_VA_BITS=48
const PAGE_OFFSET: u64 = u64::MAX << 48;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(target_os = "linux")]
    /// Wrapper for [`procfs::ProcError`]
    #[error("Unable to read procfs: {0}")]
    Proc(#[from] procfs::ProcError),
}

#[derive(Debug)]
pub(crate) struct Sampler {
    parent_pid: i32,
    page_idle_bitmap: std::fs::File,
}

impl Sampler {
    pub(crate) fn is_available() -> bool {
        nix::unistd::access(PAGE_IDLE_BITMAP, AccessFlags::R_OK | AccessFlags::W_OK).is_ok()
    }

    pub(crate) fn new(parent_pid: i32) -> Result<Self, Error> {
        Ok(Self {
            parent_pid,
            // See https://www.kernel.org/doc/html/latest/admin-guide/mm/idle_page_tracking.html
            page_idle_bitmap: fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(PAGE_IDLE_BITMAP)?,
        })
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn poll(&mut self) -> Result<(), Error> {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let page_size = unsafe { nix::libc::sysconf(nix::libc::_SC_PAGESIZE) as usize };
        let mut pfn_set = PfnSet::new();

        for process in ProcessDescendantsIterator::new(self.parent_pid) {
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
                #[allow(clippy::cast_possible_truncation)]
                let begin = begin as usize / page_size;
                #[allow(clippy::cast_possible_truncation)]
                let end = end as usize / page_size;
                for page in pagemap.get_range_info(begin..end)? {
                    if let PageInfo::MemoryPage(memory_page_flags) = page
                        && memory_page_flags.contains(MemoryPageFlags::PRESENT)
                    {
                        pfn_set.insert(memory_page_flags.get_page_frame_number());
                    }
                }
            }
        }

        let mut nb_pages = 0;

        for (pfn_block, pfn_bitset) in pfn_set {
            self.page_idle_bitmap.seek(SeekFrom::Start(pfn_block * 8))?;

            let mut buffer = [0; 8];
            self.page_idle_bitmap.read_exact(&mut buffer)?;
            let bitset = u64::from_ne_bytes(buffer);

            nb_pages += (!bitset & pfn_bitset).count_ones() as usize;

            self.page_idle_bitmap.seek(SeekFrom::Start(pfn_block * 8))?;
            self.page_idle_bitmap.write_all(&pfn_bitset.to_ne_bytes())?;
        }

        gauge!("total_wss_bytes").set((nb_pages * page_size) as f64);

        Ok(())
    }
}
