use metrics::gauge;
use procfs::process::{MemoryMap, MemoryPageFlags, PageInfo, Pfn, Process};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::debug;

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

struct ProcessDescendentsIterator {
    stack: Vec<Process>,
}

impl ProcessDescendentsIterator {
    fn new(parent_pid: i32) -> Self {
        Self {
            stack: vec![
                Process::new(parent_pid).expect(format!("process {parent_pid} not found").as_str()),
            ],
        }
    }
}

impl Iterator for ProcessDescendentsIterator {
    type Item = Process;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(process) = self.stack.pop() {
            if let Ok(tasks) = process.tasks() {
                for task in tasks.flatten() {
                    if let Ok(children) = task.children() {
                        for child in children {
                            if let Ok(c) = Process::new(child as i32) {
                                self.stack.push(c);
                            }
                        }
                    }
                }
            }
            return Some(process);
        }
        None
    }
}

#[derive(Debug)]
struct PfnSet(HashMap<u64, u64>);

impl PfnSet {
    fn new() -> Self {
        Self(HashMap::with_capacity(1024))
    }

    fn insert(&mut self, pfn: Pfn) {
        *self.0.entry(pfn.0 / 64).or_default() |= 1 << (pfn.0 % 64);
    }
}

impl IntoIterator for PfnSet {
    type Item = (u64, u64);
    type IntoIter = std::collections::hash_map::IntoIter<u64, u64>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::io::BufRead;
    use std::io::BufReader;
    use std::process::{Command, Stdio};

    #[test]
    fn process_descendants_iterator() {
        const NB_PROCESSES_PER_LEVEL: usize = 3;
        const NB_LEVELS: u32 = 3;
        // The total number of processes is the sum of the NB_LEVELS first terms
        // of the geometric progression with common ratio of NB_PROCESSES_PER_LEVEL.
        const NB_PROCESSES: usize =
            (NB_PROCESSES_PER_LEVEL.pow(NB_LEVELS + 1) - 1) / (NB_PROCESSES_PER_LEVEL - 1);

        let mut child = Command::new("src/observer/linux/wss/tests/create_process_tree.py")
            .arg(NB_PROCESSES_PER_LEVEL.to_string())
            .arg(NB_LEVELS.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to create process tree");

        let mut children_pids = HashSet::with_capacity(NB_PROCESSES);
        children_pids.insert(child.id() as i32);

        let mut reader = BufReader::new(child.stdout.take().unwrap());
        for _ in 0..NB_PROCESSES - 1 {
            let mut line = String::new();
            reader.read_line(&mut line).expect("Failed to read line");
            let pid: i32 = line.trim().parse().expect("Failed to parse PID");
            assert!(children_pids.insert(pid));
        }

        for process in ProcessDescendentsIterator::new(child.id() as i32) {
            assert!(
                children_pids.remove(&process.pid()),
                "ProcessDescendentsIterator returned unexpected PID {pid}",
                pid = process.pid()
            );
        }
        assert!(
            children_pids.is_empty(),
            "ProcessDescendentsIterator didn’t return all PIDs: {children_pids:?}"
        );

        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(child.id() as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .expect("Failed to kill process tree");

        child
            .wait()
            .expect("Failed to wait for process tree completion");
    }

    #[test]
    fn pfn_set() {
        let input = vec![0, 64, 65, 66, 128, 136, 255];
        let expected = vec![(0, 0x1), (1, 0x7), (2, 0x101), (3, 0x8000_0000_0000_0000)];

        let mut pfn_set = PfnSet::new();
        for i in input {
            pfn_set.insert(Pfn(i));
        }
        let mut output: Vec<_> = pfn_set.into_iter().collect();
        output.sort_by_key(|(k, _)| *k);

        assert_eq!(output, expected);
    }
}
