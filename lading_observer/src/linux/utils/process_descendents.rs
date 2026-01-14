use procfs::process::Process;

/// Iterator which, given a process ID, returns the process and all its descendants
pub(in crate::linux) struct ProcessDescendantsIterator {
    stack: Vec<Process>,
}

impl ProcessDescendantsIterator {
    pub(in crate::linux) fn new(parent_pid: i32) -> Self {
        Self {
            stack: vec![
                Process::new(parent_pid)
                    .unwrap_or_else(|e| panic!("process {parent_pid} not found: {e}")),
            ],
        }
    }
}

impl Iterator for ProcessDescendantsIterator {
    type Item = Process;

    fn next(&mut self) -> Option<Self::Item> {
        let process = self.stack.pop()?;
        if let Ok(tasks) = process.tasks() {
            for task in tasks.flatten() {
                if let Ok(children) = task.children() {
                    for child in children {
                        #[allow(clippy::cast_possible_wrap)]
                        if let Ok(c) = Process::new(child as i32) {
                            self.stack.push(c);
                        }
                    }
                }
            }
        }
        Some(process)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::io::BufRead;
    use std::io::BufReader;
    use std::process::{Command, Stdio};

    use nix::{sys::signal, unistd};

    /// Test the ProcessDescendentsIterator by creating a process tree.
    /// Each process will print its PID to stdout.
    /// The test will read the PIDs from stdout and check that the iterator returns all of them.
    #[test]
    fn process_descendants_iterator() {
        const NB_PROCESSES_PER_LEVEL: usize = 3;
        const NB_LEVELS: u32 = 3;
        // The total number of processes is the sum of the NB_LEVELS first terms
        // of the geometric progression with common ratio of NB_PROCESSES_PER_LEVEL.
        const NB_PROCESSES: usize =
            (NB_PROCESSES_PER_LEVEL.pow(NB_LEVELS + 1) - 1) / (NB_PROCESSES_PER_LEVEL - 1);

        let mut child = Command::new("src/linux/utils/tests/create_process_tree.py")
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

        for process in ProcessDescendantsIterator::new(child.id() as i32) {
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

        signal::kill(
            unistd::Pid::from_raw(child.id() as i32),
            signal::Signal::SIGTERM,
        )
        .expect("Failed to kill process tree");

        child
            .wait()
            .expect("Failed to wait for process tree completion");
    }
}
