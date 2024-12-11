use metrics::gauge;
use once_cell::sync::OnceCell;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;
use tokio::fs;

use tracing::info;

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub(crate) enum Error {
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Float Parsing: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("Integer Parsing: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

struct PrevStats {
    usage_usec: u64,
    user_usec: u64,
    system_usec: u64,
    last_instant: Instant,
}

static PREV: OnceCell<Mutex<PrevStats>> = OnceCell::new();

// Read cgroup CPU data and calculate a percentage of usage.
pub(crate) async fn poll(group_prefix: &Path, labels: &[(String, String)]) -> Result<(), Error> {
    // Read cpu.max (cgroup v2)
    let cpu_max = fs::read_to_string(group_prefix.join("cpu.max")).await?;
    let parts: Vec<&str> = cpu_max.split_whitespace().collect();
    let (max_str, period_str) = (parts[0], parts[1]);
    let allowed_cores = if max_str == "max" {
        num_cpus::get() as f64
    } else {
        let max_val = max_str.parse::<f64>()?;
        let period_val = period_str.parse::<f64>()?;
        max_val / period_val
    };

    // Read cpu.stat
    let cpu_stat = fs::read_to_string(group_prefix.join("cpu.stat")).await?;
    let mut usage_usec = 0u64;
    let mut user_usec = 0u64;
    let mut system_usec = 0u64;

    for line in cpu_stat.lines() {
        let mut parts = line.split_whitespace();
        let key = parts.next().expect("no key");
        let value: u64 = parts.next().expect("no value").parse()?;
        match key {
            "usage_usec" => usage_usec = value,
            "user_usec" => user_usec = value,
            "system_usec" => system_usec = value,
            _ => {}
        }
    }

    // Get or initialize previous stats
    let mut prev = PREV
        .get_or_init(|| {
            Mutex::new(PrevStats {
                usage_usec,
                user_usec,
                system_usec,
                last_instant: Instant::now(),
            })
        })
        .lock()
        .expect("could not lock stats, poisoned by my constituents");

    let now = Instant::now();
    let delta_time = now.duration_since(prev.last_instant).as_micros() as f64;
    let delta_usage = usage_usec.saturating_sub(prev.usage_usec);
    let delta_user = user_usec.saturating_sub(prev.user_usec);
    let delta_system = system_usec.saturating_sub(prev.system_usec);

    // Update previous stats
    prev.usage_usec = usage_usec;
    prev.user_usec = user_usec;
    prev.system_usec = system_usec;
    prev.last_instant = now;

    if delta_time > 0.0 {
        // Compute CPU usage as a percentage of the cgroup CPU allocation
        let total_cpu = (delta_usage as f64 / delta_time) / allowed_cores * 100.0;
        let user_cpu = (delta_user as f64 / delta_time) / allowed_cores * 100.0;
        let system_cpu = (delta_system as f64 / delta_time) / allowed_cores * 100.0;

        info!(
            "cpu: {total_cpu:.2}%, user: {user_cpu:.2}%, system: {system_cpu:.2}%",
            total_cpu = total_cpu,
            user_cpu = user_cpu,
            system_cpu = system_cpu
        );

        gauge!("cgroup_total_cpu_percentage", labels).set(total_cpu);
        gauge!("cgroup_user_cpu_percentage", labels).set(user_cpu);
        gauge!("cgroup_system_cpu_percentage", labels).set(system_cpu);
    }

    Ok(())
}
