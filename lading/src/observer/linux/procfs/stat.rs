use metrics::gauge;
use once_cell::sync::OnceCell;
use std::sync::Mutex;
use std::time::Instant;
use tokio::fs;

use crate::observer::linux::cgroup;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Float Parsing: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("Integer Parsing: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Stat Malformed: {0}")]
    StatMalformed(&'static str),
    #[error("Cgroup get_path: {0}")]
    Cgroup(#[from] cgroup::v2::Error),
}
struct PrevStats {
    total_ticks: u64,
    user_ticks: u64,
    system_ticks: u64,
    last_instant: Instant,
}

static PREV: OnceCell<Mutex<PrevStats>> = OnceCell::new();

pub(crate) async fn poll(pid: i32, labels: &[(String, String)]) -> Result<(), Error> {
    let group_prefix = cgroup::v2::get_path(pid).await?;

    // Read cpu.max (cgroup v2)
    let cpu_max = fs::read_to_string(group_prefix.join("cpu.max")).await?;
    let parts: Vec<&str> = cpu_max.split_whitespace().collect();
    let (max_str, period_str) = (parts[0], parts[1]);
    let allowed_cores = if max_str == "max" {
        // If the target cgroup has no CPU limit we assume it has access to all
        // physical cores.
        num_cpus::get_physical() as f64
    } else {
        let max_val = max_str.parse::<f64>()?;
        let period_val = period_str.parse::<f64>()?;
        max_val / period_val
    };
    let limit_millicores = allowed_cores * 1000.0;

    // Read `/proc/<PID>/stat`
    let stat_contents = fs::read_to_string(format!("/proc/{pid}/stat")).await?;
    let start_paren = stat_contents
        .find('(')
        .ok_or_else(|| Error::StatMalformed("Failed to find '(' in stat contents"))?;
    let end_paren = stat_contents
        .rfind(')')
        .ok_or_else(|| Error::StatMalformed("Failed to find ')' in stat contents"))?;
    let before = &stat_contents[..start_paren];
    let after = &stat_contents[end_paren + 2..]; // skip ") "

    let mut parts: Vec<&str> = before.split_whitespace().collect();
    let pid_str = parts[0]; // PID
    let _pid_val: u32 = pid_str.parse()?; // confirm PID

    parts = after.split_whitespace().collect();
    // Alright, per the proc(5) manpage utime = field 14, stime = field 15.
    // After skipping the name, field #3 is parts[0], so utime (14) is
    // parts[11], stime (15) is parts[12].
    let utime_ticks: u64 = parts[11].parse()?;
    let stime_ticks: u64 = parts[12].parse()?;
    let total_ticks = utime_ticks + stime_ticks;

    // See sysconf(3).
    let ticks_per_second = unsafe { nix::libc::sysconf(nix::libc::_SC_CLK_TCK) } as f64;
    // Get or initialize the previous stats. Note that the first time this is
    // initialized we intentionally set last_instance to now to avoid scheduling
    // shenanigans.
    let now = Instant::now();
    let mut prev = PREV
        .get_or_init(|| {
            Mutex::new(PrevStats {
                total_ticks,
                user_ticks: utime_ticks,
                system_ticks: stime_ticks,
                last_instant: now,
            })
        })
        .lock()
        .expect("Lock poisoned");

    let delta_time = now.duration_since(prev.last_instant).as_micros();
    let delta_total_ticks = total_ticks.saturating_sub(prev.total_ticks);
    let delta_user_ticks = utime_ticks.saturating_sub(prev.user_ticks);
    let delta_system_ticks = stime_ticks.saturating_sub(prev.system_ticks);

    // Update previous stats and if there's a time delta calculate the CPU
    // usage.
    prev.total_ticks = total_ticks;
    prev.user_ticks = utime_ticks;
    prev.system_ticks = stime_ticks;
    prev.last_instant = now;
    if delta_time > 0 {
        let delta_time = delta_time as f64;

        let tick_to_usec = 1_000_000.0 / ticks_per_second;

        let delta_usage_usec = (delta_total_ticks as f64) * tick_to_usec;
        let delta_user_usec = (delta_user_ticks as f64) * tick_to_usec;
        let delta_system_usec = (delta_system_ticks as f64) * tick_to_usec;

        // Compute CPU usage as a fraction of a single CPU
        let usage_fraction = delta_usage_usec / delta_time;
        let user_fraction = delta_user_usec / delta_time;
        let system_fraction = delta_system_usec / delta_time;

        // NOTE these metric names are paired with names in cgroup/v2/cpu.rs and
        // must remain consistent. If you change these, change those.

        // Convert usage to a percentage of the cores granted to the target.
        let total_cpu = (usage_fraction / allowed_cores) * 100.0;
        let user_cpu = (user_fraction / allowed_cores) * 100.0;
        let system_cpu = (system_fraction / allowed_cores) * 100.0;
        gauge!("stat.total_cpu_percentage", labels).set(total_cpu);
        gauge!("stat.cpu_percentage", labels).set(total_cpu); // backward compatibility
        gauge!("stat.user_cpu_percentage", labels).set(user_cpu);
        gauge!("stat.kernel_cpu_percentage", labels).set(system_cpu); // kernel is a misnomer, keeping for compatibility
        gauge!("stat.system_cpu_percentage", labels).set(system_cpu);

        // Convert usage to kubernetes style millicores.
        let total_millicores = usage_fraction * 1000.0;
        let user_millicores = user_fraction * 1000.0;
        let system_millicores = system_fraction * 1000.0;
        gauge!("stat.total_cpu_usage_millicores", labels).set(total_millicores);
        gauge!("stat.user_cpu_usage_millicores", labels).set(user_millicores);
        gauge!("stat.kernel_cpu_usage_millicores", labels).set(system_millicores); // kernel is a misnomer, keeping for compatibility
        gauge!("stat.system_cpu_usage_millicores", labels).set(system_millicores);
        gauge!("stat.cpu_limit_millicores", labels).set(limit_millicores);
    }

    Ok(())
}
