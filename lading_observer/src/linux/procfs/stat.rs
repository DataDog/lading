use metrics::gauge;
use tokio::fs;

use crate::linux::cgroup;

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

#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_field_names)] // The _ticks is useful even if clippy doesn't like it.
struct Stats {
    user_ticks: u64,
    system_ticks: u64,
    uptime_ticks: u64,
}

#[derive(Debug, Clone, Copy)]
struct CpuUtilization {
    total_cpu_percentage: f64,
    user_cpu_percentage: f64,
    system_cpu_percentage: f64,
    total_cpu_millicores: f64,
    user_cpu_millicores: f64,
    system_cpu_millicores: f64,
}

#[derive(Debug)]
pub(crate) struct Sampler {
    ticks_per_second: f64,
    prev: Stats,
}

impl Sampler {
    pub(crate) fn new() -> Self {
        Self {
            ticks_per_second: unsafe { nix::libc::sysconf(nix::libc::_SC_CLK_TCK) } as f64,
            prev: Stats::default(),
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub(crate) async fn poll(
        &mut self,
        pid: i32,
        uptime_secs: f64,
        labels: &[(&'static str, String)],
    ) -> Result<(), Error> {
        let group_prefix = cgroup::v2::get_path(pid).await?;

        // Read cpu.max (cgroup v2)
        let cpu_max = fs::read_to_string(group_prefix.join("cpu.max")).await?;
        let parts: Vec<&str> = cpu_max.split_whitespace().collect();
        let (max_str, period_str) = (parts[0], parts[1]);
        let allowed_cores = if max_str == "max" {
            // If the target cgroup has no CPU limit we assume it has access to
            // all cores.
            num_cpus::get() as f64
        } else {
            let max_val = max_str.parse::<f64>()?;
            let period_val = period_str.parse::<f64>()?;
            max_val / period_val
        };
        let limit_millicores = allowed_cores * 1000.0;

        // Read `/proc/<PID>/stat`
        let stat_contents = fs::read_to_string(format!("/proc/{pid}/stat")).await?;
        let (cur_pid, utime_ticks, stime_ticks) = parse(&stat_contents)?;
        assert!(cur_pid == pid);

        // Get or initialize the previous stats. Note that the first time this is
        // initialized we intentionally set last_instance to now to avoid scheduling
        // shenanigans.
        let cur_stats = Stats {
            user_ticks: utime_ticks,
            system_ticks: stime_ticks,
            uptime_ticks: (uptime_secs * self.ticks_per_second).round() as u64,
        };

        if let Some(util) = compute_cpu_usage(self.prev, cur_stats, allowed_cores) {
            // NOTE these metric names are paired with names in cgroup/v2/cpu.rs and
            // must remain consistent. If you change these, change those.
            gauge!("stat.total_cpu_percentage", labels).set(util.total_cpu_percentage);
            gauge!("stat.cpu_percentage", labels).set(util.total_cpu_percentage); // backward compatibility
            gauge!("stat.user_cpu_percentage", labels).set(util.user_cpu_percentage);
            gauge!("stat.kernel_cpu_percentage", labels).set(util.system_cpu_percentage); // kernel is a misnomer, keeping for compatibility
            gauge!("stat.system_cpu_percentage", labels).set(util.system_cpu_percentage);

            gauge!("stat.total_cpu_usage_millicores", labels).set(util.total_cpu_millicores);
            gauge!("stat.user_cpu_usage_millicores", labels).set(util.user_cpu_millicores);
            gauge!("stat.kernel_cpu_usage_millicores", labels).set(util.system_cpu_millicores); // kernel is a misnomer
            gauge!("stat.system_cpu_usage_millicores", labels).set(util.system_cpu_millicores);
            gauge!("stat.cpu_limit_millicores", labels).set(limit_millicores);
        }

        self.prev = cur_stats;

        Ok(())
    }
}

/// Parse `/proc/<pid>/stat` and extracts:
///
/// * pid (1st field)
/// * utime (14th field)
/// * stime (15th field)
///
/// The pid we already have and it's a check. The utime and stime are going to
/// be used to calculate CPU data.
///
/// # Errors
///
/// Function will fail if the stat file is malformed.
fn parse(contents: &str) -> Result<(i32, u64, u64), Error> {
    // Search first for command name by searching for the parantheses that
    // surround it. These allow us to divide the stat file into two parts, the
    // bit with the pid and all the rest of the fields that Linux adds to over
    // time.
    let start_paren = contents
        .find('(')
        .ok_or(Error::StatMalformed("Failed to find '(' in stat contents"))?;
    let end_paren = contents
        .rfind(')')
        .ok_or(Error::StatMalformed("Failed to find ')' in stat contents"))?;

    let before = &contents[..start_paren];
    let after = &contents[end_paren + 2..]; // skip ") "
    let before_parts: Vec<&str> = before.split_whitespace().collect();
    if before_parts.is_empty() {
        return Err(Error::StatMalformed("Not enough fields before paren"));
    }

    let pid_str = before_parts[0];
    let pid = pid_str.parse::<i32>()?;

    let after_parts: Vec<&str> = after.split_whitespace().collect();
    // Okay, looking at proc_pid_stat(5) here's a little table to convince you
    // the indexes are right:
    //
    // Field #   Name       Index in after_parts
    // 3         state      0
    // 4         ppid       1
    // 5         pgrp       2
    // 6         session    3
    // 7         tty_nr     4
    // 8         tpgid      5
    // 9         flags      6
    // 10        minflt     7
    // 11        cminflt    8
    // 12        majflt     9
    // 13        cmajflt    10
    // 14        utime      11
    // 15        stime      12

    // There might be more fields after stime, but we don't parse these.
    if after_parts.len() < 13 {
        return Err(Error::StatMalformed("Not enough fields after comm"));
    }

    let utime = after_parts[11].parse::<u64>()?;
    let stime = after_parts[12].parse::<u64>()?;

    Ok((pid, utime, stime))
}

/// Computes CPU usage given current and previous `Stats`.
///
/// Returns a `CpuUtilization` struct if successful, or `None` if no time has passed.
fn compute_cpu_usage(prev: Stats, cur: Stats, allowed_cores: f64) -> Option<CpuUtilization> {
    // Time in ticks since between prev and cur samples.
    let delta_time = cur.uptime_ticks.saturating_sub(prev.uptime_ticks);
    // If time has not passed we cannot make any claims.
    if delta_time == 0 {
        return None;
    }
    // Calculate actual time passed in CPU ticks.
    let delta_user_ticks = cur.user_ticks.saturating_sub(prev.user_ticks);
    let delta_system_ticks = cur.system_ticks.saturating_sub(prev.system_ticks);
    let delta_total_ticks = delta_user_ticks + delta_system_ticks;

    // Fraction of one core's capacity used.
    let usage_fraction = (delta_total_ticks as f64) / (delta_time as f64);
    let user_fraction = (delta_user_ticks as f64) / (delta_time as f64);
    let system_fraction = (delta_system_ticks as f64) / (delta_time as f64);

    // Calculate percentage and millicore views.
    let total_cpu_percentage = (usage_fraction / allowed_cores) * 100.0;
    let user_cpu_percentage = (user_fraction / allowed_cores) * 100.0;
    let system_cpu_percentage = (system_fraction / allowed_cores) * 100.0;
    let total_cpu_millicores = usage_fraction * 1000.0;
    let user_cpu_millicores = user_fraction * 1000.0;
    let system_cpu_millicores = system_fraction * 1000.0;

    Some(CpuUtilization {
        total_cpu_percentage,
        user_cpu_percentage,
        system_cpu_percentage,
        total_cpu_millicores,
        user_cpu_millicores,
        system_cpu_millicores,
    })
}

#[cfg(test)]
mod test {
    use super::{Stats, compute_cpu_usage, parse};

    #[test]
    fn parse_basic() {
        let line =
            "1234 (some process) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23";
        let (pid, utime, stime) = parse(line).unwrap();
        assert_eq!(pid, 1234);
        assert_eq!(utime, 11);
        assert_eq!(stime, 12);
    }

    #[test]
    fn compute_cpu_usage_basic() {
        // 1 second of time passes over 1 allowed core with 100 ticks per
        // second. Values chosen to make the math simple.

        let allowed_cores = 1.0;

        let prev = Stats {
            user_ticks: 1_000,
            system_ticks: 2_000,
            uptime_ticks: 0,
        };
        let cur = Stats {
            user_ticks: 1_500,
            system_ticks: 2_500,
            uptime_ticks: 1_000,
        };

        let util = compute_cpu_usage(prev, cur, allowed_cores).unwrap();
        assert!((util.total_cpu_percentage - 100.0).abs() < f64::EPSILON);
        assert!((util.user_cpu_percentage - 50.0).abs() < f64::EPSILON);
        assert!((util.system_cpu_percentage - 50.0).abs() < f64::EPSILON);
        assert!((util.total_cpu_millicores - 1000.0).abs() < f64::EPSILON);
        assert!((util.user_cpu_millicores - 500.0).abs() < f64::EPSILON);
        assert!((util.system_cpu_millicores - 500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compute_cpu_usage_no_time_passed() {
        let allowed_cores = 1.0;
        let prev = Stats {
            user_ticks: 1_000,
            system_ticks: 2_000,
            uptime_ticks: 10_000,
        };
        let cur = Stats {
            user_ticks: prev.user_ticks + 100,
            system_ticks: prev.system_ticks + 100,
            uptime_ticks: prev.uptime_ticks, // no time passed
        };

        let util = compute_cpu_usage(prev, cur, allowed_cores);
        assert!(util.is_none());
    }

    #[test]
    fn compute_cpu_usage_fractional_cores() {
        let allowed_cores = 0.5;
        let prev = Stats {
            user_ticks: 1_000,
            system_ticks: 2_000,
            uptime_ticks: 0,
        };
        let cur = Stats {
            user_ticks: prev.user_ticks + 500,
            system_ticks: prev.system_ticks + 500,
            uptime_ticks: prev.uptime_ticks + 1_000,
        };

        let util = compute_cpu_usage(prev, cur, allowed_cores).unwrap();
        assert!((util.total_cpu_percentage - 200.0).abs() < f64::EPSILON);
        assert!((util.user_cpu_percentage - 100.0).abs() < f64::EPSILON);
        assert!((util.system_cpu_percentage - 100.0).abs() < f64::EPSILON);
        assert!((util.total_cpu_millicores - 1000.0).abs() < f64::EPSILON);
        assert!((util.user_cpu_millicores - 500.0).abs() < f64::EPSILON);
        assert!((util.system_cpu_millicores - 500.0).abs() < f64::EPSILON);
    }
}
