use metrics::gauge;
use std::path::Path;
use std::time::Instant;
use tokio::fs;

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

#[derive(Debug)]
struct Stats {
    usage_usec: u64,
    user_usec: u64,
    system_usec: u64,
    last_instant: Instant,
}

#[derive(Debug)]
pub(crate) struct Sampler {
    prev: Stats,
}

impl Sampler {
    pub(crate) fn new() -> Self {
        Self {
            prev: Stats {
                usage_usec: 0,
                user_usec: 0,
                system_usec: 0,
                last_instant: Instant::now(),
            },
        }
    }

    // Read cgroup CPU data and calculate a percentage of usage.
    pub(crate) async fn poll(
        &mut self,
        group_prefix: &Path,
        labels: &[(String, String)],
    ) -> Result<(), Error> {
        // Read cpu.max (cgroup v2)
        let cpu_max = fs::read_to_string(group_prefix.join("cpu.max")).await?;
        let parts: Vec<&str> = cpu_max.split_whitespace().collect();
        let (max_str, period_str) = (parts[0], parts[1]);
        let allowed_cores = if max_str == "max" {
            // If the target cgroup has no CPU limit we assume it has access to all
            // logical cores, inclusive of hyperthreaded cores.
            num_cpus::get() as f64
        } else {
            let max_val = max_str.parse::<f64>()?;
            let period_val = period_str.parse::<f64>()?;
            max_val / period_val
        };
        let limit_millicores = allowed_cores * 1000.0;

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

        let now = Instant::now();
        let delta_time = now.duration_since(self.prev.last_instant).as_micros();
        let delta_usage = usage_usec.saturating_sub(self.prev.usage_usec);
        let delta_user = user_usec.saturating_sub(self.prev.user_usec);
        let delta_system = system_usec.saturating_sub(self.prev.system_usec);

        // Update previous stats and if there's a time delta calculate the CPU
        // usage.
        self.prev.usage_usec = usage_usec;
        self.prev.user_usec = user_usec;
        self.prev.system_usec = system_usec;
        self.prev.last_instant = now;
        if delta_time > 0 {
            let delta_time = delta_time as f64;

            // Compute CPU usage as a fraction of a single CPU
            let usage_fraction = delta_usage as f64 / delta_time;
            let user_fraction = delta_user as f64 / delta_time;
            let system_fraction = delta_system as f64 / delta_time;

            // NOTE these metric names are paired with names in procfs/stat.rs and
            // must remain consistent. If you change these, change those.

            // Convert usage to a percentage of the cores granted to the target.
            let total_cpu = (usage_fraction / allowed_cores) * 100.0;
            let user_cpu = (user_fraction / allowed_cores) * 100.0;
            let system_cpu = (system_fraction / allowed_cores) * 100.0;
            gauge!("total_cpu_percentage", labels).set(total_cpu);
            gauge!("cpu_percentage", labels).set(total_cpu); // backward compatibility
            gauge!("user_cpu_percentage", labels).set(user_cpu);
            gauge!("kernel_cpu_percentage", labels).set(system_cpu); // kernel is a misnomer, keeping for compatibility
            gauge!("system_cpu_percentage", labels).set(system_cpu);

            // Convert usage to kubernetes style millicores.
            let total_millicores = usage_fraction * 1000.0;
            let user_millicores = user_fraction * 1000.0;
            let system_millicores = system_fraction * 1000.0;
            gauge!("total_cpu_usage_millicores", labels).set(total_millicores);
            gauge!("user_cpu_usage_millicores", labels).set(user_millicores);
            gauge!("kernel_cpu_usage_millicores", labels).set(system_millicores); // kernel is a misnomer, keeping for compatibility
            gauge!("system_cpu_usage_millicores", labels).set(system_millicores);
            gauge!("cpu_limit_millicores", labels).set(limit_millicores);
        }

        Ok(())
    }
}
