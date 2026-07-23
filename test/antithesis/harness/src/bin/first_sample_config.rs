//! Antithesis `first_` command: sample this timeline's lading config and release
//! the blocked system-under-test.
//!
//! Runs once per timeline after `setup_complete`, so the `AntithesisRng` draws
//! are post-snapshot decisions Antithesis branches: each timeline boots lading
//! under its own sampled config. Writes the config to the shared volume, tags
//! the sample for triage, then writes the `ready` sentinel last so the config is
//! always present before the SUT unblocks.

use std::path::PathBuf;

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    lading_antithesis::init();

    let dir: PathBuf =
        std::env::var_os("CONFIG_DIR").map_or_else(|| PathBuf::from("/shared"), PathBuf::from);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create config dir {}", dir.display()))?;

    // Draw structured choices from AntithesisRng so Antithesis branches each pick
    // and explores the config menu across timelines. UnwrapErr adapts the SDK's
    // fallible RNG to rand's infallible RngCore.
    let mut rng = rand::rand_core::UnwrapErr(antithesis_sdk::random::AntithesisRng);
    let cfg = harness::config::sample(&mut rng);
    let variant = harness::config::variant_label(&cfg.variant);
    let yaml = harness::config::to_yaml(&cfg).context("serialize sampled config")?;

    let config_path = dir.join("lading.yaml");
    std::fs::write(&config_path, yaml.as_bytes())
        .with_context(|| format!("write {}", config_path.display()))?;

    // Per-timeline anchor: counting these in triage shows how many distinct
    // variants the run explored.
    lading_antithesis::reachable!("first_sample_config sampled a config", { "variant": variant });

    let ready = dir.join("ready");
    std::fs::write(&ready, b"ready\n")
        .with_context(|| format!("write sentinel {}", ready.display()))?;

    Ok(())
}
