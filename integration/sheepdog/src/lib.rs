//! Sheepdog is an integration testing harness for lading.
//!
//! Sheepdog performs correctness testing on lading by running it against ducks.
//! Ducks exhibits testable behavior and exposes measurements for sheepdog to
//! assert against.
//!
//! Currently, sheepdog performs these tasks:
//! - Via ducks, verify that lading produces data
//!
//! Upcoming goals:
//! - Via ducks, verify that lading sinks data
//! - Verify that lading capture outputs are parseable
//! - Attempt to validate lading's data rate consistency
//! - Verify that lading generates meaningful data (is entropy a good enough signal?)

use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::Context;
use assert_fs::TempDir;
use shared::DucksMessage;

/// Run a cargo build of ducks and return the path of the output binary
pub fn build_ducks() -> Result<PathBuf, anyhow::Error> {
    let bin = escargot::CargoBuild::new()
        .bin("ducks")
        .package("ducks")
        .current_release()
        .current_target()
        .manifest_path("Cargo.toml")
        .run()?
        .path()
        .to_owned();
    Ok(bin)
}

/// Run a cargo build of lading and return the path of the output binary
// tbh not sure if we should be building lading or accepting a binary
pub fn build_lading() -> Result<PathBuf, anyhow::Error> {
    let bin = escargot::CargoBuild::new()
        .bin("lading")
        .package("lading")
        .current_release()
        .current_target()
        .manifest_path("Cargo.toml")
        .run()?
        .path()
        .to_owned();
    Ok(bin)
}

pub struct DucksRun {
    lading_config_template: String,
    tempdir: TempDir,
    experiment_duration: Duration,
    experiment_warmup: Duration,
}

impl DucksRun {
    pub fn new<S: ToString>(lading_config: S) -> Result<Self, anyhow::Error> {
        let tempdir = TempDir::new().context("create tempdir")?;

        Ok(Self {
            lading_config_template: lading_config.to_string(),
            tempdir,
            experiment_duration: Duration::from_secs(1),
            experiment_warmup: Duration::from_secs(0),
        })
    }

    pub fn run(self) -> Result<DucksMessage, anyhow::Error> {
        // Build and launch ducks. Cargo's locking is sufficient for this to
        // work correctly when called in parallel. It would be more efficient to
        // only run this a single time though.
        let ducks = build_ducks()?;

        // Every ducks-sheepdog pair is connected by a unique socket file
        let ducks_comm_file = self.tempdir.join("ducks_socket");
        let ducks_comm = UnixListener::bind(&ducks_comm_file).context("bind to ducks_socket")?;

        let mut ducks = Command::new(ducks)
            .stdout(Stdio::piped())
            .env("RUST_LOG", "info") // switch stdout to inherit to have these messages printed directly to your terminal
            .arg(ducks_comm_file.to_str().unwrap())
            .spawn()
            .context("launch ducks")?;

        let mut ducks_comm = ducks_comm
            .incoming()
            .next()
            .unwrap()
            .context("wait for ducks connection")?;

        // TODO: this is terrible. Improve it.
        let mut buf = [0; 36];
        let read = ducks_comm.read(&mut buf).unwrap();
        let buf = String::from_utf8(buf[0..read].to_vec()).unwrap();
        println!("buf: {}", buf);
        let ducks_report = serde_json::from_str(&buf)?;
        let port = if let DucksMessage::OpenPort(port) = ducks_report {
            port
        } else {
            panic!("expected open port")
        };

        let lading_config_file = self.tempdir.join("lading.yaml");
        let mut file =
            std::fs::File::create(&lading_config_file).context("create lading config file")?;
        let lading_config = self
            .lading_config_template
            .replace("{{port_number}}", &port.to_string());
        file.write_all(lading_config.as_bytes())
            .context("write lading config")?;

        let lading = build_lading()?;

        // Run lading in PID mode
        let captures_file = self.tempdir.join("captures");
        let mut lading = Command::new(lading)
            .stdout(Stdio::piped())
            .env("RUST_LOG", "warn") // switch stdout to inherit to have these messages printed directly to your terminal
            .arg("--target-pid")
            .arg(ducks.id().to_string())
            .arg("--config-path")
            .arg(lading_config_file.to_str().unwrap())
            .arg("--experiment-duration-seconds")
            .arg(self.experiment_duration.as_secs().to_string())
            .arg("--warmup-duration-seconds")
            .arg(self.experiment_warmup.as_secs().to_string())
            .arg("--capture-path")
            .arg(captures_file.to_str().unwrap())
            .spawn()
            .unwrap();
        println!("lading running");

        // Have lading push some load. It will exit on its own.
        let _status = lading.wait();

        println!("lading shutdown");
        // Ask ducks to shutdown
        ducks_comm.write_all(b"pls shutdown\n").unwrap();

        println!("shutdown sent");
        // Have ducks report on the load
        let mut buf = Vec::new();
        ducks_comm.read_to_end(&mut buf).unwrap();

        println!("lading done");
        ducks.wait().unwrap();

        let buf = String::from_utf8(buf).unwrap();
        println!("buf: {}", buf);

        // todo: report captures file / provide some utilities for asserting against it

        let ducks_report = serde_json::from_str(&buf)?;
        Ok(ducks_report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_test() -> Result<(), anyhow::Error> {
        let simple_run = DucksRun::new(
            r#"
generator:
  http:
    seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
      59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
    headers: {}
    target_uri: "http://localhost:{{port_number}}/"
    bytes_per_second: "500 Mb"
    parallel_connections: 10
    method:
      post:
        maximum_prebuild_cache_size_bytes: "256 Mb"
        variant: "apache_common"
        "#,
        )?;

        let result = simple_run.run()?;

        if let DucksMessage::MetricsReport(reqs) = result {
            assert!(reqs.request_count > 10);
        } else {
            panic!("unexpected ducks message");
        }
        Ok(())
    }

    #[test]
    fn simple_test2() -> Result<(), anyhow::Error> {
        let simple_run = DucksRun::new(
            r#"
generator:
  http:
    seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
      59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
    headers: {}
    target_uri: "http://localhost:{{port_number}}/"
    bytes_per_second: "500 Mb"
    parallel_connections: 10
    method:
      post:
        maximum_prebuild_cache_size_bytes: "256 Mb"
        variant: "apache_common"
        "#,
        )?;

        let result = simple_run.run()?;

        if let DucksMessage::MetricsReport(reqs) = result {
            assert!(reqs.request_count > 10);
            println!("{:?}", reqs)
        } else {
            panic!("unexpected ducks message");
        }
        Ok(())
    }
}
