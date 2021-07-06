use crate::payload::{Error, Serialize};
use arbitrary::{self, Arbitrary};
use std::io::Write;

#[derive(Arbitrary, Debug, serde::Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "PascalCase")]
enum Member {
    // Many keys are missing, those with complex internal structure especially.
    SlowTask {
        severity: u8,
        m_clocks: u32,
    },
    TransactionMetrics {
        severity: u8,
        time: f32,
        id: u64,
        elapsed: f32,
        internal: u8,
        mean_latency: u16,
        median_latency: u16,
        max_latency: u16,
        latency90: u32,
        latency98: u32,
        mean_row_read_latency: f32,
        median_row_read_latency: f32,
        max_row_read_latency: f32,
        mean_commit_latency: u32,
        median_commit_latency: u32,
        max_commit_latency: u32,
        mean_grv_latency: f32,
        max_grv_latency: f32,
        mean_mutations_per_commit: u32,
        median_mutations_per_commit: u32,
        max_mutations_per_commit: u32,
        mean_bytes_per_commit: u32,
        median_bytes_per_commit: u32,
        max_bytes_per_commit: u32,
    },
}

#[derive(Arbitrary, Debug)]
pub struct FoundationDb {
    members: Vec<Member>,
}

impl Serialize for FoundationDb {
    fn to_bytes<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        for member in &self.members {
            serde_json::to_writer(&mut *writer, member)?;
            writeln!(writer)?;
        }
        Ok(())
    }
}
