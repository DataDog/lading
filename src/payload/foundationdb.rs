use crate::payload::{Error, Serialize};
use arbitrary::{self, Arbitrary};
use serde::Serializer;
use std::io::Write;

#[derive(Arbitrary, Debug)]
struct StrF32 {
    inner: f32,
}

impl serde::ser::Serialize for StrF32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self.inner))
    }
}

#[derive(Arbitrary, Debug)]
struct StrU8 {
    inner: u8,
}

impl serde::ser::Serialize for StrU8 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self.inner))
    }
}

#[derive(Arbitrary, Debug)]
struct StrU16 {
    inner: u16,
}

impl serde::ser::Serialize for StrU16 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self.inner))
    }
}

#[derive(Arbitrary, Debug)]
struct StrU32 {
    inner: u32,
}

impl serde::ser::Serialize for StrU32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self.inner))
    }
}

#[derive(Arbitrary, Debug)]
struct StrU64 {
    inner: u64,
}

impl serde::ser::Serialize for StrU64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self.inner))
    }
}

#[derive(Arbitrary, Debug, serde::Serialize)]
#[serde(tag = "Type")]
enum Member {
    // Many keys are missing, those with complex internal structure especially.
    #[serde(rename_all = "PascalCase")]
    SlowTask { severity: StrU8, m_clocks: StrU32 },
    #[serde(rename_all = "PascalCase")]
    TransactionMetrics {
        severity: StrU8,
        time: StrF32,
        #[serde(rename = "ID")]
        id: StrU64,
        elapsed: StrF32,
        internal: StrU8,
        mean_latency: StrU16,
        median_latency: StrU16,
        max_latency: StrU16,
        latency90: StrU32,
        latency98: StrU32,
        mean_row_read_latency: StrF32,
        median_row_read_latency: StrF32,
        max_row_read_latency: StrF32,
        mean_commit_latency: StrU32,
        median_commit_latency: StrU32,
        max_commit_latency: StrU32,
        #[serde(rename = "MeanGRVLatency")]
        mean_grv_latency: StrF32,
        #[serde(rename = "MaxGRVLatency")]
        max_grv_latency: StrF32,
        mean_mutations_per_commit: StrU32,
        median_mutations_per_commit: StrU32,
        max_mutations_per_commit: StrU32,
        mean_bytes_per_commit: StrU32,
        median_bytes_per_commit: StrU32,
        max_bytes_per_commit: StrU32,
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
