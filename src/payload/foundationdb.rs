use crate::payload::{Error, Serialize};
use arbitrary::Unstructured;
use arbitrary::{self, Arbitrary};
use rand::{thread_rng, RngCore};
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

#[derive(Debug, Default)]
pub struct FoundationDb {}

impl Serialize for FoundationDb {
    fn to_bytes<W>(&self, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        let mut rng = thread_rng();
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        let mut bytes_remaining = max_bytes;
        while let Ok(member) = unstructured.arbitrary::<Member>() {
            let encoding = serde_json::to_string(&member)?;
            let line_length = encoding.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writeln!(writer, "{}", encoding)?;
                    bytes_remaining = remainder
                }
                None => break,
            }
        }
        Ok(())
    }
}
