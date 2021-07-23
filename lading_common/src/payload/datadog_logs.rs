use crate::payload::common::AsciiStr;
use crate::payload::{Error, Serialize};
use arbitrary::{size_hint, Unstructured};
use rand::Rng;
use std::io::Write;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
struct Member {
    /// The message is a short ascii string, without newlines for now
    pub message: String,
    /// The message status
    pub status: String,
    /// The timestamp is a simple integer value since epoch, presumably
    pub timestamp: u32,
    /// The hostname that sent the logs
    pub hostname: String,
    /// The service that sent the logs
    pub service: String,
    /// The ultimate source of the logs
    pub ddsource: String,
    /// Comma-separate list of tags
    pub ddtags: String,
}

impl<'a> arbitrary::Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let ascii_str = u.arbitrary::<AsciiStr>()?;
        let status = u.arbitrary::<AsciiStr>()?;
        let timestamp = u.arbitrary::<u32>()?;
        let hostname = u.arbitrary::<AsciiStr>()?;
        let service = u.arbitrary::<AsciiStr>()?;
        let source = u.arbitrary::<AsciiStr>()?;
        let tags = u.arbitrary::<AsciiStr>()?;

        Ok(Member {
            message: ascii_str.as_str().to_string(),
            status: status.as_str().to_string(),
            timestamp,
            hostname: hostname.as_str().to_string(),
            service: service.as_str().to_string(),
            ddsource: source.as_str().to_string(),
            ddtags: tags.as_str().to_string(),
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and(
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <u32 as arbitrary::Arbitrary>::size_hint(depth),
            )
        })
    }
}

#[derive(Debug, Default)]
pub struct DatadogLog {}

impl Serialize for DatadogLog {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        if max_bytes < 2 {
            // 'empty' payload  is []
            return Ok(());
        }

        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let unstructured = Unstructured::new(&entropy);

        let mut members = <Vec<Member> as arbitrary::Arbitrary>::arbitrary_take_rest(unstructured)?;
        loop {
            let encoding = serde_json::to_string(&members)?;
            if encoding.len() < max_bytes {
                let encoding = serde_json::to_string(&members)?;
                write!(writer, "{}", encoding)?;
                break;
            } else {
                members.pop();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use quickcheck::{QuickCheck, TestResult};
    use rand::rngs::SmallRng;
    use rand::SeedableRng;

    use super::Member;
    use crate::payload::{DatadogLog, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    #[test]
    fn payload_not_exceed_max_bytes() {
        fn inner(seed: u64, max_bytes: u16) -> TestResult {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let ddlogs = DatadogLog::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1_000)
            .quickcheck(inner as fn(u64, u16) -> TestResult);
    }

    // We want to know that every payload produced by this type actually
    // deserializes as json, is not truncated etc.
    #[test]
    fn every_payload_deserializes() {
        fn inner(seed: u64, max_bytes: u16) -> TestResult {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let ddlogs = DatadogLog::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            let payload = std::str::from_utf8(&bytes).unwrap();
            for msg in payload.lines() {
                let _members: Vec<Member> = serde_json::from_str(msg).unwrap();
            }

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1_000_000)
            .quickcheck(inner as fn(u64, u16) -> TestResult);
    }
}
