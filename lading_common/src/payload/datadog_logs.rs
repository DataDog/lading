use crate::payload::common::AsciiStr;
use crate::payload::{Error, Serialize};
use arbitrary::{size_hint, Unstructured};
use rand::Rng;
use std::io::Write;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Notice,
    Info,
    Warning,
}

impl<'a> arbitrary::Arbitrary<'a> for Status {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice = u.arbitrary::<u8>()?;
        let res = match choice % 3 {
            0 => Status::Notice,
            1 => Status::Info,
            2 => Status::Warning,
            _ => unreachable!(),
        };
        Ok(res)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Hostname {
    Alpha,
    Beta,
    Gamma,
    Localhost,
}

impl<'a> arbitrary::Arbitrary<'a> for Hostname {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice = u.arbitrary::<u8>()?;
        let res = match choice % 4 {
            0 => Hostname::Alpha,
            1 => Hostname::Beta,
            2 => Hostname::Gamma,
            3 => Hostname::Localhost,
            _ => unreachable!(),
        };
        Ok(res)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Service {
    Vector,
    Lading,
    Cernan,
}

impl<'a> arbitrary::Arbitrary<'a> for Service {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice = u.arbitrary::<u8>()?;
        let res = match choice % 3 {
            0 => Service::Vector,
            1 => Service::Lading,
            2 => Service::Cernan,
            _ => unreachable!(),
        };
        Ok(res)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    Bergman,
    Keaton,
    Kurosawa,
    Lynch,
    Waters,
    Tarkovsky,
}

impl<'a> arbitrary::Arbitrary<'a> for Source {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice = u.arbitrary::<u8>()?;
        let res = match choice % 6 {
            0 => Source::Bergman,
            1 => Source::Keaton,
            2 => Source::Kurosawa,
            3 => Source::Lynch,
            4 => Source::Waters,
            5 => Source::Tarkovsky,
            _ => unreachable!(),
        };
        Ok(res)
    }
}

const TAG_OPTIONS: [&'static str; 4] = ["", "env:prod", "env:dev", "env:prod,version:1.1"];

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Structured {
    proportional: u32,
    integral: u64,
    derivative: f64,
    vegetable: i16,
    mineral: String,
}

impl<'a> arbitrary::Arbitrary<'a> for Structured {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let ascii_str = u.arbitrary::<AsciiStr>()?;

        Ok(Structured {
            mineral: ascii_str.as_str().to_string(),
            proportional: u.arbitrary::<u32>()?,
            integral: u.arbitrary::<u64>()?,
            derivative: u.arbitrary::<f64>()?,
            vegetable: u.arbitrary::<i16>()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <u32 as arbitrary::Arbitrary>::size_hint(depth),
                <u64 as arbitrary::Arbitrary>::size_hint(depth),
                <f64 as arbitrary::Arbitrary>::size_hint(depth),
                <i16 as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum Message {
    Unstructured(String),
    Structured(String),
}

impl<'a> arbitrary::Arbitrary<'a> for Message {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let inner = if u.arbitrary::<bool>()? {
            let ascii_str = u.arbitrary::<AsciiStr>()?;
            Message::Unstructured(ascii_str.as_str().to_string())
        } else {
            let structured = u.arbitrary::<Structured>()?;
            Message::Structured(serde_json::to_string(&structured).unwrap())
        };

        Ok(inner)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and(
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <Structured as arbitrary::Arbitrary>::size_hint(depth),
            )
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
struct Member {
    /// The message is a short ascii string, without newlines for now
    pub message: Message,
    /// The message status
    pub status: Status,
    /// The timestamp is a simple integer value since epoch, presumably
    pub timestamp: u32,
    /// The hostname that sent the logs
    pub hostname: Hostname,
    /// The service that sent the logs
    pub service: Service,
    /// The ultimate source of the logs
    pub ddsource: Source,
    /// Comma-separate list of tags
    pub ddtags: String,
}

impl<'a> arbitrary::Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let message = u.arbitrary::<Message>()?;
        let status = u.arbitrary::<Status>()?;
        let timestamp = u.arbitrary::<u32>()?;
        let hostname = u.arbitrary::<Hostname>()?;
        let service = u.arbitrary::<Service>()?;
        let source = u.arbitrary::<Source>()?;
        let tag_idx = u.arbitrary::<usize>()? % TAG_OPTIONS.len();

        Ok(Member {
            message,
            status,
            timestamp,
            hostname,
            service,
            ddsource: source,
            ddtags: TAG_OPTIONS[tag_idx as usize].to_string(),
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
