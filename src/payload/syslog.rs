use std::{io::Write, time::SystemTime};

use arbitrary::{size_hint, Unstructured};
use rand::Rng;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::payload::{Error, Serialize};

#[derive(Debug, Default, Clone, Copy)]
#[allow(clippy::module_name_repetitions)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct Syslog5424 {}

const HOSTNAMES: [&str; 4] = [
    "troutwine.us",
    "vector.dev",
    "skullmountain.us",
    "zombo.com",
];
const APP_NAMES: [&str; 4] = ["vector", "cernan", "lading", "execsnoop"];

#[derive(serde::Serialize)]
struct Message {
    eccentricity: u32,
    semimajor_axis: u32,
    inclination: f32,
    longitude_of_ascending_node: u32,
    periapsis: u64,
    true_anomaly: u8,
}

impl<'a> arbitrary::Arbitrary<'a> for Message {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Message {
            eccentricity: u.arbitrary::<u32>()?,
            semimajor_axis: u.arbitrary::<u32>()?,
            inclination: u.arbitrary::<f32>()?,
            longitude_of_ascending_node: u.arbitrary::<u32>()?,
            periapsis: u.arbitrary::<u64>()?,
            true_anomaly: u.arbitrary::<u8>()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <u32 as arbitrary::Arbitrary>::size_hint(depth),
                <u32 as arbitrary::Arbitrary>::size_hint(depth),
                <f32 as arbitrary::Arbitrary>::size_hint(depth),
                <u32 as arbitrary::Arbitrary>::size_hint(depth),
                <u64 as arbitrary::Arbitrary>::size_hint(depth),
                <u8 as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
    }
}

struct Member {
    priority: u8,       // 0 - 191
    syslog_version: u8, // 1 - 3
    timestamp: String,  // seconds format in millis
    hostname: String,   // name.tld
    app_name: String,   // shortish string
    procid: u16,        // 100 - 9999
    msgid: u16,         // 1 - 999
    message: String,    // shortish structured string
}

fn to_rfc3339<T>(dt: T) -> String
where
    T: Into<OffsetDateTime>,
{
    dt.into().format(&Rfc3339).unwrap()
}

impl<'a> arbitrary::Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let priority = u.arbitrary::<u8>()? % 191;
        let syslog_version = (u.arbitrary::<u8>()? % 3) + 1;
        let timestamp = to_rfc3339(SystemTime::now());
        let hostname_idx = u.arbitrary::<usize>()? % HOSTNAMES.len();
        let app_name_idx = u.arbitrary::<usize>()? % APP_NAMES.len();
        let procid = (u.arbitrary::<u16>()? % 9899) + 101;
        let msgid = (u.arbitrary::<u16>()? % 999) + 1;
        let message = u.arbitrary::<Message>()?;

        Ok(Member {
            priority,
            syslog_version,
            timestamp,
            hostname: HOSTNAMES[hostname_idx as usize].to_string(),
            app_name: APP_NAMES[app_name_idx as usize].to_string(),
            procid,
            msgid,
            message: serde_json::to_string(&message).unwrap(),
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <u8 as arbitrary::Arbitrary>::size_hint(depth),
                <u8 as arbitrary::Arbitrary>::size_hint(depth),
                (16, Some(64)), // timestamp
                (8, Some(16)),  // hostnames
                (4, Some(20)),  // app names
                <u16 as arbitrary::Arbitrary>::size_hint(depth),
                <u16 as arbitrary::Arbitrary>::size_hint(depth),
                (128, Some(512)), // message
            ])
        })
    }
}

impl Member {
    fn into_string(self) -> String {
        format!(
            "<{}>{} {} {} {} {} ID{} - {}",
            self.priority,
            self.syslog_version,
            self.timestamp,
            self.hostname,
            self.app_name,
            self.procid,
            self.msgid,
            self.message
        )
    }
}

impl Serialize for Syslog5424 {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if max_bytes < 2 {
            // 'empty' payload  is []
            return Ok(());
        }

        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let unstructured = Unstructured::new(&entropy);

        let members = <Vec<Member> as arbitrary::Arbitrary>::arbitrary_take_rest(unstructured)?;
        let encoded = members.into_iter().map(Member::into_string);

        let mut written_bytes = 0;
        for line in encoded {
            if line.len() + 1 + written_bytes > max_bytes {
                break;
            }

            writeln!(writer, "{}", line)?;

            written_bytes += 1; // newline
            written_bytes += line.len();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::payload::{Serialize, Syslog5424};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let syslog = Syslog5424::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            syslog.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }
}
