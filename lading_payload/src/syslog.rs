//! Syslog payload.

use std::{io::Write, time::SystemTime};

use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::Error;

#[derive(Debug, Default, Clone, Copy)]
#[allow(clippy::module_name_repetitions)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
/// Syslog 5424 payload
pub struct Syslog5424 {}

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

impl Distribution<Message> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Message
    where
        R: Rng + ?Sized,
    {
        Message {
            eccentricity: rng.random(),
            semimajor_axis: rng.random(),
            inclination: rng.random(),
            longitude_of_ascending_node: rng.random(),
            periapsis: rng.random(),
            true_anomaly: rng.random(),
        }
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

impl Distribution<Member> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        Member {
            priority: rng.random_range(0..=191),
            syslog_version: rng.random_range(1..=3),
            timestamp: to_rfc3339(SystemTime::now()),
            hostname: (*HOSTNAMES.choose(rng).expect("failed to choose hostnanme")).to_string(),
            app_name: (*APP_NAMES.choose(rng).expect("failed to choose app name")).to_string(),
            procid: rng.random_range(100..=9999),
            msgid: rng.random_range(1..=999),
            message: serde_json::to_string(&rng.random::<Message>()).expect("failed to serialize"),
        }
    }
}

fn to_rfc3339<T>(dt: T) -> String
where
    T: Into<OffsetDateTime>,
{
    dt.into().format(&Rfc3339).expect("failed to format")
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

impl crate::Serialize for Syslog5424 {
    fn to_bytes<W, R>(&mut self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if max_bytes < 2 {
            // 'empty' payload  is []
            return Ok(());
        }

        let mut written_bytes = 0;
        for member in rng.sample_iter::<Member, StandardUniform>(StandardUniform) {
            let encoded = member.into_string();

            if encoded.len() + 1 + written_bytes > max_bytes {
                break;
            }

            writeln!(writer, "{encoded}")?;

            written_bytes += 1; // newline
            written_bytes += encoded.len();
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use crate::{Serialize, Syslog5424};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let mut syslog = Syslog5424::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            syslog.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }
}
