use std::{io::Write, time::SystemTime};

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};
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

impl Distribution<Message> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Message
    where
        R: Rng + ?Sized,
    {
        Message {
            eccentricity: rng.gen(),
            semimajor_axis: rng.gen(),
            inclination: rng.gen(),
            longitude_of_ascending_node: rng.gen(),
            periapsis: rng.gen(),
            true_anomaly: rng.gen(),
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

impl Distribution<Member> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        Member {
            priority: rng.gen_range(0..=191),
            syslog_version: rng.gen_range(1..=3),
            timestamp: to_rfc3339(SystemTime::now()),
            hostname: HOSTNAMES.choose(rng).unwrap().to_string(),
            app_name: APP_NAMES.choose(rng).unwrap().to_string(),
            procid: rng.gen_range(100..=9999),
            msgid: rng.gen_range(1..=999),
            message: serde_json::to_string(&rng.gen::<Message>()).unwrap(),
        }
    }
}

fn to_rfc3339<T>(dt: T) -> String
where
    T: Into<OffsetDateTime>,
{
    dt.into().format(&Rfc3339).unwrap()
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
    fn to_bytes<W, R>(&self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if max_bytes < 2 {
            // 'empty' payload  is []
            return Ok(());
        }

        let mut written_bytes = 0;
        for member in rng.sample_iter::<Member, Standard>(Standard) {
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
