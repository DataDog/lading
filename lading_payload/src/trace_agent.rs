//! Trace-agent payload.

use std::io::Write;

use rand::{seq::SliceRandom, Rng};
use rmp_serde::Serializer;
use rustc_hash::FxHashMap;

use crate::{common::strings, Error, Generator};
use serde::Serialize;

const SERVICES: [&str; 7] = [
    "tablet",
    "phone",
    "phone2",
    "laptop",
    "desktop",
    "monitor",
    "bigger-monitor",
];
const TAG_NAMES: [&str; 8] = [
    "one", "two", "three", "four", "five", "six", "seven", "eight",
];
const SERVICE_KIND: [&str; 4] = ["web", "db", "lambda", "cicd"];

// Manual implementation of [this protobuf](https://github.com/DataDog/datadog-agent/blob/main/pkg/trace/pb/span.proto).
//
// ```
// syntax = "proto3";
//
// package pb;
//
// import "github.com/gogo/protobuf/gogoproto/gogo.proto";
//
// message Span {
//     // service is the name of the service with which this span is associated.
//     string service = 1 [(gogoproto.jsontag) = "service", (gogoproto.moretags) = "msg:\"service\""];
//     // name is the operation name of this span.
//     string name = 2 [(gogoproto.jsontag) = "name", (gogoproto.moretags) = "msg:\"name\""];
//     // resource is the resource name of this span, also sometimes called the endpoint (for web spans).
//     string resource = 3 [(gogoproto.jsontag) = "resource", (gogoproto.moretags) = "msg:\"resource\""];
//     // traceID is the ID of the trace to which this span belongs.
//     uint64 traceID = 4 [(gogoproto.jsontag) = "trace_id", (gogoproto.moretags) = "msg:\"trace_id\""];
//     // spanID is the ID of this span.
//     uint64 spanID = 5 [(gogoproto.jsontag) = "span_id", (gogoproto.moretags) = "msg:\"span_id\""];
//     // parentID is the ID of this span's parent, or zero if this span has no parent.
//     uint64 parentID = 6 [(gogoproto.jsontag) = "parent_id", (gogoproto.moretags) = "msg:\"parent_id\""];
//     // start is the number of nanoseconds between the Unix epoch and the beginning of this span.
//     int64 start = 7 [(gogoproto.jsontag) = "start", (gogoproto.moretags) = "msg:\"start\""];
//     // duration is the time length of this span in nanoseconds.
//     int64 duration = 8 [(gogoproto.jsontag) = "duration", (gogoproto.moretags) = "msg:\"duration\""];
//     // error is 1 if there is an error associated with this span, or 0 if there is not.
//     int32 error = 9 [(gogoproto.jsontag) = "error", (gogoproto.moretags) = "msg:\"error\""];
//     // meta is a mapping from tag name to tag value for string-valued tags.
//     map<string, string> meta = 10 [(gogoproto.jsontag) = "meta", (gogoproto.moretags) = "msg:\"meta\""];
//     // metrics is a mapping from tag name to tag value for numeric-valued tags.
//     map<string, double> metrics = 11 [(gogoproto.jsontag) = "metrics", (gogoproto.moretags) = "msg:\"metrics\""];
//     // type is the type of the service with which this span is associated.  Example values: web, db, lambda.
//     string type = 12 [(gogoproto.jsontag) = "type", (gogoproto.moretags) = "msg:\"type\""];
//     // meta_struct is a registry of structured "other" data used by, e.g., AppSec.
//     map<string, bytes> meta_struct = 13 [(gogoproto.jsontag) = "meta_struct,omitempty", (gogoproto.moretags) = "msg:\"meta_struct\""];
// }
// ```
//
// Note that this protobuf carries go-isms in it, documented
// [here](https://github.com/gogo/protobuf/blob/master/extensions.md#more-serialization-formats),
// although awkwardly this shunts to a [Google
// Groups](https://groups.google.com/g/gogoprotobuf/c/xmFnqAS6MIc) thread for
// further elaboration. I _think_ this is the equivalent of a serde rename to
// camel_case for all the field names and then `meta_struct`, the `jsontag`. If
// I understand correctly the `moretags` also implies that the field names are
// camel_case in msgpack.

/// `TraceAgent` span
#[derive(serde::Serialize)]
#[allow(clippy::struct_field_names)]
pub(crate) struct Span<'a> {
    /// service is the name of the service with which this span is associated.
    service: &'a str,
    /// name is the operation name of this span.
    name: &'a str,
    /// resource is the resource name of this span, also sometimes called the endpoint (for web spans).
    resource: &'a str,
    /// traceID is the ID of the trace to which this span belongs.
    trace_id: u64,
    /// spanID is the ID of this span.
    span_id: u64,
    /// parentID is the ID of this span's parent, or zero if this span has no parent.
    parent_id: u64,
    /// start is the number of nanoseconds between the Unix epoch and the beginning of this span.
    start: i64,
    /// duration is the time length of this span in nanoseconds.
    duration: i64,
    /// error is 1 if there is an error associated with this span, or 0 if there is not.
    error: i32,
    /// meta is a mapping from tag name to tag value for string-valued tags.
    meta: FxHashMap<&'a str, &'a str>,
    /// metrics is a mapping from tag name to tag value for numeric-valued tags.
    metrics: FxHashMap<&'a str, f64>,
    /// type is the type of the service with which this span is associated.  Example values: web, db, lambda.
    #[serde(alias = "type")]
    kind: &'a str,
    /// `meta_struct` is a registry of structured "other" data used by, e.g., `AppSec`.
    meta_struct: FxHashMap<&'a str, Vec<u8>>,
}

#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
/// Encoding options for the trace-agent
pub enum Encoding {
    /// Encode `TraceAgent` payload in JSON format
    Json,
    /// Encode `TraceAgent` payload in `MsgPack` format
    #[default]
    MsgPack,
}

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
/// Trace Agent payload
pub struct TraceAgent {
    encoding: Encoding,
    str_pool: strings::Pool,
}

impl TraceAgent {
    /// JSON encoding
    #[must_use]
    pub fn json<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            encoding: Encoding::Json,
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }

    /// `MsgPack` encoding
    #[must_use]
    pub fn msg_pack<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            encoding: Encoding::MsgPack,
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for TraceAgent {
    type Output = Span<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let total_metrics = rng.gen_range(0..6);
        let mut metrics: FxHashMap<&'static str, f64> = FxHashMap::default();
        for _ in 0..total_metrics {}
        for k in TAG_NAMES.choose_multiple(rng, total_metrics) {
            metrics.insert(*k, rng.gen());
        }

        let name = self
            .str_pool
            .of_size_range(&mut rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;
        let resource = self
            .str_pool
            .of_size_range(&mut rng, 1_u8..8)
            .ok_or(Error::StringGenerate)?;

        Ok(Span {
            service: SERVICES.choose(rng).expect("failed to choose service"),
            name,
            resource,
            trace_id: rng.gen(),
            span_id: rng.gen(),
            parent_id: rng.gen(),
            start: rng.gen(),
            duration: rng.gen(),
            error: rng.gen_range(0..=1),
            meta: FxHashMap::default(),
            metrics,
            kind: SERVICE_KIND
                .choose(rng)
                .expect("failed to choose service kind"),
            meta_struct: FxHashMap::default(),
        })
    }
}

impl crate::Serialize for TraceAgent {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // We will arbitrarily generate Member instances and then serialize. If
        // this is below `max_bytes` we'll add more until we're over. Once we
        // are we'll start removing instances until we're back below the limit.
        //
        // NOTE we might consider a method that allows us to construct a tree of
        // Spans as an improvement in the future, one in which parent_ids are
        // obeyed, as an example. We could then have a 'shrink' or 'expand'
        // method on that tree to avoid this loop.
        let mut members: Vec<Vec<Span>> = vec![];
        let mut remaining: u16 = 10_000;
        while remaining > 0 {
            let total = rng.gen_range(0..=remaining);
            let spans: Vec<Span> = (0..total)
                .map(|_| self.generate(&mut rng).expect("Generate failed"))
                .collect();
            members.push(spans);
            remaining = remaining.saturating_sub(total);
        }

        // Search for too many Member instances.
        loop {
            let encoding = match self.encoding {
                Encoding::Json => serde_json::to_vec(&members[0..])?,
                Encoding::MsgPack => {
                    let mut buf = Vec::with_capacity(max_bytes);
                    members[0..].serialize(&mut Serializer::new(&mut buf))?;
                    buf
                }
            };
            if encoding.len() > max_bytes {
                break;
            }

            members.push(
                (0..5_000)
                    .map(|_| self.generate(&mut rng).expect("Generate failed"))
                    .collect(),
            );
        }

        // Search for an encoding that's just right.
        let mut high = members.len();
        loop {
            let encoding = match self.encoding {
                Encoding::Json => serde_json::to_vec(&members[0..high])?,
                Encoding::MsgPack => {
                    let mut buf = Vec::with_capacity(max_bytes);
                    members[0..high].serialize(&mut Serializer::new(&mut buf))?;
                    buf
                }
            };
            // NOTE because the type of Vec<Vec<Span>> this shrink isn't as
            // efficient as it could be. We want to shrink the tree present
            // here. This algorithm _does_ work perfectly if the tree is a
            // straight pipe.
            if encoding.len() > max_bytes {
                high /= 2;
            } else {
                writer.write_all(&encoding)?;
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::{Serialize, TraceAgent};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes_json(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let trace_agent = TraceAgent::json(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            trace_agent.to_bytes(rng, max_bytes, &mut bytes)?;
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes)?
            );
        }
    }

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes_msg_pack(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let trace_agent = TraceAgent::json(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            trace_agent.to_bytes(rng, max_bytes, &mut bytes)?;
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes)?
            );
        }
    }
}
