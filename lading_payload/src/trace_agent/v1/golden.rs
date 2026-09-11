//! Golden tests for the v1.0 wire format, validated against a real tracer payload.
//!
//! The fixtures in `testdata/golden_captures.json` are raw `/v1.0/traces` request bodies
//! captured from `dd-trace-go` v2.11.0-dev.1 (via the `apm-v1-trace-smoke` traced application):
//! three services with nested parent/child spans and error spans, six chunks per payload, and a
//! second capture also carrying a span link and a span event. The hand-written byte-literal
//! tests elsewhere in this module only prove the encoder agrees with itself; these tests prove
//! it agrees with the reference implementations.
//!
//! The decoder below is transcribed from the reference encoder and decoder, not from this
//! module's encoder: field identifiers, the `[key, type, value]` attribute layout, the
//! `AnyValue` type discriminants (string 1, bool 2, double 3, int 4), and the streaming string
//! table (index 0 pre-seeded as the empty string, indices assigned in first-write order) follow
//! `dd-trace-go` (`ddtrace/tracer/payload_v1.go`) and the receiving decoder in Saluki
//! (`saluki-components/src/decoders/datadog`). The generator's encoder is only correct if bytes
//! it produces decode, through this independent path, to the same payload the real tracer sent.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::{AttributeValue, Span, SpanEvent, SpanLink, TraceChunk, TracerPayload};

/// The captured fixtures, stored as base64 in JSON following the sibling decoder's testdata
/// convention (saluki `decoders/datadog/testdata/v1_decoder_cases.json`).
const GOLDEN_CAPTURES_JSON: &str = include_str!("testdata/golden_captures.json");

#[derive(serde::Deserialize)]
struct GoldenCaptures {
    cases: Vec<GoldenCapture>,
}

#[derive(serde::Deserialize)]
struct GoldenCapture {
    name: String,
    payload_base64: String,
}

/// Returns the captured bytes for the named case.
fn golden_payload(name: &str) -> Vec<u8> {
    let captures: GoldenCaptures =
        serde_json::from_str(GOLDEN_CAPTURES_JSON).expect("golden captures should parse");
    let case = captures
        .cases
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("golden capture '{name}' should exist"));
    decode_base64(&case.payload_base64)
}

/// Decodes standard, padded base64.
///
/// Test-only, and self-checking: the golden assertions compare the decoded bytes against
/// hand-verified values, so a decoding bug fails the tests loudly rather than passing silently.
fn decode_base64(encoded: &str) -> Vec<u8> {
    fn value(byte: u8) -> u8 {
        match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("invalid base64 byte {byte:#x}"),
        }
    }

    let bytes: Vec<u8> = encoded
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    assert!(
        !bytes.is_empty() && bytes.len().is_multiple_of(4),
        "invalid base64 length {}",
        bytes.len()
    );

    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        // Padding appears only in the final group: one `=` drops the third byte, two drop the
        // third and fourth.
        let pads = match chunk {
            [_, _, _, b'='] if chunk[2] == b'=' => 2,
            [_, _, _, b'='] => 1,
            _ => 0,
        };
        let digits: [u8; 4] = chunk
            .iter()
            .map(|&b| if b == b'=' { 0 } else { value(b) })
            .collect::<Vec<_>>()
            .try_into()
            .expect("four digits");
        let group = (u32::from(digits[0]) << 18)
            | (u32::from(digits[1]) << 12)
            | (u32::from(digits[2]) << 6)
            | u32::from(digits[3]);
        // Four six-bit digits form 24 bits; the shifts below select whole bytes, so the casts
        // only drop zero high bits.
        #[allow(clippy::cast_possible_truncation)]
        {
            out.push((group >> 16) as u8);
            if pads < 2 {
                out.push((group >> 8) as u8);
            }
            if pads < 1 {
                out.push(group as u8);
            }
        }
    }
    out
}

/// A schema-aware reader over a v1.0 `MessagePack` payload.
struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
    /// Streaming string table: index 0 is the pre-seeded empty string, and each inline string
    /// takes the next index in write order.
    strings: FxHashMap<u64, String>,
    next_string_index: u64,
}

impl<'a> Decoder<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            strings: FxHashMap::from_iter([(0, String::new())]),
            next_string_index: 1,
        }
    }

    fn byte(&mut self) -> Result<u8, String> {
        let byte = *self
            .data
            .get(self.pos)
            .ok_or_else(|| "unexpected end of payload".to_string())?;
        self.pos += 1;
        Ok(byte)
    }

    fn peek(&self) -> Result<u8, String> {
        self.data
            .get(self.pos)
            .copied()
            .ok_or_else(|| "unexpected end of payload".to_string())
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| "payload length overflow".to_string())?;
        let slice = self
            .data
            .get(self.pos..end)
            .ok_or_else(|| "unexpected end of payload".to_string())?;
        self.pos = end;
        Ok(slice)
    }

    /// Reads an unsigned integer in any of its `MessagePack` encodings. The reference encoder
    /// routes some numeric fields through its signed path, so positive values may also carry
    /// signed markers.
    fn uint(&mut self) -> Result<u64, String> {
        let marker = self.byte()?;
        match marker {
            0x00..=0x7F => Ok(u64::from(marker)),
            0xCC => Ok(u64::from(self.byte()?)),
            0xCD => Ok(u64::from(u16::from_be_bytes(
                self.take(2)?.try_into().expect("two bytes"),
            ))),
            0xCE => Ok(u64::from(u32::from_be_bytes(
                self.take(4)?.try_into().expect("four bytes"),
            ))),
            0xCF => Ok(u64::from_be_bytes(
                self.take(8)?.try_into().expect("eight bytes"),
            )),
            0xD0..=0xD3 => {
                let value = self.signed_value(marker)?;
                u64::try_from(value).map_err(|_| format!("negative value in uint field: {value}"))
            }
            _ => Err(format!("not an unsigned integer: 0x{marker:02x}")),
        }
    }

    fn signed_value(&mut self, marker: u8) -> Result<i64, String> {
        match marker {
            0xD0 => Ok(i8::from_be_bytes(self.take(1)?.try_into().expect("one byte")).into()),
            0xD1 => Ok(i16::from_be_bytes(self.take(2)?.try_into().expect("two bytes")).into()),
            0xD2 => Ok(i32::from_be_bytes(self.take(4)?.try_into().expect("four bytes")).into()),
            0xD3 => Ok(i64::from_be_bytes(
                self.take(8)?.try_into().expect("eight bytes"),
            )),
            _ => Err(format!("not a signed integer: 0x{marker:02x}")),
        }
    }

    fn signed(&mut self) -> Result<i64, String> {
        let marker = self.byte()?;
        match marker {
            0x00..=0x7F => Ok(i64::from(marker)),
            0xE0..=0xFF => Ok(i64::from(marker) - 256),
            0xD0..=0xD3 => self.signed_value(marker),
            _ => Err(format!("not a signed integer: 0x{marker:02x}")),
        }
    }

    fn boolean(&mut self) -> Result<bool, String> {
        match self.byte()? {
            0xC2 => Ok(false),
            0xC3 => Ok(true),
            marker => Err(format!("not a boolean: 0x{marker:02x}")),
        }
    }

    fn double(&mut self) -> Result<f64, String> {
        match self.byte()? {
            0xCB => Ok(f64::from_be_bytes(
                self.take(8)?.try_into().expect("eight bytes"),
            )),
            marker => Err(format!("not a double: 0x{marker:02x}")),
        }
    }

    fn inline_str(&mut self) -> Result<String, String> {
        let marker = self.byte()?;
        let len = match marker {
            0xA0..=0xBF => usize::from(marker & 0x1F),
            0xD9 => usize::from(self.byte()?),
            0xDA => usize::from(u16::from_be_bytes(
                self.take(2)?.try_into().expect("two bytes"),
            )),
            0xDB => u32::from_be_bytes(self.take(4)?.try_into().expect("four bytes")) as usize,
            _ => return Err(format!("not a string: 0x{marker:02x}")),
        };
        String::from_utf8(self.take(len)?.to_vec()).map_err(|e| format!("invalid UTF-8: {e}"))
    }

    /// Reads a streaming string: inline on its first appearance, by table index thereafter.
    fn string(&mut self) -> Result<String, String> {
        let marker = self.peek();
        if matches!(marker, Ok(0xA0..=0xBF | 0xD9 | 0xDA | 0xDB)) {
            let s = self.inline_str()?;
            let index = self.next_string_index;
            self.strings.insert(index, s.clone());
            self.next_string_index += 1;
            Ok(s)
        } else {
            let index = self.uint()?;
            self.strings
                .get(&index)
                .cloned()
                .ok_or_else(|| format!("reference to unassigned string index {index}"))
        }
    }

    fn bin(&mut self) -> Result<Vec<u8>, String> {
        let marker = self.byte()?;
        let len = match marker {
            0xC4 => usize::from(self.byte()?),
            0xC5 => usize::from(u16::from_be_bytes(
                self.take(2)?.try_into().expect("two bytes"),
            )),
            0xC6 => u32::from_be_bytes(self.take(4)?.try_into().expect("four bytes")) as usize,
            _ => return Err(format!("not a byte string: 0x{marker:02x}")),
        };
        Ok(self.take(len)?.to_vec())
    }

    fn arr_len(&mut self) -> Result<usize, String> {
        let marker = self.byte()?;
        match marker {
            0x90..=0x9F => Ok(usize::from(marker & 0x0F)),
            0xDC => Ok(usize::from(u16::from_be_bytes(
                self.take(2)?.try_into().expect("two bytes"),
            ))),
            0xDD => Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("four bytes")) as usize),
            _ => Err(format!("not an array header: 0x{marker:02x}")),
        }
    }

    fn map_len(&mut self) -> Result<usize, String> {
        let marker = self.byte()?;
        match marker {
            0x80..=0x8F => Ok(usize::from(marker & 0x0F)),
            0xDE => Ok(usize::from(u16::from_be_bytes(
                self.take(2)?.try_into().expect("two bytes"),
            ))),
            0xDF => Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("four bytes")) as usize),
            _ => Err(format!("not a map header: 0x{marker:02x}")),
        }
    }

    /// Reads a field identifier. All v1.0 field IDs are 1-16 and encode as one byte.
    fn field_id(&mut self) -> Result<u32, String> {
        let id = self.uint()?;
        u32::try_from(id)
            .ok()
            .filter(|id| (1..=16).contains(id))
            .ok_or_else(|| format!("field identifier out of range: {id}"))
    }

    /// Reads an attribute map: a flat `[key, type, value]` array, three slots per entry.
    fn attributes(&mut self) -> Result<Vec<(String, AttributeValue)>, String> {
        let slots = self.arr_len()?;
        if slots % 3 != 0 {
            return Err(format!(
                "attribute array length {slots} is not a multiple of 3"
            ));
        }

        let mut attributes = Vec::with_capacity(slots / 3);
        for _ in 0..slots / 3 {
            let key = self.string()?;
            let value = match self.uint()? {
                1 => AttributeValue::String(self.string()?),
                2 => AttributeValue::Bool(self.boolean()?),
                3 => AttributeValue::Double(self.double()?),
                4 => AttributeValue::Int(self.signed()?),
                other => return Err(format!("unsupported attribute value type {other}")),
            };
            attributes.push((key, value));
        }
        Ok(attributes)
    }

    fn span_link(&mut self) -> Result<SpanLink, String> {
        let mut link = SpanLink::default();
        for _ in 0..self.map_len()? {
            match self.field_id()? {
                1 => {
                    link.trace_id = self
                        .bin()?
                        .try_into()
                        .map_err(|_| "trace id length".to_string())?;
                }
                2 => link.span_id = self.uint()?,
                3 => link.attributes = self.attributes()?,
                4 => link.tracestate = self.string()?,
                5 => link.flags = u32::try_from(self.uint()?).map_err(|_| "flags".to_string())?,
                other => return Err(format!("unknown span link field {other}")),
            }
        }
        Ok(link)
    }

    fn span_event(&mut self) -> Result<SpanEvent, String> {
        let mut event = SpanEvent::default();
        for _ in 0..self.map_len()? {
            match self.field_id()? {
                1 => event.time = self.uint()?,
                2 => event.name = self.string()?,
                3 => event.attributes = self.attributes()?,
                other => return Err(format!("unknown span event field {other}")),
            }
        }
        Ok(event)
    }

    fn span(&mut self) -> Result<Span, String> {
        let mut span = Span::default();
        for _ in 0..self.map_len()? {
            match self.field_id()? {
                1 => span.service = self.string()?.into(),
                2 => span.name = self.string()?.into(),
                3 => span.resource = self.string()?.into(),
                4 => span.span_id = self.uint()?,
                5 => span.parent_id = self.uint()?,
                6 => span.start = self.uint()?,
                7 => span.duration = self.uint()?,
                8 => span.error = self.boolean()?,
                9 => span.attributes = Arc::from(self.attributes()?),
                10 => span.span_type = self.string()?.into(),
                11 => {
                    let links = self.arr_len()?;
                    span.links = (0..links)
                        .map(|_| self.span_link())
                        .collect::<Result<Vec<_>, _>>()?;
                }
                12 => {
                    let events = self.arr_len()?;
                    span.events = (0..events)
                        .map(|_| self.span_event())
                        .collect::<Result<Vec<_>, _>>()?;
                }
                13 => span.env = self.string()?.into(),
                14 => span.version = self.string()?.into(),
                15 => span.component = self.string()?.into(),
                16 => {
                    span.kind = u32::try_from(self.uint()?).map_err(|_| "span kind".to_string())?;
                }
                other => return Err(format!("unknown span field {other}")),
            }
        }
        Ok(span)
    }

    fn chunk(&mut self) -> Result<TraceChunk, String> {
        let mut chunk = TraceChunk::default();
        for _ in 0..self.map_len()? {
            match self.field_id()? {
                1 => {
                    chunk.priority =
                        Some(i32::try_from(self.signed()?).map_err(|_| "priority".to_string())?);
                }
                2 => chunk.origin = self.string()?,
                3 => chunk.attributes = self.attributes()?,
                4 => {
                    let spans = self.arr_len()?;
                    chunk.spans = (0..spans)
                        .map(|_| self.span())
                        .collect::<Result<Vec<_>, _>>()?;
                }
                5 => chunk.dropped_trace = self.boolean()?,
                6 => {
                    chunk.trace_id = self
                        .bin()?
                        .try_into()
                        .map_err(|_| "trace id length".to_string())?;
                }
                7 => {
                    chunk.sampling_mechanism = u32::try_from(self.uint()?)
                        .map_err(|_| "sampling mechanism".to_string())?;
                }
                other => return Err(format!("unknown trace chunk field {other}")),
            }
        }
        Ok(chunk)
    }

    fn tracer_payload(&mut self) -> Result<TracerPayload, String> {
        let mut payload = TracerPayload::default();
        for _ in 0..self.map_len()? {
            match self.field_id()? {
                2 => payload.container_id = self.string()?,
                3 => payload.language_name = self.string()?,
                4 => payload.language_version = self.string()?,
                5 => payload.tracer_version = self.string()?,
                6 => payload.runtime_id = self.string()?,
                7 => payload.env = self.string()?,
                8 => payload.hostname = self.string()?,
                9 => payload.app_version = self.string()?,
                10 => payload.attributes = self.attributes()?,
                11 => {
                    let chunks = self.arr_len()?;
                    payload.chunks = (0..chunks)
                        .map(|_| self.chunk())
                        .collect::<Result<Vec<_>, _>>()?;
                }
                other => return Err(format!("unknown tracer payload field {other}")),
            }
        }
        Ok(payload)
    }
}

/// Decodes a v1.0 payload through the independent reference path.
fn decode(data: &[u8]) -> Result<TracerPayload, String> {
    let mut decoder = Decoder::new(data);
    let payload = decoder.tracer_payload()?;
    if decoder.pos != data.len() {
        return Err(format!("decoded {} of {} bytes", decoder.pos, data.len()));
    }
    Ok(payload)
}

/// Sorts every attribute vector by key, so comparisons are insensitive to the wire order, which
/// the real tracer derives from Go map iteration and this module derives from key sorting.
fn normalize(payload: &mut TracerPayload) {
    payload
        .attributes
        .sort_by(|(left, _), (right, _)| left.cmp(right));
    for chunk in &mut payload.chunks {
        chunk
            .attributes
            .sort_by(|(left, _), (right, _)| left.cmp(right));
        for span in &mut chunk.spans {
            let mut attributes = span.attributes.to_vec();
            attributes.sort_by(|(left, _), (right, _)| left.cmp(right));
            span.attributes = Arc::from(attributes);
            span.links.iter_mut().for_each(|link| {
                link.attributes
                    .sort_by(|(left, _), (right, _)| left.cmp(right));
            });
            span.events.iter_mut().for_each(|event| {
                event
                    .attributes
                    .sort_by(|(left, _), (right, _)| left.cmp(right));
            });
        }
    }
}

fn attribute<'a>(attributes: &'a [(String, AttributeValue)], key: &str) -> &'a AttributeValue {
    &attributes
        .iter()
        .find(|(k, _)| k == key)
        .expect("attribute is present")
        .1
}

// One test covering the whole fixture keeps the hand-verified expectations together.
#[allow(clippy::too_many_lines)]
#[test]
fn golden_fixture_decodes_to_the_expected_tracer_payload() {
    let golden = decode(&golden_payload("nested_service_graph")).expect("fixture should decode");

    // Payload metadata written by dd-trace-go for the apm-v1-trace-smoke application.
    assert_eq!(golden.container_id, "");
    assert_eq!(golden.language_name, "go");
    assert_eq!(golden.language_version, "go1.26.1");
    assert_eq!(golden.tracer_version, "v2.11.0-dev.1");
    assert_eq!(golden.runtime_id, "a9de2e98-0940-44c4-af2b-a0fb18396b59");
    assert_eq!(golden.env, "smoke");
    assert_eq!(golden.hostname, "");
    assert_eq!(golden.app_version, "0.1.0");
    assert_eq!(
        attribute(&golden.attributes, "_dd.tags.process"),
        &AttributeValue::String(
            "entrypoint.basedir:apm-v1-trace-smoke,entrypoint.name:apm-v1-trace-smoke,\
             entrypoint.type:executable,entrypoint.workdir:lading,svc.user:true"
                .to_string()
        )
    );

    // The tracer batched three emission rounds, the first two carrying a failing trace.
    assert_eq!(golden.chunks.len(), 6);
    assert_eq!(
        golden
            .chunks
            .iter()
            .map(|chunk| chunk.spans.len())
            .collect::<Vec<_>>(),
        vec![4, 4, 1, 4, 4, 1]
    );
    for chunk in &golden.chunks {
        assert_eq!(chunk.priority, Some(1));
        // Empty streaming strings decode through table index 0.
        assert_eq!(chunk.origin, "");
        assert_eq!(chunk.sampling_mechanism, 1);
        assert!(!chunk.dropped_trace);
    }

    // The first chunk, verified span for span: a web entry point nesting a database query and a
    // two-deep cache lookup.
    let chunk = &golden.chunks[0];
    assert_eq!(
        chunk.attributes,
        vec![(
            "service".to_string(),
            AttributeValue::String("smoke-web".to_string())
        )]
    );
    assert_eq!(
        chunk.trace_id,
        [
            0x6a, 0xa4, 0x14, 0x75, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x54, 0x4b, 0xbf, 0x21, 0x12,
            0x6a, 0x84
        ]
    );

    let entry = &chunk.spans[0];
    assert_eq!(entry.service.as_ref(), "smoke-web");
    assert_eq!(entry.name.as_ref(), "web.request");
    assert_eq!(entry.resource.as_ref(), "POST /checkout");
    assert_eq!(entry.span_id, 816_360_716_726_594_180);
    assert_eq!(entry.parent_id, 0);
    assert_eq!(entry.start, 1_789_138_037_047_199_000);
    assert_eq!(entry.duration, 101_000);
    assert!(!entry.error);
    assert_eq!(entry.env.as_ref(), "smoke");
    assert_eq!(entry.version.as_ref(), "0.1.0");
    assert_eq!(entry.component.as_ref(), "");
    assert_eq!(entry.kind, 2);
    // String-table resolution across nesting levels: the attribute value 'go' and 'runtime-id'
    // reference the strings the payload-level fields wrote earlier, and the process-tags value
    // references the payload-level attribute's value.
    assert_eq!(
        attribute(&entry.attributes, "language"),
        &AttributeValue::String("go".to_string())
    );
    assert_eq!(
        attribute(&entry.attributes, "_dd.p.dm"),
        &AttributeValue::String("-1".to_string())
    );
    assert_eq!(
        attribute(&entry.attributes, "_dd.p.tid"),
        &AttributeValue::String("6aa4147500000000".to_string())
    );
    assert_eq!(
        attribute(&entry.attributes, "runtime-id"),
        &AttributeValue::String("a9de2e98-0940-44c4-af2b-a0fb18396b59".to_string())
    );
    assert_eq!(
        attribute(&entry.attributes, "_dd.profiling.enabled"),
        &AttributeValue::Double(0.0)
    );
    assert_eq!(
        attribute(&entry.attributes, "_dd.top_level"),
        &AttributeValue::Double(1.0)
    );
    assert_eq!(
        attribute(&entry.attributes, "process_id"),
        &AttributeValue::Double(96_756.0)
    );
    assert_eq!(entry.attributes.len(), 11);

    let db_query = &chunk.spans[1];
    assert_eq!(db_query.service.as_ref(), "smoke-db");
    assert_eq!(db_query.name.as_ref(), "db.query");
    assert_eq!(db_query.resource.as_ref(), "INSERT INTO orders");
    assert_eq!(db_query.span_id, 3_367_157_276_942_034_446);
    assert_eq!(db_query.parent_id, 816_360_716_726_594_180);
    assert_eq!(db_query.duration, 23_000);
    assert_eq!(db_query.kind, 3);
    assert_eq!(db_query.attributes.len(), 8);

    let cache_get = &chunk.spans[2];
    assert_eq!(cache_get.service.as_ref(), "smoke-cache");
    assert_eq!(cache_get.name.as_ref(), "cache.get");
    assert_eq!(cache_get.resource.as_ref(), "cart:*");
    assert_eq!(cache_get.parent_id, 816_360_716_726_594_180);
    assert_eq!(cache_get.kind, 3);

    let cache_deserialize = &chunk.spans[3];
    assert_eq!(cache_deserialize.service.as_ref(), "smoke-cache");
    assert_eq!(cache_deserialize.name.as_ref(), "cache.deserialize");
    assert_eq!(cache_deserialize.parent_id, 2_114_690_021_254_732_574);
    assert_eq!(
        attribute(&cache_deserialize.attributes, "cache.hit"),
        &AttributeValue::String("true".to_string())
    );
    assert_eq!(cache_deserialize.attributes.len(), 7);

    // Error spans carry their failure as attributes plus the error bit, not as a status code.
    for (chunk_index, span_index, name) in [(1, 2, "db.query"), (1, 3, "db.rows.scan")] {
        let span = &golden.chunks[chunk_index].spans[span_index];
        assert!(span.error, "span {name} should be marked as an error");
        assert_eq!(
            attribute(&span.attributes, "error.type"),
            &AttributeValue::String("main.searchFailed".to_string()),
            "span {name} should carry its error type"
        );
    }
}

#[test]
fn lading_encoding_of_the_golden_chunk_decodes_identically() {
    let golden = decode(&golden_payload("nested_service_graph")).expect("fixture should decode");

    // The payload-level metadata plus the first chunk, as lading's own encoder would carry them.
    let expected = TracerPayload {
        container_id: golden.container_id.clone(),
        language_name: golden.language_name.clone(),
        language_version: golden.language_version.clone(),
        tracer_version: golden.tracer_version.clone(),
        runtime_id: golden.runtime_id.clone(),
        env: golden.env.clone(),
        hostname: golden.hostname.clone(),
        app_version: golden.app_version.clone(),
        attributes: golden.attributes.clone(),
        chunks: vec![golden.chunks[0].clone()],
    };

    let encoded = expected
        .encode()
        .expect("encoding into a buffer cannot fail");
    let mut round_tripped = decode(&encoded).expect("lading encoding should decode");

    normalize(&mut round_tripped);
    let mut expected = expected;
    normalize(&mut expected);
    assert_eq!(round_tripped, expected);
}

#[test]
fn golden_links_and_events_decode_to_the_expected_values() {
    let golden = decode(&golden_payload("links_and_events")).expect("fixture should decode");

    // The tracer attached one link to each trace's entry span, referencing a fixed synthetic
    // trace so the wire values are predictable.
    let linked = &golden.chunks[0].spans[0];
    assert_eq!(linked.links.len(), 1);
    let link = &linked.links[0];
    // The 16-byte trace ID is the linked span's ID as high-bits-then-low-bits big-endian.
    assert_eq!(
        link.trace_id,
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88
        ]
    );
    assert_eq!(link.span_id, 0x99aa_bbcc_ddee_ff00);
    assert_eq!(
        attribute(&link.attributes, "link.kind"),
        &AttributeValue::String("follows-from".to_string())
    );
    // An unset tracestate decodes through table index 0.
    assert_eq!(link.tracestate, "");
    assert_eq!(link.flags, 1);

    // The cache span carries an event with one attribute of each scalar type.
    let event_span = &golden.chunks[0].spans[3];
    assert_eq!(event_span.name.as_ref(), "cache.deserialize");
    assert_eq!(event_span.events.len(), 1);
    let event = &event_span.events[0];
    assert_eq!(event.name, "cache.miss");
    assert!(event.time > 0);
    assert_eq!(
        attribute(&event.attributes, "cache.key"),
        &AttributeValue::String("cart:*".to_string())
    );
    assert_eq!(
        attribute(&event.attributes, "cache.depth"),
        &AttributeValue::Int(2)
    );
    assert_eq!(
        attribute(&event.attributes, "cache.hot"),
        &AttributeValue::Bool(false)
    );
    assert_eq!(
        attribute(&event.attributes, "cache.ratio"),
        &AttributeValue::Double(0.25)
    );
}

#[test]
fn lading_encoding_of_golden_links_and_events_decodes_identically() {
    let golden = decode(&golden_payload("links_and_events")).expect("fixture should decode");

    let expected = TracerPayload {
        container_id: golden.container_id.clone(),
        hostname: golden.hostname.clone(),
        language_name: golden.language_name.clone(),
        language_version: golden.language_version.clone(),
        tracer_version: golden.tracer_version.clone(),
        runtime_id: golden.runtime_id.clone(),
        env: golden.env.clone(),
        app_version: golden.app_version.clone(),
        attributes: golden.attributes.clone(),
        chunks: vec![golden.chunks[0].clone()],
    };

    let encoded = expected
        .encode()
        .expect("encoding into a buffer cannot fail");
    let mut round_tripped = decode(&encoded).expect("lading encoding should decode");

    normalize(&mut round_tripped);
    let mut expected = expected;
    normalize(&mut expected);
    assert_eq!(round_tripped, expected);
}
