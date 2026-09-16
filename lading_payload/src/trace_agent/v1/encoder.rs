//! Streaming string-table `MessagePack` encoder for v1.0 payloads.
use std::io::Write;

use rmp::encode::ValueWriteError;
use rustc_hash::FxHashMap;

use super::{AttributeValue, Span, SpanEvent, SpanLink, TraceChunk, TracerPayload};
use crate::Error;

/// Encoder-side streaming string table.
///
/// Assigns each distinct non-empty string the index it will occupy on the decoding side, which is
/// determined solely by the order strings are first written. Index 0 is the pre-seeded empty
/// string and is never assigned here.
struct StringTable {
    indices: FxHashMap<String, u32>,
    next_index: u32,
}

impl StringTable {
    fn new() -> Self {
        Self {
            indices: FxHashMap::default(),
            next_index: 1,
        }
    }
}

/// Writes a streaming string: inline on its first appearance, by index thereafter.
///
/// The empty string is always index 0. Writing it inline would consume a table slot on the reading
/// side and shift every later index, so it is never written as a literal.
fn write_streaming_string<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    s: &str,
) -> Result<(), Error> {
    if s.is_empty() {
        return write_u32(w, 0);
    }

    if let Some(index) = strings.indices.get(s) {
        return write_u32(w, *index);
    }

    rmp::encode::write_str(w, s).map_err(map_write_error)?;
    strings.indices.insert(s.to_string(), strings.next_index);
    strings.next_index += 1;
    Ok(())
}

/// Maps a length that exceeds the format's `u32` bound into the crate error type.
fn cast_error(error: std::num::TryFromIntError) -> Error {
    Error::Validation(error.to_string())
}

/// Maps an `rmp` encoding error, which wraps an `io::Error` when writing to a std writer, into the
/// crate error type.
fn map_write_error(error: rmp::encode::ValueWriteError<std::io::Error>) -> Error {
    match error {
        ValueWriteError::InvalidMarkerWrite(e) | ValueWriteError::InvalidDataWrite(e) => {
            Error::Io(e)
        }
    }
}

fn write_u32<W: Write>(w: &mut W, value: u32) -> Result<(), Error> {
    rmp::encode::write_uint(w, u64::from(value))
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_u64<W: Write>(w: &mut W, value: u64) -> Result<(), Error> {
    rmp::encode::write_uint(w, value)
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_map_len<W: Write>(w: &mut W, len: usize) -> Result<(), Error> {
    rmp::encode::write_map_len(w, u32::try_from(len).map_err(cast_error)?)
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_array_len<W: Write>(w: &mut W, len: usize) -> Result<(), Error> {
    rmp::encode::write_array_len(w, u32::try_from(len).map_err(cast_error)?)
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_bool<W: Write>(w: &mut W, value: bool) -> Result<(), Error> {
    rmp::encode::write_bool(w, value).map_err(Error::Io)
}

fn write_bin<W: Write>(w: &mut W, value: &[u8]) -> Result<(), Error> {
    rmp::encode::write_bin(w, value).map_err(map_write_error)
}

/// Writes an attribute map as a flat `[key, type, value]` array, three slots per entry.
fn write_attributes<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    attributes: &[(String, AttributeValue)],
) -> Result<(), Error> {
    write_array_len(w, attributes.len() * 3)?;
    for (key, value) in attributes {
        write_streaming_string(w, strings, key)?;
        write_any_value(w, strings, value)?;
    }
    Ok(())
}

/// Writes an `AnyValue`: a `uint32` type discriminant followed by the value itself.
fn write_any_value<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    value: &AttributeValue,
) -> Result<(), Error> {
    match value {
        AttributeValue::String(s) => {
            write_u32(w, 1)?;
            write_streaming_string(w, strings, s)
        }
        AttributeValue::Bool(b) => {
            write_u32(w, 2)?;
            write_bool(w, *b)
        }
        AttributeValue::Double(d) => {
            write_u32(w, 3)?;
            rmp::encode::write_f64(w, *d).map_err(map_write_error)
        }
        AttributeValue::Int(i) => {
            write_u32(w, 4)?;
            rmp::encode::write_sint(w, *i)
                .map(|_| ())
                .map_err(map_write_error)
        }
    }
}

fn write_tracer_payload<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    payload: &TracerPayload,
) -> Result<(), Error> {
    // The reference encoder omits every field holding its zero value, and counts the survivors to
    // size the map header. Field 1, the explicit string table, is never emitted: strings stream
    // inline instead.
    let string_fields = [
        (2, &payload.container_id),
        (3, &payload.language_name),
        (4, &payload.language_version),
        (5, &payload.tracer_version),
        (6, &payload.runtime_id),
        (7, &payload.env),
        (8, &payload.hostname),
        (9, &payload.app_version),
    ];

    let num_fields = string_fields.iter().filter(|(_, v)| !v.is_empty()).count()
        + usize::from(!payload.attributes.is_empty())
        + usize::from(!payload.chunks.is_empty());
    write_map_len(w, num_fields)?;

    for (field, value) in string_fields {
        if !value.is_empty() {
            write_u32(w, field)?;
            write_streaming_string(w, strings, value)?;
        }
    }

    if !payload.attributes.is_empty() {
        write_u32(w, 10)?;
        write_attributes(w, strings, &payload.attributes)?;
    }

    if !payload.chunks.is_empty() {
        write_u32(w, 11)?;
        write_array_len(w, payload.chunks.len())?;
        for chunk in &payload.chunks {
            write_chunk(w, strings, chunk)?;
        }
    }

    Ok(())
}

fn write_chunk<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    chunk: &TraceChunk,
) -> Result<(), Error> {
    let num_fields = usize::from(chunk.priority.is_some())
        + usize::from(!chunk.origin.is_empty())
        + usize::from(!chunk.attributes.is_empty())
        + usize::from(!chunk.spans.is_empty())
        + usize::from(chunk.dropped_trace)
        + usize::from(chunk.trace_id != [0u8; 16])
        + usize::from(chunk.sampling_mechanism != 0);
    write_map_len(w, num_fields)?;

    if let Some(priority) = chunk.priority {
        write_u32(w, 1)?;
        rmp::encode::write_sint(w, i64::from(priority))
            .map(|_| ())
            .map_err(map_write_error)?;
    }

    if !chunk.origin.is_empty() {
        write_u32(w, 2)?;
        write_streaming_string(w, strings, &chunk.origin)?;
    }

    if !chunk.attributes.is_empty() {
        write_u32(w, 3)?;
        write_attributes(w, strings, &chunk.attributes)?;
    }

    if !chunk.spans.is_empty() {
        write_u32(w, 4)?;
        write_array_len(w, chunk.spans.len())?;
        for span in &chunk.spans {
            write_span(w, strings, span)?;
        }
    }

    if chunk.dropped_trace {
        write_u32(w, 5)?;
        write_bool(w, true)?;
    }

    if chunk.trace_id != [0u8; 16] {
        write_u32(w, 6)?;
        write_bin(w, &chunk.trace_id)?;
    }

    if chunk.sampling_mechanism != 0 {
        write_u32(w, 7)?;
        write_u32(w, chunk.sampling_mechanism)?;
    }

    Ok(())
}

fn write_span<W: Write>(w: &mut W, strings: &mut StringTable, span: &Span) -> Result<(), Error> {
    let num_fields = usize::from(!span.service.is_empty())
        + usize::from(!span.name.is_empty())
        + usize::from(!span.resource.is_empty())
        + usize::from(span.span_id != 0)
        + usize::from(span.parent_id != 0)
        + usize::from(span.start != 0)
        + usize::from(span.duration != 0)
        + usize::from(span.error)
        + usize::from(!span.attributes.is_empty())
        + usize::from(!span.span_type.is_empty())
        + usize::from(!span.links.is_empty())
        + usize::from(!span.events.is_empty())
        + usize::from(!span.env.is_empty())
        + usize::from(!span.version.is_empty())
        + usize::from(!span.component.is_empty())
        + usize::from(span.kind != 0);
    write_map_len(w, num_fields)?;

    if !span.service.is_empty() {
        write_u32(w, 1)?;
        write_streaming_string(w, strings, &span.service)?;
    }
    if !span.name.is_empty() {
        write_u32(w, 2)?;
        write_streaming_string(w, strings, &span.name)?;
    }
    if !span.resource.is_empty() {
        write_u32(w, 3)?;
        write_streaming_string(w, strings, &span.resource)?;
    }
    if span.span_id != 0 {
        write_u32(w, 4)?;
        write_u64(w, span.span_id)?;
    }
    if span.parent_id != 0 {
        write_u32(w, 5)?;
        write_u64(w, span.parent_id)?;
    }
    if span.start != 0 {
        write_u32(w, 6)?;
        write_u64(w, span.start)?;
    }
    if span.duration != 0 {
        write_u32(w, 7)?;
        write_u64(w, span.duration)?;
    }
    if span.error {
        write_u32(w, 8)?;
        write_bool(w, true)?;
    }
    if !span.attributes.is_empty() {
        write_u32(w, 9)?;
        write_attributes(w, strings, &span.attributes)?;
    }
    if !span.span_type.is_empty() {
        write_u32(w, 10)?;
        write_streaming_string(w, strings, &span.span_type)?;
    }
    if !span.links.is_empty() {
        write_u32(w, 11)?;
        write_array_len(w, span.links.len())?;
        for link in &span.links {
            write_span_link(w, strings, link)?;
        }
    }
    if !span.events.is_empty() {
        write_u32(w, 12)?;
        write_array_len(w, span.events.len())?;
        for event in &span.events {
            write_span_event(w, strings, event)?;
        }
    }
    if !span.env.is_empty() {
        write_u32(w, 13)?;
        write_streaming_string(w, strings, &span.env)?;
    }
    if !span.version.is_empty() {
        write_u32(w, 14)?;
        write_streaming_string(w, strings, &span.version)?;
    }
    if !span.component.is_empty() {
        write_u32(w, 15)?;
        write_streaming_string(w, strings, &span.component)?;
    }
    if span.kind != 0 {
        write_u32(w, 16)?;
        write_u32(w, span.kind)?;
    }

    Ok(())
}

fn write_span_link<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    link: &SpanLink,
) -> Result<(), Error> {
    let num_fields = usize::from(link.trace_id != [0u8; 16])
        + usize::from(link.span_id != 0)
        + usize::from(!link.attributes.is_empty())
        + usize::from(!link.tracestate.is_empty())
        + usize::from(link.flags != 0);
    write_map_len(w, num_fields)?;

    if link.trace_id != [0u8; 16] {
        write_u32(w, 1)?;
        write_bin(w, &link.trace_id)?;
    }
    if link.span_id != 0 {
        write_u32(w, 2)?;
        write_u64(w, link.span_id)?;
    }
    if !link.attributes.is_empty() {
        write_u32(w, 3)?;
        write_attributes(w, strings, &link.attributes)?;
    }
    if !link.tracestate.is_empty() {
        write_u32(w, 4)?;
        write_streaming_string(w, strings, &link.tracestate)?;
    }
    if link.flags != 0 {
        write_u32(w, 5)?;
        write_u32(w, link.flags)?;
    }

    Ok(())
}

fn write_span_event<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    event: &SpanEvent,
) -> Result<(), Error> {
    let num_fields = usize::from(event.time != 0)
        + usize::from(!event.name.is_empty())
        + usize::from(!event.attributes.is_empty());
    write_map_len(w, num_fields)?;

    if event.time != 0 {
        write_u32(w, 1)?;
        write_u64(w, event.time)?;
    }
    if !event.name.is_empty() {
        write_u32(w, 2)?;
        write_streaming_string(w, strings, &event.name)?;
    }
    if !event.attributes.is_empty() {
        write_u32(w, 3)?;
        write_attributes(w, strings, &event.attributes)?;
    }

    Ok(())
}

impl TracerPayload {
    /// Encodes the payload into its `v1.0` `MessagePack` representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails. Writing into an in-memory buffer cannot
    /// fail, so callers that do so can treat this as infallible.
    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        let mut strings = StringTable::new();
        write_tracer_payload(&mut buf, &mut strings, self)?;
        Ok(buf)
    }
}
