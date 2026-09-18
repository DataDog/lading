//! Streaming string-table `MessagePack` encoder for v1.0 payloads.
use std::io;

use rmp::encode::ValueWriteError;
use rustc_hash::FxHashMap;

use super::{AttributeValue, Span, SpanEvent, SpanLink, TraceChunk, TracerPayload};

/// Encoder failure modes.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The underlying writer failed while emitting `MessagePack` bytes.
    #[error("MessagePack write failed: {0}")]
    Write(#[from] ValueWriteError<io::Error>),
    /// A map or array length exceeded the format's `u32` bound.
    #[error("map or array length exceeds the MessagePack u32 bound")]
    LengthOverflow,
}

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

/// Bundles the destination buffer with its streaming string table.
///
/// Grouping the two frees every write step from threading a writer and a table through its
/// signature separately.
struct Encoder {
    writer: Vec<u8>,
    strings: StringTable,
}

impl Encoder {
    /// Writes a streaming string: inline on its first appearance, by index thereafter.
    ///
    /// The empty string is always index 0. Writing it inline would consume a table slot on the
    /// reading side and shift every later index, so it is never written as a literal.
    fn write_streaming_string(&mut self, s: &str) -> Result<(), Error> {
        if s.is_empty() {
            return self.write_u32(0);
        }

        if let Some(&index) = self.strings.indices.get(s) {
            return self.write_u32(index);
        }

        rmp::encode::write_str(&mut self.writer, s)?;
        self.strings
            .indices
            .insert(s.to_string(), self.strings.next_index);
        self.strings.next_index += 1;
        Ok(())
    }

    fn write_u32(&mut self, value: u32) -> Result<(), Error> {
        rmp::encode::write_uint(&mut self.writer, u64::from(value))?;
        Ok(())
    }

    fn write_u64(&mut self, value: u64) -> Result<(), Error> {
        rmp::encode::write_uint(&mut self.writer, value)?;
        Ok(())
    }

    fn write_sint(&mut self, value: i64) -> Result<(), Error> {
        rmp::encode::write_sint(&mut self.writer, value)?;
        Ok(())
    }

    fn write_bool(&mut self, value: bool) -> Result<(), Error> {
        // `write_bool` reports a bare writer error: its only write is the marker byte.
        rmp::encode::write_bool(&mut self.writer, value)
            .map_err(|e| Error::Write(ValueWriteError::InvalidMarkerWrite(e)))
    }

    fn write_bin(&mut self, value: &[u8]) -> Result<(), Error> {
        rmp::encode::write_bin(&mut self.writer, value)?;
        Ok(())
    }

    fn write_map_len(&mut self, len: usize) -> Result<(), Error> {
        let len = u32::try_from(len).map_err(|_| Error::LengthOverflow)?;
        rmp::encode::write_map_len(&mut self.writer, len)?;
        Ok(())
    }

    fn write_array_len(&mut self, len: usize) -> Result<(), Error> {
        let len = u32::try_from(len).map_err(|_| Error::LengthOverflow)?;
        rmp::encode::write_array_len(&mut self.writer, len)?;
        Ok(())
    }

    /// Writes an attribute map as a flat `[key, type, value]` array, three slots per entry.
    fn write_attributes(&mut self, attributes: &[(String, AttributeValue)]) -> Result<(), Error> {
        self.write_array_len(attributes.len() * 3)?;
        for (key, value) in attributes {
            self.write_streaming_string(key)?;
            self.write_any_value(value)?;
        }
        Ok(())
    }

    /// Writes an `AnyValue`: a `uint32` type discriminant followed by the value itself.
    fn write_any_value(&mut self, value: &AttributeValue) -> Result<(), Error> {
        match value {
            AttributeValue::String(s) => {
                self.write_u32(1)?;
                self.write_streaming_string(s)
            }
            AttributeValue::Bool(b) => {
                self.write_u32(2)?;
                self.write_bool(*b)
            }
            AttributeValue::Double(d) => {
                self.write_u32(3)?;
                rmp::encode::write_f64(&mut self.writer, *d)?;
                Ok(())
            }
            AttributeValue::Int(i) => {
                self.write_u32(4)?;
                self.write_sint(*i)
            }
        }
    }
}

impl TracerPayload {
    /// Encodes the payload into its `v1.0` `MessagePack` representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails. Writing into an in-memory buffer cannot
    /// fail, so callers that do so can treat this as infallible.
    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut encoder = Encoder {
            writer: Vec::new(),
            strings: StringTable::new(),
        };
        self.write(&mut encoder)?;
        Ok(encoder.writer)
    }

    fn write(&self, encoder: &mut Encoder) -> Result<(), Error> {
        // The reference encoder omits every field holding its zero value, and counts the survivors
        // to size the map header. Field 1, the explicit string table, is never emitted: strings
        // stream inline instead.
        let string_fields = [
            (2, &self.container_id),
            (3, &self.language_name),
            (4, &self.language_version),
            (5, &self.tracer_version),
            (6, &self.runtime_id),
            (7, &self.env),
            (8, &self.hostname),
            (9, &self.app_version),
        ];

        let num_fields = string_fields.iter().filter(|(_, v)| !v.is_empty()).count()
            + usize::from(!self.attributes.is_empty())
            + usize::from(!self.chunks.is_empty());
        encoder.write_map_len(num_fields)?;

        for (field, value) in string_fields {
            if !value.is_empty() {
                encoder.write_u32(field)?;
                encoder.write_streaming_string(value)?;
            }
        }

        if !self.attributes.is_empty() {
            encoder.write_u32(10)?;
            encoder.write_attributes(&self.attributes)?;
        }

        if !self.chunks.is_empty() {
            encoder.write_u32(11)?;
            encoder.write_array_len(self.chunks.len())?;
            for chunk in &self.chunks {
                chunk.write(encoder)?;
            }
        }

        Ok(())
    }
}

impl TraceChunk {
    fn write(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let num_fields = usize::from(self.priority.is_some())
            + usize::from(!self.origin.is_empty())
            + usize::from(!self.attributes.is_empty())
            + usize::from(!self.spans.is_empty())
            + usize::from(self.dropped_trace)
            + usize::from(self.trace_id != [0u8; 16])
            + usize::from(self.sampling_mechanism != 0);
        encoder.write_map_len(num_fields)?;

        if let Some(priority) = self.priority {
            encoder.write_u32(1)?;
            encoder.write_sint(i64::from(priority))?;
        }

        if !self.origin.is_empty() {
            encoder.write_u32(2)?;
            encoder.write_streaming_string(&self.origin)?;
        }

        if !self.attributes.is_empty() {
            encoder.write_u32(3)?;
            encoder.write_attributes(&self.attributes)?;
        }

        if !self.spans.is_empty() {
            encoder.write_u32(4)?;
            encoder.write_array_len(self.spans.len())?;
            for span in &self.spans {
                span.write(encoder)?;
            }
        }

        if self.dropped_trace {
            encoder.write_u32(5)?;
            encoder.write_bool(true)?;
        }

        if self.trace_id != [0u8; 16] {
            encoder.write_u32(6)?;
            encoder.write_bin(&self.trace_id)?;
        }

        if self.sampling_mechanism != 0 {
            encoder.write_u32(7)?;
            encoder.write_u32(self.sampling_mechanism)?;
        }

        Ok(())
    }
}

impl Span {
    fn write(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let num_fields = usize::from(!self.service.is_empty())
            + usize::from(!self.name.is_empty())
            + usize::from(!self.resource.is_empty())
            + usize::from(self.span_id != 0)
            + usize::from(self.parent_id != 0)
            + usize::from(self.start != 0)
            + usize::from(self.duration != 0)
            + usize::from(self.error)
            + usize::from(!self.attributes.is_empty())
            + usize::from(!self.span_type.is_empty())
            + usize::from(!self.links.is_empty())
            + usize::from(!self.events.is_empty())
            + usize::from(!self.env.is_empty())
            + usize::from(!self.version.is_empty())
            + usize::from(!self.component.is_empty())
            + usize::from(self.kind != 0);
        encoder.write_map_len(num_fields)?;

        if !self.service.is_empty() {
            encoder.write_u32(1)?;
            encoder.write_streaming_string(&self.service)?;
        }
        if !self.name.is_empty() {
            encoder.write_u32(2)?;
            encoder.write_streaming_string(&self.name)?;
        }
        if !self.resource.is_empty() {
            encoder.write_u32(3)?;
            encoder.write_streaming_string(&self.resource)?;
        }
        if self.span_id != 0 {
            encoder.write_u32(4)?;
            encoder.write_u64(self.span_id)?;
        }
        if self.parent_id != 0 {
            encoder.write_u32(5)?;
            encoder.write_u64(self.parent_id)?;
        }
        if self.start != 0 {
            encoder.write_u32(6)?;
            encoder.write_u64(self.start)?;
        }
        if self.duration != 0 {
            encoder.write_u32(7)?;
            encoder.write_u64(self.duration)?;
        }
        if self.error {
            encoder.write_u32(8)?;
            encoder.write_bool(true)?;
        }
        if !self.attributes.is_empty() {
            encoder.write_u32(9)?;
            encoder.write_attributes(&self.attributes)?;
        }
        if !self.span_type.is_empty() {
            encoder.write_u32(10)?;
            encoder.write_streaming_string(&self.span_type)?;
        }
        if !self.links.is_empty() {
            encoder.write_u32(11)?;
            encoder.write_array_len(self.links.len())?;
            for link in &self.links {
                link.write(encoder)?;
            }
        }
        if !self.events.is_empty() {
            encoder.write_u32(12)?;
            encoder.write_array_len(self.events.len())?;
            for event in &self.events {
                event.write(encoder)?;
            }
        }
        if !self.env.is_empty() {
            encoder.write_u32(13)?;
            encoder.write_streaming_string(&self.env)?;
        }
        if !self.version.is_empty() {
            encoder.write_u32(14)?;
            encoder.write_streaming_string(&self.version)?;
        }
        if !self.component.is_empty() {
            encoder.write_u32(15)?;
            encoder.write_streaming_string(&self.component)?;
        }
        if self.kind != 0 {
            encoder.write_u32(16)?;
            encoder.write_u32(self.kind)?;
        }

        Ok(())
    }
}

impl SpanLink {
    fn write(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let num_fields = usize::from(self.trace_id != [0u8; 16])
            + usize::from(self.span_id != 0)
            + usize::from(!self.attributes.is_empty())
            + usize::from(!self.tracestate.is_empty())
            + usize::from(self.flags != 0);
        encoder.write_map_len(num_fields)?;

        if self.trace_id != [0u8; 16] {
            encoder.write_u32(1)?;
            encoder.write_bin(&self.trace_id)?;
        }
        if self.span_id != 0 {
            encoder.write_u32(2)?;
            encoder.write_u64(self.span_id)?;
        }
        if !self.attributes.is_empty() {
            encoder.write_u32(3)?;
            encoder.write_attributes(&self.attributes)?;
        }
        if !self.tracestate.is_empty() {
            encoder.write_u32(4)?;
            encoder.write_streaming_string(&self.tracestate)?;
        }
        if self.flags != 0 {
            encoder.write_u32(5)?;
            encoder.write_u32(self.flags)?;
        }

        Ok(())
    }
}

impl SpanEvent {
    fn write(&self, encoder: &mut Encoder) -> Result<(), Error> {
        let num_fields = usize::from(self.time != 0)
            + usize::from(!self.name.is_empty())
            + usize::from(!self.attributes.is_empty());
        encoder.write_map_len(num_fields)?;

        if self.time != 0 {
            encoder.write_u32(1)?;
            encoder.write_u64(self.time)?;
        }
        if !self.name.is_empty() {
            encoder.write_u32(2)?;
            encoder.write_streaming_string(&self.name)?;
        }
        if !self.attributes.is_empty() {
            encoder.write_u32(3)?;
            encoder.write_attributes(&self.attributes)?;
        }

        Ok(())
    }
}
