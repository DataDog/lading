use std::io;

use serde_json::Value;

use crate::Error;

const HEX: &[u8] = b"0123456789abcdef";

// ── JsonString ────────────────────────────────────────────────────────────────

/// Pre-serialized compact JSON text used as an intermediate buffer during generation.
///
/// Keeping values in their serialized form avoids redundant deserialization/
/// re-serialization when values are bound by `!with` and reused by `!var`.
#[derive(Clone, Debug, Default)]
pub(super) struct JsonString(String);

impl JsonString {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self(String::with_capacity(capacity))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }

    /// Reset the buffer length to zero while retaining allocated capacity.
    pub(super) fn clear(&mut self) {
        self.0.clear();
    }

    pub(super) fn push_raw_str(&mut self, s: &str) {
        self.0.push_str(s);
    }

    pub(super) fn push_char(&mut self, c: char) {
        self.0.push(c);
    }

    pub(super) fn push_value(&mut self, value: &Value) -> Result<(), Error> {
        serde_json::to_writer(&mut *self, value)?;
        Ok(())
    }

    pub(super) fn push_str_as_json(&mut self, value: &str) -> Result<(), Error> {
        serde_json::to_writer(&mut *self, value)?;
        Ok(())
    }

    /// Append `s` with JSON string escaping applied, for direct embedding inside
    /// an already-open JSON string literal (no surrounding quotes added).
    ///
    /// Handles `"`, `\`, and all ASCII control characters. Non-ASCII UTF-8 bytes
    /// are copied verbatim, which is correct because JSON permits raw Unicode.
    /// Bytes >= 0x80 are never `"`, `\`, or control characters, so they are
    /// copied in bulk via the `_ => continue` path without slicing UTF-8 sequences.
    pub(super) fn push_str_json_escaped(&mut self, s: &str) {
        let out = &mut self.0;
        let bytes = s.as_bytes();
        let mut start = 0usize;
        for i in 0..bytes.len() {
            let esc: &str = match bytes[i] {
                b'"' => "\\\"",
                b'\\' => "\\\\",
                b'\x08' => "\\b",
                b'\t' => "\\t",
                b'\n' => "\\n",
                b'\x0c' => "\\f",
                b'\r' => "\\r",
                0x00..=0x07 | 0x0b | 0x0e..=0x1f => {
                    out.push_str(&s[start..i]);
                    out.push_str("\\u00");
                    out.push(HEX[(bytes[i] >> 4) as usize] as char);
                    out.push(HEX[(bytes[i] & 0xf) as usize] as char);
                    start = i + 1;
                    continue;
                }
                _ => continue,
            };
            out.push_str(&s[start..i]);
            out.push_str(esc);
            start = i + 1;
        }
        out.push_str(&s[start..]);
    }
}

impl io::Write for JsonString {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Safety: this impl is only called by `serde_json::to_writer`, which
        // exclusively emits valid UTF-8 (JSON is defined as UTF-8). Validating
        // every buffer with `from_utf8` is therefore redundant overhead.
        let s = unsafe { std::str::from_utf8_unchecked(buf) };
        self.0.push_str(s);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
