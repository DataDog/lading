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

    /// Shorten the buffer to `len` bytes, dropping any trailing content.
    ///
    /// Callers must ensure `len` falls on a UTF-8 character boundary. In
    /// practice this is always satisfied because we only truncate at the
    /// position of a known ASCII terminator (`"`, `]`, or `}`).
    pub(super) fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    /// Merge `next` into `self` according to `!concat` semantics.
    ///
    /// Same-typed strings, arrays, and objects are spliced together by simple
    /// suffix/prefix removal:
    ///
    /// - Both strings: the closing `"` is popped from `self` and the body of
    ///   `next` (everything after its opening `"`) is appended, yielding one
    ///   combined quoted string.
    /// - Both arrays: the closing `]` is popped from `self`; a `,` separator
    ///   is inserted unless `self` was an empty array (`[`); then the body of
    ///   `next` (everything after its opening `[`) is appended.
    /// - Both objects: same pattern as arrays, using `{` / `}`.
    /// - Any other combination (type mismatch or non-concatenable scalar):
    ///   `self` and `next` are swapped so that `self` holds the newer value.
    ///   The caller is expected to clear `next` before its next use; no
    ///   additional cleanup is needed here.
    pub(super) fn merge_concat(&mut self, next: &mut Self) {
        let cur_byte0 = self.as_str().as_bytes().first().copied();
        let nxt_byte0 = next.as_str().as_bytes().first().copied();
        match (cur_byte0, nxt_byte0) {
            (Some(b'"'), Some(b'"')) => self.concat_delimited(next, "\"\"", false),
            (Some(b'['), Some(b'[')) => self.concat_delimited(next, "[]", true),
            (Some(b'{'), Some(b'{')) => self.concat_delimited(next, "{}", true),
            _ => {
                // Type mismatch or non-concatenable scalar: replace self with next via a zero-copy
                // swap. The discarded value ends up in next to retain the allocated capacity, which
                // we truncate to leave it empty.
                std::mem::swap(self, next);
                next.clear();
            }
        }
    }

    /// Append the body of `next` into `self` for any delimited JSON
    /// type (strings, arrays, and objects).
    ///
    /// `empty` is the two-character sentinel for an empty value of this type
    /// (e.g. `"\"\""` for strings, `"[]"` for arrays, `"{}"` for objects).
    /// `needs_separator` controls whether a `','` is emitted between the
    /// existing content and the new body; strings never need one, whereas
    /// arrays and objects do.
    ///
    /// - If `next` is empty: no-op.
    /// - If `self` is empty: swap `self` and `next` (zero-copy), then clear
    ///   `next` so the caller starts the following iteration with a clean buffer.
    /// - Otherwise: pop the trailing closing delimiter from `self`, optionally
    ///   emit `','`, then append `next[1..]` (body + closing delimiter).
    fn concat_delimited(&mut self, next: &mut Self, empty: &str, needs_separator: bool) {
        if next.as_str() == empty {
            return;
        }
        if self.as_str() == empty {
            std::mem::swap(self, next);
            next.clear();
        } else {
            let cur_len = self.as_str().len();
            self.truncate(cur_len - 1);
            if needs_separator {
                self.push_char(',');
            }
            self.push_raw_str(&next.as_str()[1..]);
        }
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
