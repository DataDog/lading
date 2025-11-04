//! In-memory writer for testing
//!
//! Provides a thread-safe in-memory buffer that implements Write for use in
//! tests and fuzzing.

use std::io::Write;
use std::sync::{Arc, Mutex};

/// In-memory writer for testing
///
/// This writer accumulates all written data in an in-memory buffer that can be
/// retrieved for assertions. Thread-safe through Arc<Mutex>.
#[derive(Clone, Debug)]
pub struct InMemoryWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl InMemoryWriter {
    /// Create a new in-memory writer
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the raw bytes written to the buffer
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned
    #[must_use]
    pub fn get_bytes(&self) -> Vec<u8> {
        self.buffer.lock().expect("mutex poisoned").clone()
    }

    /// Get the buffer contents as a UTF-8 string
    ///
    /// # Panics
    ///
    /// Panics if the buffer contains invalid UTF-8
    #[must_use]
    pub fn get_string(&self) -> String {
        String::from_utf8(self.get_bytes()).expect("buffer contains invalid UTF-8")
    }

    /// Parse the buffer contents as JSON lines
    ///
    /// # Errors
    ///
    /// Returns an error if any line cannot be parsed as JSON
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned
    pub fn parse_lines(&self) -> Result<Vec<crate::json::Line>, serde_json::Error> {
        let buffer = self.buffer.lock().expect("mutex poisoned");
        let content_str = String::from_utf8_lossy(&buffer);
        content_str
            .lines()
            .filter(|line| !line.is_empty())
            .map(serde_json::from_str)
            .collect()
    }
}

impl Default for InMemoryWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for InMemoryWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .expect("mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
