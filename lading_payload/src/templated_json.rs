//! Templated JSON payload.
//!
//! Generates JSON records driven by a user-supplied YAML template file. The
//! template is parsed and validated once at construction time; the hot path
//! only fills in generated values and serializes them into the writer.
//!
//! Template files use YAML with custom tags to express the generation logic.
//! See [`config::Generator`] for the full tag vocabulary.

use std::{io::Write, path::Path};

use rand::Rng;

use crate::Error;

mod config;
mod generator;
mod json_string;
mod resolver;
#[cfg(test)]
mod tests;

use config::TemplateConfig;
use generator::Generator;
use json_string::JsonString;

// ── TemplatedJson serializer ──────────────────────────────────────────────────

/// Generates JSON records from a user-supplied YAML template file.
///
/// The template is loaded and validated at construction time. Each call to
/// `to_bytes` fills a block by generating records until the byte budget is
/// exhausted, using the `rng` threaded in from the block cache for
/// determinism.
///
/// After construction, all definition and variable names have been resolved to
/// compact integer indices, so the hot path uses direct `Vec` indexing instead
/// of hash-map lookups.
pub struct TemplatedJson {
    definitions: Vec<Generator>,
    generator: Generator,
    ctx: Vec<Option<JsonString>>,
    line_buf: JsonString,
}

impl std::fmt::Debug for TemplatedJson {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemplatedJson")
            .field("num_definitions", &self.definitions.len())
            .field("num_vars", &self.ctx.len())
            .finish_non_exhaustive()
    }
}

impl TemplatedJson {
    /// Build from a deserialized template config, validating and resolving
    /// names to indices.
    ///
    /// # Errors
    ///
    /// Returns an error if validation or name resolution fails.
    fn from_config(config: TemplateConfig) -> Result<Self, Error> {
        let resolved = config.resolve()?;
        Ok(Self {
            definitions: resolved.definitions,
            generator: resolved.generator,
            ctx: vec![None; resolved.num_vars],
            line_buf: JsonString::with_capacity(1024),
        })
    }

    /// Load and parse a template file from `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or if the YAML content
    /// cannot be deserialized into a valid template.
    pub fn from_path(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path)?;
        let config: TemplateConfig = serde_yaml::from_str(&text)?;
        Self::from_config(config)
    }

    /// Generate a single JSON record into `self.line_buf`.
    ///
    /// Context slots are reset to `None` and the line buffer is cleared before
    /// each record so their allocated capacity is retained across calls.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying generator fails.
    fn generate_line(&mut self, rng: &mut impl Rng) -> Result<(), Error> {
        self.ctx.fill(None);
        self.line_buf.clear();
        self.generator
            .generate(rng, &mut self.ctx, &self.definitions, &mut self.line_buf)
    }
}

impl crate::Serialize for TemplatedJson {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            self.generate_line(&mut rng)?;
            let line_length = self.line_buf.as_str().len() + 1; // +1 for the trailing newline
            let Some(remainder) = bytes_remaining.checked_sub(line_length) else {
                break;
            };
            self.line_buf.push_char('\n');
            writer.write_all(self.line_buf.as_str().as_bytes())?;
            bytes_remaining = remainder;
        }
        Ok(())
    }
}
