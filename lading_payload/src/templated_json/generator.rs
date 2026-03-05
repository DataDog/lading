use std::cell::{Cell, RefCell};
use std::io::Write;
use std::num::NonZeroI64;

use rand::Rng;
use rand::prelude::IndexedRandom;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::JsonString;
use crate::Error;

// ── Context ───────────────────────────────────────────────────────────────────

/// Variable bindings threaded through a single record-generation pass.
///
/// Slots are indexed by the variable id assigned during name resolution.
/// `None` means the variable is not currently bound.
pub(super) type Context = Vec<Option<JsonString>>;

// ── Timestamp constants ───────────────────────────────────────────────────────

/// Earliest allowed starting Unix second: 2001-09-09 01:46:40 UTC.
const TIMESTAMP_BASE_SECS_MIN: i64 = 1_000_000_000;
/// Latest allowed starting Unix second: 2033-05-18 03:33:20 UTC.
const TIMESTAMP_BASE_SECS_MAX: i64 = 2_000_000_000;
/// Milliseconds per second, used to convert between the two units.
const MS_PER_SEC: i64 = 1_000;
/// Maximum random millisecond increment applied on each generate call.
const TIMESTAMP_DELTA_MS_MAX: i64 = 1_000;

// ── Resolved generator types ──────────────────────────────────────────────────

/// Resolved value generator with all names replaced by integer indices.
///
/// This is the hot-path representation produced by the resolver from the
/// deserialized (string-based) [`super::config::Generator`].
pub(super) enum Generator {
    Const(JsonString),
    Choose(Vec<JsonString>),
    /// Index into the definitions vec.
    Reference(usize),
    Weighted(WeightedList),
    Range(RangeSpec),
    Format(FormatSpec),
    Object(ObjectFields),
    With(WithSpec),
    /// Index into the context vec.
    Var(usize),
    Timestamp(Timestamp),
    Array(ArraySpec),
}

pub(super) struct RangeSpec {
    pub(super) min: i64,
    pub(super) max: i64,
}

pub(super) struct WeightedItem {
    pub(super) weight: u32,
    pub(super) value: Generator,
}

pub(super) struct WeightedList {
    pub(super) items: Vec<WeightedItem>,
    pub(super) total_weight: u32,
}

pub(super) struct ObjectField {
    pub(super) needs_comma: bool,
    pub(super) key: JsonString,
    pub(super) value: Generator,
}

pub(super) struct ObjectFields(pub(super) Vec<ObjectField>);

pub(super) struct FormatSegment {
    pub(super) literal_escaped: String,
    pub(super) arg: Generator,
}

pub(super) struct FormatSpec {
    pub(super) segments: Vec<FormatSegment>,
    pub(super) trailing_escaped: String,
}

pub(super) struct WithSpec {
    pub(super) bind: Vec<(usize, Generator)>,
    pub(super) body: Box<Generator>,
    /// One pre-allocated `JsonString` per binding, reused across calls.
    /// Each is written to, swapped into the context slot, then recovered
    /// after the body runs so no per-call heap allocation is needed.
    scratch: RefCell<Vec<JsonString>>,
}

impl WithSpec {
    pub(super) fn new(bind: Vec<(usize, Generator)>, body: Box<Generator>) -> Self {
        let n = bind.len();
        Self {
            bind,
            body,
            scratch: RefCell::new((0..n).map(|_| JsonString::with_capacity(64)).collect()),
        }
    }
}

/// How many elements a `!array` generator produces per call.
pub(super) enum ArrayLength {
    Fixed(usize),
    Range { min: usize, max: usize },
    Choose(Vec<usize>),
}

/// Resolved `!array` generator node.
pub(super) struct ArraySpec {
    pub(super) length: ArrayLength,
    pub(super) element: Box<Generator>,
}

/// State for a single `!timestamp` generator node.
///
/// Wraps a `Cell<Option<NonZeroI64>>` holding the current value in
/// milliseconds since the Unix epoch. `None` is the uninitialized state;
/// the first call to `generate` draws a random starting second from the RNG
/// and every call adds a random 1..=1000 ms increment before emitting the
/// whole-second portion.
pub(super) struct Timestamp(Cell<Option<NonZeroI64>>);

impl Timestamp {
    /// Create a new, uninitialized timestamp state.
    pub(super) fn new() -> Self {
        Self(Cell::new(None))
    }

    /// Advance the timestamp and write it as a quoted RFC-3339 string into `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if the computed Unix second is out of range for
    /// `OffsetDateTime::from_unix_timestamp` or if formatting fails.
    pub(super) fn generate(&self, rng: &mut impl Rng, out: &mut JsonString) -> Result<(), Error> {
        // First call: draw a random starting second in the range
        // [2001-09-09 01:46:40, 2033-05-18 03:33:20] (Unix seconds),
        // then convert to milliseconds.
        let base_ms: i64 = self.0.get().map_or_else(
            || {
                rng.random_range(TIMESTAMP_BASE_SECS_MIN..=TIMESTAMP_BASE_SECS_MAX)
                    .saturating_mul(MS_PER_SEC)
            },
            NonZeroI64::get,
        );
        // Advance by a random number of milliseconds on every call.
        let delta_ms = rng.random_range(1..=TIMESTAMP_DELTA_MS_MAX);
        let new_ms = base_ms.saturating_add(delta_ms);
        // base_ms >= TIMESTAMP_BASE_SECS_MIN * MS_PER_SEC and delta_ms >= 1,
        // so new_ms is always nonzero; the error branch is unreachable in practice.
        let new_nz = NonZeroI64::new(new_ms).expect("operations above guarantee nonzero");
        self.0.set(Some(new_nz));
        // Emit only the whole-second part as RFC-3339.
        let dt = OffsetDateTime::from_unix_timestamp(new_ms / MS_PER_SEC)
            .map_err(|e| Error::TemplateError(e.to_string()))?;
        out.push_char('"');
        dt.format_into(out, &Rfc3339)
            .map_err(|e| Error::TemplateError(e.to_string()))?;
        out.push_char('"');
        Ok(())
    }
}

// ── generate() ───────────────────────────────────────────────────────────────

impl Generator {
    pub(super) fn generate(
        &self,
        rng: &mut impl Rng,
        ctx: &mut Context,
        defs: &[Generator],
        out: &mut JsonString,
    ) -> Result<(), Error> {
        match self {
            Self::Const(v) => out.push_raw_str(v.as_str()),

            Self::Choose(values) => {
                out.push_raw_str(
                    values
                        .choose(rng)
                        .ok_or_else(|| Error::TemplateError("!choose list is empty".to_string()))?
                        .as_str(),
                );
            }

            Self::Reference(idx) => {
                defs[*idx].generate(rng, ctx, defs, out)?;
            }

            Self::Weighted(list) => list.generate(rng, ctx, defs, out)?,

            Self::Range(r) => {
                let n = rng.random_range(r.min..=r.max);
                write!(out, "{n}").map_err(|e| Error::TemplateError(e.to_string()))?;
            }

            Self::Format(f) => f.generate(rng, ctx, defs, out)?,

            Self::Object(fields) => fields.generate(rng, ctx, defs, out)?,

            Self::With(spec) => spec.generate(rng, ctx, defs, out)?,

            Self::Var(idx) => {
                let value = ctx[*idx].as_ref().ok_or_else(|| {
                    Error::TemplateError(format!("unbound variable at index {idx}"))
                })?;
                out.push_raw_str(value.as_str());
            }

            Self::Timestamp(ts) => ts.generate(rng, out)?,

            Self::Array(spec) => spec.generate(rng, ctx, defs, out)?,
        }
        Ok(())
    }
}

impl WeightedList {
    /// Select one item by weight and generate its value into `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if the selected item's generator fails.
    pub(super) fn generate(
        &self,
        rng: &mut impl Rng,
        ctx: &mut Context,
        defs: &[Generator],
        out: &mut JsonString,
    ) -> Result<(), Error> {
        let mut n = rng.random_range(0..self.total_weight);
        for item in &self.items {
            if n < item.weight {
                return item.value.generate(rng, ctx, defs, out);
            }
            n -= item.weight;
        }
        Err(Error::TemplateError(
            "weighted selection exhausted without match".to_string(),
        ))
    }
}

impl ObjectFields {
    /// Generate a JSON object from the pre-encoded fields into `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if any field's value generator fails.
    pub(super) fn generate(
        &self,
        rng: &mut impl Rng,
        ctx: &mut Context,
        defs: &[Generator],
        out: &mut JsonString,
    ) -> Result<(), Error> {
        out.push_char('{');
        for field in &self.0 {
            if field.needs_comma {
                out.push_char(',');
            }
            out.push_raw_str(field.key.as_str());
            out.push_char(':');
            field.value.generate(rng, ctx, defs, out)?;
        }
        out.push_char('}');
        Ok(())
    }
}

impl FormatSpec {
    /// Generate a JSON string value from this pre-compiled template into `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if any argument generator fails.
    pub(super) fn generate(
        &self,
        rng: &mut impl Rng,
        ctx: &mut Context,
        defs: &[Generator],
        out: &mut JsonString,
    ) -> Result<(), Error> {
        let mut arg_buf = JsonString::with_capacity(64);
        out.push_char('"');
        for segment in &self.segments {
            out.push_raw_str(&segment.literal_escaped);
            arg_buf.clear();
            segment.arg.generate(rng, ctx, defs, &mut arg_buf)?;
            let s = arg_buf.as_str();
            let b = s.as_bytes();
            if b.len() >= 2 && b[0] == b'"' && b[b.len() - 1] == b'"' {
                out.push_raw_str(&s[1..s.len() - 1]);
            } else {
                out.push_str_json_escaped(s);
            }
        }
        out.push_raw_str(&self.trailing_escaped);
        out.push_char('"');
        Ok(())
    }
}

impl ArraySpec {
    /// Generate a JSON array into `out`.
    ///
    /// The element count is determined by `self.length`; then `self.element`
    /// is called once per slot.
    ///
    /// # Errors
    ///
    /// Returns an error if the length list is empty or if any element
    /// generator fails.
    pub(super) fn generate(
        &self,
        rng: &mut impl Rng,
        ctx: &mut Context,
        defs: &[Generator],
        out: &mut JsonString,
    ) -> Result<(), Error> {
        let count = self.length.choose(rng)?;
        out.push_char('[');
        for i in 0..count {
            if i > 0 {
                out.push_char(',');
            }
            self.element.generate(rng, ctx, defs, out)?;
        }
        out.push_char(']');
        Ok(())
    }
}

impl ArrayLength {
    fn choose(&self, rng: &mut impl Rng) -> Result<usize, Error> {
        Ok(match self {
            Self::Fixed(n) => *n,
            Self::Range { min, max } => rng.random_range(*min..=*max),
            Self::Choose(choices) => *choices
                .choose(rng)
                .ok_or_else(|| Error::TemplateError("!array length list is empty".to_string()))?,
        })
    }
}

impl WithSpec {
    /// Evaluate this `!with` block into `out`.
    ///
    /// Each binding owns a dedicated context slot (allocated fresh by the resolver, never shared
    /// with parent scopes). The scratch buffer is moved into the slot for the body to read via
    /// `!var`, then recovered afterwards so its heap allocation survives across calls.
    ///
    /// The generator tree is acyclic, so this method is never called re-entrantly on the same
    /// `WithSpec` instance; the `RefCell` borrow therefore never panics.
    ///
    /// # Errors
    ///
    /// Returns an error if any binding generator or the body generator fails.
    pub(super) fn generate(
        &self,
        rng: &mut impl Rng,
        ctx: &mut Context,
        defs: &[Generator],
        out: &mut JsonString,
    ) -> Result<(), Error> {
        let mut scratch = self.scratch.borrow_mut();
        for (i, (idx, binding)) in self.bind.iter().enumerate() {
            scratch[i].clear();
            binding.generate(rng, ctx, defs, &mut scratch[i])?;
            ctx[*idx] = Some(std::mem::take(&mut scratch[i]));
        }
        let result = self.body.generate(rng, ctx, defs, out);
        for (i, (idx, _)) in self.bind.iter().enumerate() {
            scratch[i] = ctx[*idx].take().unwrap_or_default();
        }
        result
    }
}
