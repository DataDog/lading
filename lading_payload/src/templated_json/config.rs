use std::collections::BTreeMap;

use rustc_hash::FxHashMap;
use serde::Deserialize;
use serde_json::Value;

use super::JsonString;

// ── Template config ───────────────────────────────────────────────────────────

/// Parsed content of a template file.
///
/// This is the deserialized form of the YAML template file. `definitions` acts
/// as a named generator library referenced by `!reference` tags; `generator`
/// is the root generator that produces each record.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TemplateConfig {
    #[serde(default)]
    pub(super) definitions: FxHashMap<String, Generator>,
    pub(super) generator: Generator,
}

impl std::fmt::Debug for TemplateConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemplateConfig")
            .field("definitions_count", &self.definitions.len())
            .finish_non_exhaustive()
    }
}

// ── Generator ────────────────────────────────────────────────────────────────

/// A composable value generator, fully deserializable from YAML.
///
/// Each variant maps to a YAML tag (e.g. `!const`, `!choose`). Generators are
/// freely nestable; complex structures are built by composition.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Generator {
    /// Fixed JSON value.
    ///
    /// ```yaml
    /// !const "hello"
    /// !const 42
    /// !const null
    /// ```
    Const(#[serde(deserialize_with = "deserialize_const_value")] JsonString),

    /// Uniform random selection from a static list of JSON values.
    ///
    /// ```yaml
    /// !choose ["GET", "POST", "PUT", "DELETE"]
    /// ```
    Choose(#[serde(deserialize_with = "deserialize_choose_values")] Vec<JsonString>),

    /// Evaluate the named generator from the top-level `definitions` table.
    ///
    /// ```yaml
    /// !reference http_methods
    /// ```
    Reference(String),

    /// Weighted random selection.
    ///
    /// ```yaml
    /// !weighted
    ///   - weight: 70
    ///     value: !const "OK"
    ///   - weight: 30
    ///     value: !reference error_messages
    /// ```
    Weighted(WeightedList),

    /// Uniform random integer in the closed interval `[min, max]`.
    ///
    /// ```yaml
    /// !range { min: 1, max: 99 }
    /// ```
    Range(RangeSpec),

    /// String template with `{}` placeholders replaced by sub-generators.
    ///
    /// ```yaml
    /// !format
    ///   template: "host-{}-pod-{}"
    ///   args:
    ///     - !var service
    ///     - !range { min: 1, max: 99 }
    ///     - !range { min: 1, max: 99 }
    /// ```
    Format(FormatSpec),

    /// Build a JSON object; keys are fixed strings, values are generators.
    ///
    /// ```yaml
    /// !object
    ///   field1: !const "static"
    ///   field2: !range { min: 0, max: 9 }
    /// ```
    Object(ObjectFields),

    /// Bind multiple generated values into the context, then evaluate `in`.
    ///
    /// ```yaml
    /// !with
    ///   bind:
    ///     sev: !reference severity
    ///     svc: !reference services
    ///   in: !object
    ///     service: !var svc
    ///     severity: !var sev
    /// ```
    With(WithSpec),

    /// Look up a variable bound by an enclosing `!with`.
    ///
    /// ```yaml
    /// !var name
    /// ```
    Var(String),

    /// Deterministic monotonically-advancing UTC timestamp, formatted as
    /// RFC-3339 at whole-second precision.
    ///
    /// At first use the generator draws a random starting second from the RNG
    /// (in the range 2001-09-09 to 2033-05-18), then increments the value by
    /// a random number of milliseconds (1..=1000) on every subsequent call.
    /// Only the whole-second part is emitted, so sub-second increments
    /// accumulate silently until they cross a second boundary.
    ///
    /// Because the starting point and every increment are derived from the
    /// configured seed, output is fully reproducible.
    ///
    /// ```yaml
    /// !timestamp
    /// ```
    Timestamp,

    /// Generate a JSON array whose elements are drawn from a sub-generator.
    ///
    /// The number of elements is controlled by the `length` field, which
    /// accepts three forms:
    ///
    /// - A plain integer: every array has exactly that many elements.
    /// - A range mapping `{min: N, max: M}`: the count is drawn uniformly
    ///   from `[N, M]` (inclusive) on each call.
    /// - A sequence of integers `[N, M, ...]`: one length is chosen uniformly
    ///   at random from the list on each call.
    ///
    /// ```yaml
    /// # Fixed length
    /// !array
    ///   length: 3
    ///   element: !range { min: 1, max: 100 }
    ///
    /// # Uniform random length
    /// !array
    ///   length: { min: 1, max: 5 }
    ///   element: !const "hello"
    ///
    /// # Choose length from a set
    /// !array
    ///   length: [2, 3, 5]
    ///   element: !reference my_def
    /// ```
    Array(ArraySpec),
}

// ── Array types ───────────────────────────────────────────────────────────────

/// Determines how many elements a `!array` generator produces per call.
///
/// Deserializes from three YAML forms:
/// - scalar integer   → `Fixed`
/// - sequence         → `Choose`
/// - mapping          → `Range`
#[derive(Clone)]
pub(super) enum ArrayLength {
    /// Always produce exactly `n` elements.
    Fixed(usize),
    /// Draw the count uniformly from `[min, max]` inclusive.
    Range { min: usize, max: usize },
    /// Choose the count uniformly from the given list of values.
    Choose(Vec<usize>),
}

impl<'de> Deserialize<'de> for ArrayLength {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Untagged: integer -> Fixed, sequence -> Choose, mapping -> Range.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawArrayLength {
            Fixed(usize),
            Choose(Vec<usize>),
            Range { min: usize, max: usize },
        }
        match RawArrayLength::deserialize(deserializer)? {
            RawArrayLength::Fixed(n) => Ok(Self::Fixed(n)),
            RawArrayLength::Choose(choices) => {
                if choices.is_empty() {
                    return Err(<D::Error as serde::de::Error>::custom(
                        "!array length list must not be empty",
                    ));
                }
                Ok(Self::Choose(choices))
            }
            RawArrayLength::Range { min, max } => {
                if min > max {
                    return Err(<D::Error as serde::de::Error>::custom(format!(
                        "!array length range min ({min}) must be <= max ({max})"
                    )));
                }
                Ok(Self::Range { min, max })
            }
        }
    }
}

/// Configuration for a `!array` generator node.
#[derive(Clone, Deserialize)]
pub(super) struct ArraySpec {
    pub length: ArrayLength,
    pub element: Box<Generator>,
}

/// Return the JSON string body encoding of `s`: the bytes that would appear
/// between the outer quotation marks if `s` were serialized as a JSON string.
/// Called once at parse time when pre-compiling a `!format` template.
fn json_string_escape_content(s: &str) -> String {
    let encoded = serde_json::to_string(s).expect("string serialization cannot fail");
    encoded[1..encoded.len() - 1].to_string()
}

// ── Weighted items ────────────────────────────────────────────────────────────

#[derive(Clone, Deserialize)]
pub(super) struct WeightedItem {
    pub weight: u32,
    pub value: Generator,
}

#[derive(Clone)]
pub(super) struct WeightedList {
    pub items: Vec<WeightedItem>,
    pub total_weight: u32,
}

impl<'de> Deserialize<'de> for WeightedList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let items = Vec::<WeightedItem>::deserialize(deserializer)?;
        let mut total_weight = 0u32;
        for item in &items {
            total_weight = total_weight.checked_add(item.weight).ok_or_else(|| {
                <D::Error as serde::de::Error>::custom("weighted total exceeds u32::MAX")
            })?;
        }
        if total_weight == 0 {
            return Err(<D::Error as serde::de::Error>::custom(
                "weighted list must have total weight > 0",
            ));
        }
        Ok(Self {
            items,
            total_weight,
        })
    }
}

#[derive(Clone)]
pub(super) struct ObjectField {
    pub needs_comma: bool,
    pub key: JsonString,
    pub value: Generator,
}

#[derive(Clone)]
pub(super) struct ObjectFields(pub Vec<ObjectField>);

impl<'de> Deserialize<'de> for ObjectFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields = BTreeMap::<String, Generator>::deserialize(deserializer)?;
        let mut encoded_fields = Vec::with_capacity(fields.len());
        for (idx, (key, value)) in fields.into_iter().enumerate() {
            let mut key_json = JsonString::with_capacity(key.len() + 2);
            key_json
                .push_str_as_json(&key)
                .map_err(|e| <D::Error as serde::de::Error>::custom(e.to_string()))?;
            encoded_fields.push(ObjectField {
                needs_comma: idx != 0,
                key: key_json,
                value,
            });
        }
        Ok(Self(encoded_fields))
    }
}

// ── Supporting structs ────────────────────────────────────────────────────────

#[derive(Clone)]
pub(super) struct RangeSpec {
    pub min: i64,
    pub max: i64,
}

impl<'de> Deserialize<'de> for RangeSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawRangeSpec {
            min: i64,
            max: i64,
        }
        let raw = RawRangeSpec::deserialize(deserializer)?;
        if raw.min > raw.max {
            return Err(<D::Error as serde::de::Error>::custom(format!(
                "!range min ({}) must be <= max ({})",
                raw.min, raw.max
            )));
        }
        Ok(Self {
            min: raw.min,
            max: raw.max,
        })
    }
}

/// One `(literal, arg)` pair produced by pre-compiling a `!format` template.
#[derive(Clone)]
pub(super) struct FormatSegment {
    /// Literal text between two adjacent placeholders, pre-escaped for direct
    /// embedding inside a JSON string body (no surrounding quotes).
    pub literal_escaped: String,
    pub arg: Generator,
}

/// Pre-compiled `!format` template.
///
/// At deserialization time the template string is split on `{}` into N+1
/// literal segments and zipped with the N args. Each literal is pre-escaped
/// for embedding inside a JSON string so the hot path avoids any re-escaping.
/// Arity mismatches (wrong number of `{}` vs args) are rejected here rather
/// than at validation time.
#[derive(Clone)]
pub(super) struct FormatSpec {
    pub segments: Vec<FormatSegment>,
    /// Literal text after the final placeholder, pre-escaped.
    pub trailing_escaped: String,
}

impl<'de> Deserialize<'de> for FormatSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawFormatSpec {
            template: String,
            args: Vec<Generator>,
        }
        let raw = RawFormatSpec::deserialize(deserializer)?;

        // Split at each `{}` placeholder to get N+1 literal parts.
        let parts: Vec<&str> = raw.template.split("{}").collect();
        let placeholder_count = parts.len() - 1;
        if placeholder_count != raw.args.len() {
            return Err(<D::Error as serde::de::Error>::custom(format!(
                "!format template has {placeholder_count} placeholder(s) \
                 but {} arg(s) provided",
                raw.args.len()
            )));
        }

        let trailing_escaped =
            json_string_escape_content(parts.last().expect("split yields at least one part"));

        let segments = parts[..parts.len() - 1]
            .iter()
            .zip(raw.args)
            .map(|(literal, arg)| FormatSegment {
                literal_escaped: json_string_escape_content(literal),
                arg,
            })
            .collect();

        Ok(Self {
            segments,
            trailing_escaped,
        })
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WithSpec {
    pub bind: BTreeMap<String, Generator>,
    #[serde(rename = "in")]
    pub body: Box<Generator>,
}

// ── Deserialization helpers ───────────────────────────────────────────────────

fn deserialize_const_value<'de, D>(deserializer: D) -> Result<JsonString, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let mut encoded = JsonString::with_capacity(64);
    encoded
        .push_value(&value)
        .map_err(|e| <D::Error as serde::de::Error>::custom(e.to_string()))?;
    Ok(encoded)
}

fn deserialize_choose_values<'de, D>(deserializer: D) -> Result<Vec<JsonString>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = Vec::<Value>::deserialize(deserializer)?;
    if values.is_empty() {
        return Err(<D::Error as serde::de::Error>::custom(
            "!choose list must not be empty",
        ));
    }
    let mut encoded_values = Vec::with_capacity(values.len());
    for value in values {
        let mut encoded = JsonString::with_capacity(64);
        encoded
            .push_value(&value)
            .map_err(|e| <D::Error as serde::de::Error>::custom(e.to_string()))?;
        encoded_values.push(encoded);
    }
    Ok(encoded_values)
}
