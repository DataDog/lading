use rustc_hash::FxHashMap;

use super::config::{ArraySpec, ConcatSpec, FormatSpec, ObjectFields, WeightedList, WithSpec};
use super::{TemplateConfig, config, generator};
use crate::Error;

// ── Name table ────────────────────────────────────────────────────────────────

/// Bidirectional map from string names to compact integer indices.
///
/// Separate index spaces are used for definition names and variable names
/// so the two namespaces cannot collide.
struct NameTable {
    defs: FxHashMap<String, usize>,
    next_var: usize,
}

impl NameTable {
    fn new() -> Self {
        Self {
            defs: FxHashMap::default(),
            next_var: 0,
        }
    }

    fn intern_def(&mut self, name: &str) -> usize {
        let next = self.defs.len();
        *self.defs.entry(name.to_string()).or_insert(next)
    }

    /// Allocate a fresh variable slot. Every `!with` binding gets its own
    /// slot so that shadowing never reuses a parent's context index.
    fn alloc_var(&mut self) -> usize {
        let idx = self.next_var;
        self.next_var += 1;
        idx
    }

    fn lookup_def(&self, name: &str) -> Option<usize> {
        self.defs.get(name).copied()
    }

    fn num_vars(&self) -> usize {
        self.next_var
    }
}

// ── Public interface ──────────────────────────────────────────────────────────

/// Output of the resolution pass.
pub(super) struct ResolvedConfig {
    pub definitions: Vec<generator::Generator>,
    pub generator: generator::Generator,
    pub num_vars: usize,
}

impl TemplateConfig {
    /// Validate and resolve this template config in a single pass.
    ///
    /// Walks the generator tree once, simultaneously:
    /// - detecting cyclic `!reference` chains (depth-first search grey/black marking),
    /// - verifying that every `!var` is bound by an enclosing `!with`,
    /// - interning all definition and variable names as compact indices, and
    /// - building the index-based generator tree used by the hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - a `!reference` names an undefined definition,
    /// - a `!reference` chain contains a cycle,
    /// - a `!var` refers to a name not bound by any enclosing `!with`.
    pub(super) fn resolve(self) -> Result<ResolvedConfig, Error> {
        let mut r = Resolver::new(self.definitions);

        // Resolve every definition via depth-first search (detects cycles).
        let num_defs = r.raw_defs.len();
        for idx in 0..num_defs {
            if !r.black[idx] {
                r.resolve_def(idx)?;
            }
        }

        let resolved_root = self.generator.resolve(&mut r)?;

        let resolved_defs = r
            .resolved_defs
            .into_iter()
            .map(|opt| {
                opt.ok_or_else(|| Error::TemplateError("missing resolved definition".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResolvedConfig {
            definitions: resolved_defs,
            generator: resolved_root,
            num_vars: r.table.num_vars(),
        })
    }
}

// ── Resolver ──────────────────────────────────────────────────────────────────

struct Resolver {
    table: NameTable,
    def_names: Vec<String>,
    raw_defs: Vec<Option<config::Generator>>,
    resolved_defs: Vec<Option<generator::Generator>>,
    /// Currently on the depth-first search stack (back-edge = cycle).
    grey: Vec<bool>,
    /// Fully explored and resolved.
    black: Vec<bool>,
    /// Variable bindings in scope. Each entry is `(name, slot_index)` where
    /// `slot_index` was freshly allocated by `NameTable::alloc_var`. Searched
    /// from the back so inner bindings shadow outer ones with the same name.
    scope: Vec<(String, usize)>,
}

impl Resolver {
    /// Build a resolver pre-loaded with the named definitions.
    fn new(definitions: FxHashMap<String, config::Generator>) -> Self {
        let num_defs = definitions.len();
        let mut table = NameTable::new();
        let mut def_names: Vec<String> = vec![String::new(); num_defs];
        let mut raw_defs: Vec<Option<config::Generator>> = (0..num_defs).map(|_| None).collect();
        for (name, vgen) in definitions {
            let idx = table.intern_def(&name);
            def_names[idx] = name;
            raw_defs[idx] = Some(vgen);
        }
        Self {
            table,
            def_names,
            raw_defs,
            resolved_defs: (0..num_defs).map(|_| None).collect(),
            grey: vec![false; num_defs],
            black: vec![false; num_defs],
            scope: Vec::new(),
        }
    }

    /// Resolve a definition by index, following `!reference` chains depth-first.
    fn resolve_def(&mut self, idx: usize) -> Result<(), Error> {
        if self.black[idx] {
            return Ok(());
        }
        if self.grey[idx] {
            return Err(Error::TemplateError(format!(
                "cyclic !reference: \"{}\" is part of a definition cycle",
                self.def_names[idx]
            )));
        }
        self.grey[idx] = true;
        let vgen = self.raw_defs[idx]
            .take()
            .expect("raw definition should exist");
        let resolved = vgen.resolve(self)?;
        self.resolved_defs[idx] = Some(resolved);
        self.grey[idx] = false;
        self.black[idx] = true;
        Ok(())
    }
}

impl config::Generator {
    /// Resolve this raw generator node into its indexed counterpart.
    ///
    /// Validates `!reference` (existence and cycles) and `!var` (lexical scope)
    /// as it walks, interning names on the fly.
    ///
    /// # Errors
    ///
    /// Returns an error if any `!reference` or `!var` cannot be resolved.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        Ok(match self {
            Self::Const(v) => generator::Generator::Const(v),
            Self::Choose(values) => generator::Generator::Choose(values),

            Self::Reference(name) => {
                let idx = resolver.table.lookup_def(&name).ok_or_else(|| {
                    Error::TemplateError(format!("unknown !reference: \"{name}\" is not defined"))
                })?;
                if !resolver.black[idx] {
                    resolver.resolve_def(idx)?;
                }
                generator::Generator::Reference(idx)
            }

            Self::Var(name) => {
                let idx = resolver
                    .scope
                    .iter()
                    .rev()
                    .find(|(n, _)| n == &name)
                    .map(|(_, idx)| *idx)
                    .ok_or_else(|| {
                        Error::TemplateError(format!(
                            "!var \"{name}\" is not bound by any enclosing !with"
                        ))
                    })?;
                generator::Generator::Var(idx)
            }

            Self::With(spec) => spec.resolve(resolver)?,
            Self::Weighted(list) => list.resolve(resolver)?,
            Self::Range(spec) => generator::Generator::Range(generator::RangeSpec {
                min: spec.min,
                max: spec.max,
            }),
            Self::Format(f) => f.resolve(resolver)?,
            Self::Object(fields) => fields.resolve(resolver)?,
            Self::Timestamp => generator::Generator::Timestamp(generator::Timestamp::new()),
            Self::Array(spec) => spec.resolve(resolver)?,
            Self::Concat(spec) => spec.resolve(resolver)?,
        })
    }
}

impl WeightedList {
    /// Resolve each item's value generator into its indexed counterpart.
    ///
    /// # Errors
    ///
    /// Returns an error if any item's value generator fails to resolve.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        let items = self
            .items
            .into_iter()
            .map(|item| {
                Ok(generator::WeightedItem {
                    weight: item.weight,
                    value: item.value.resolve(resolver)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(generator::Generator::Weighted(generator::WeightedList {
            items,
            total_weight: self.total_weight,
        }))
    }
}

impl FormatSpec {
    /// Resolve each argument generator into its indexed counterpart.
    ///
    /// # Errors
    ///
    /// Returns an error if any argument generator fails to resolve.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        let segments = self
            .segments
            .into_iter()
            .map(|seg| {
                Ok(generator::FormatSegment {
                    literal_escaped: seg.literal_escaped,
                    arg: seg.arg.resolve(resolver)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(generator::Generator::Format(generator::FormatSpec {
            segments,
            trailing_escaped: self.trailing_escaped,
        }))
    }
}

impl ObjectFields {
    /// Resolve each field's value generator into its indexed counterpart.
    ///
    /// # Errors
    ///
    /// Returns an error if any field's value generator fails to resolve.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        let fields = self
            .0
            .into_iter()
            .map(|field| {
                Ok(generator::ObjectField {
                    needs_comma: field.needs_comma,
                    key: field.key,
                    value: field.value.resolve(resolver)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(generator::Generator::Object(generator::ObjectFields(
            fields,
        )))
    }
}

impl WithSpec {
    /// Validate and resolve this `!with` node into its indexed counterpart.
    ///
    /// Bind values are resolved in the outer scope (not visible to sibling
    /// bindings). Variable names are interned and pushed into scope for the
    /// duration of the body resolution, then popped on return.
    ///
    /// # Errors
    ///
    /// Returns an error if any bind value generator or the body generator
    /// fails to resolve.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        // Resolve bind values in the outer scope. Each binding gets a
        // freshly allocated slot so shadowing never reuses a parent slot.
        let mut bind = Vec::with_capacity(self.bind.len());
        let mut names = Vec::with_capacity(self.bind.len());
        for (name, binding_vgen) in self.bind {
            let idx = resolver.table.alloc_var();
            let resolved_binding = binding_vgen.resolve(resolver)?;
            bind.push((idx, resolved_binding));
            names.push(name);
        }
        // Push all bound names into scope for the body.
        let prev_len = resolver.scope.len();
        for (name, &(idx, _)) in names.into_iter().zip(bind.iter()) {
            resolver.scope.push((name, idx));
        }
        let body = Box::new((*self.body).resolve(resolver)?);
        resolver.scope.truncate(prev_len);
        Ok(generator::Generator::With(generator::WithSpec::new(
            bind, body,
        )))
    }
}

impl ArraySpec {
    /// Resolve the element sub-generator into its indexed counterpart.
    ///
    /// The `length` field contains only plain integers and requires no
    /// name resolution; it is copied verbatim into the generator type.
    ///
    /// # Errors
    ///
    /// Returns an error if the element generator fails to resolve.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        let length = match self.length {
            config::ArrayLength::Fixed(n) => generator::ArrayLength::Fixed(n),
            config::ArrayLength::Range { min, max } => generator::ArrayLength::Range { min, max },
            config::ArrayLength::Choose(choices) => generator::ArrayLength::Choose(choices),
        };
        let element = Box::new((*self.element).resolve(resolver)?);
        Ok(generator::Generator::Array(generator::ArraySpec {
            length,
            element,
        }))
    }
}

impl ConcatSpec {
    /// Resolve each part generator into its indexed counterpart.
    ///
    /// # Errors
    ///
    /// Returns an error if any part generator fails to resolve.
    fn resolve(self, resolver: &mut Resolver) -> Result<generator::Generator, Error> {
        let parts = self
            .0
            .into_iter()
            .map(|p| p.resolve(resolver))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(generator::Generator::Concat(generator::ConcatSpec::new(
            parts,
        )))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rustc_hash::FxHashMap;

    use super::super::TemplateConfig;
    use super::super::config;
    use crate::Error;

    const CONST_ROOT: &str = r#"!const "root""#;

    fn parse_generator(yaml: &str) -> config::Generator {
        serde_yaml::from_str(yaml).expect("failed to parse config::Generator from YAML")
    }

    fn make_defs(pairs: &[(&str, &str)]) -> FxHashMap<String, config::Generator> {
        pairs
            .iter()
            .map(|&(name, yaml)| (name.to_string(), parse_generator(yaml)))
            .collect()
    }

    fn resolve_generator(yaml: &str) -> Result<(), Error> {
        let config = TemplateConfig {
            definitions: FxHashMap::default(),
            generator: parse_generator(yaml),
        };
        config.resolve().map(|_| ())
    }

    fn resolve_template(defs_yaml: &[(&str, &str)], root_yaml: &str) -> Result<(), Error> {
        let config = TemplateConfig {
            definitions: make_defs(defs_yaml),
            generator: parse_generator(root_yaml),
        };
        config.resolve().map(|_| ())
    }

    // ── !var / !with ───────────────────────────────────────────────────────

    #[test]
    fn var_inside_with_body_is_accepted() {
        assert!(
            resolve_generator(
                "
!with
  bind:
    x: !const 1
  in: !var x
"
            )
            .is_ok()
        );
    }

    #[test]
    fn var_outside_with_is_rejected() {
        assert!(resolve_generator("!var x").is_err());
    }

    #[test]
    fn var_not_in_enclosing_with_is_rejected() {
        assert!(
            resolve_generator(
                "
!with
  bind:
    x: !const 1
  in: !var y
"
            )
            .is_err()
        );
    }

    #[test]
    fn nested_with_inner_var_is_accepted() {
        assert!(
            resolve_generator(
                r#"
!with
  bind:
    outer: !const "a"
  in: !with
    bind:
      inner: !const "b"
    in: !format
      template: "{}-{}"
      args:
        - !var outer
        - !var inner
"#
            )
            .is_ok()
        );
    }

    #[test]
    fn var_in_with_bind_value_is_rejected() {
        // Bind values are evaluated in the outer scope, so !var x is unbound
        // here even though x is a sibling binding.
        assert!(
            resolve_generator(
                "
!with
  bind:
    x: !const 1
    y: !var x
  in: !var y
"
            )
            .is_err()
        );
    }

    // ── !reference cycles ──────────────────────────────────────────────────

    #[test]
    fn unknown_reference_is_rejected() {
        assert!(resolve_template(&[("a", "!reference nonexistent")], CONST_ROOT).is_err());
    }

    #[test]
    fn self_referencing_definition_is_rejected() {
        assert!(resolve_template(&[("a", "!reference a")], CONST_ROOT).is_err());
    }

    #[test]
    fn mutual_cycle_is_rejected() {
        assert!(
            resolve_template(&[("a", "!reference b"), ("b", "!reference a")], CONST_ROOT).is_err()
        );
    }

    #[test]
    fn longer_cycle_is_rejected() {
        assert!(
            resolve_template(
                &[
                    ("a", "!reference b"),
                    ("b", "!reference c"),
                    ("c", "!reference a"),
                ],
                CONST_ROOT
            )
            .is_err()
        );
    }

    proptest! {
        // A linear chain a0 -> a1 -> ... -> aN (DAG) must always be accepted.
        #[test]
        fn acyclic_chain_is_accepted(depth in 1usize..8) {
            let pairs: Vec<(String, String)> = (0..depth)
                .map(|i| {
                    let name = format!("a{i}");
                    let yaml = format!("!reference a{}", i + 1);
                    (name, yaml)
                })
                .chain(std::iter::once((
                    format!("a{depth}"),
                    r#"!const "leaf""#.to_string(),
                )))
                .collect();
            let config = TemplateConfig {
                definitions: pairs
                    .iter()
                    .map(|(n, y)| (n.clone(), parse_generator(y)))
                    .collect(),
                generator: parse_generator(CONST_ROOT),
            };
            prop_assert!(config.resolve().is_ok());
        }
    }

    // ── root generator is validated ────────────────────────────────────────

    #[test]
    fn root_unknown_reference_is_rejected() {
        assert!(resolve_template(&[], "!reference missing").is_err());
    }

    #[test]
    fn root_unbound_var_is_rejected() {
        assert!(resolve_template(&[], "!var x").is_err());
    }

    #[test]
    fn root_format_arity_mismatch_is_rejected() {
        let yaml = r#"
definitions: {}
generator: !format
  template: "{}-{}"
  args:
    - !const "a"
"#;
        assert!(serde_yaml::from_str::<TemplateConfig>(yaml).is_err());
    }

    #[test]
    fn root_valid_reference_is_accepted() {
        assert!(
            resolve_template(&[("greeting", r#"!const "hello""#)], "!reference greeting").is_ok()
        );
    }

    // ── !format placeholder/arg mismatch ───────────────────────────────────

    #[test]
    fn format_too_few_args_is_rejected() {
        assert!(
            serde_yaml::from_str::<config::Generator>(
                r#"
!format
  template: "{}-{}"
  args:
    - !const "a"
"#
            )
            .is_err()
        );
    }

    #[test]
    fn format_too_many_args_is_rejected() {
        assert!(
            serde_yaml::from_str::<config::Generator>(
                r#"
!format
  template: "{}"
  args:
    - !const "a"
    - !const "b"
"#
            )
            .is_err()
        );
    }

    #[test]
    fn format_matching_args_is_accepted() {
        assert!(
            resolve_generator(
                r#"
!format
  template: "{}-{}"
  args:
    - !const "a"
    - !const "b"
"#
            )
            .is_ok()
        );
    }

    #[test]
    fn format_zero_placeholders_zero_args_is_accepted() {
        assert!(
            resolve_generator(
                r#"
!format
  template: "static"
  args: []
"#
            )
            .is_ok()
        );
    }
}
