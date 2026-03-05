use proptest::prelude::*;
use rand::{SeedableRng, rngs::SmallRng};
use rustc_hash::FxHashMap;
use serde_json::{Value, json};
use time::format_description::well_known::Rfc3339;

use super::{TemplateConfig, TemplatedJson, config, json_string::JsonString};

// ── Helpers ────────────────────────────────────────────────────────────────

fn parse_generator(yaml: &str) -> config::Generator {
    serde_yaml::from_str(yaml).expect("failed to parse config::Generator from YAML")
}

fn run(vgen: &config::Generator, seed: u64) -> Value {
    run_with_defs(vgen, seed, &FxHashMap::default())
}

fn run_with_defs(
    vgen: &config::Generator,
    seed: u64,
    defs: &FxHashMap<String, config::Generator>,
) -> Value {
    let config = TemplateConfig {
        definitions: defs.clone(),
        generator: vgen.clone(),
    };
    let resolved = config.resolve().expect("resolution failed");
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut ctx = vec![None; resolved.num_vars];
    let mut result = JsonString::with_capacity(256);
    resolved
        .generator
        .generate(&mut rng, &mut ctx, &resolved.definitions, &mut result)
        .expect("generation failed");
    // Every generator must produce text that is valid JSON; callers that
    // inspect the output further rely on this invariant holding universally.
    serde_json::from_str(result.as_str())
        .unwrap_or_else(|e| panic!("generator output is not valid JSON: {e}\noutput: {result:?}"))
}

fn run_template(yaml: &str, seed: u64) -> Value {
    let config: TemplateConfig =
        serde_yaml::from_str(yaml).expect("failed to parse TemplateConfig");
    let mut tj = TemplatedJson::from_config(config).expect("from_config failed");
    let mut rng = SmallRng::seed_from_u64(seed);
    tj.generate_line(&mut rng).expect("generation failed");
    let result = tj.line_buf;
    serde_json::from_str::<Value>(result.as_str())
        .unwrap_or_else(|e| panic!("generator output is not valid JSON: {e}\noutput: {result:?}"))
}

// ── !const ─────────────────────────────────────────────────────────────────

proptest! {
    // !const is fully deterministic: RNG state must have no effect.
    #[test]
    fn const_output_is_rng_independent(seed1 in any::<u64>(), seed2 in any::<u64>()) {
        let vgen = parse_generator(r#"!const "hello""#);
        prop_assert_eq!(run(&vgen, seed1), run(&vgen, seed2));
    }

    #[test]
    fn const_integer_roundtrips(v in any::<i64>()) {
        let vgen = parse_generator(&format!("!const {v}"));
        prop_assert_eq!(run(&vgen, 0), json!(v));
    }

    #[test]
    fn const_bool_roundtrips(v in any::<bool>()) {
        let vgen = parse_generator(&format!("!const {v}"));
        prop_assert_eq!(run(&vgen, 0), json!(v));
    }
}

#[test]
fn const_null_produces_null() {
    let vgen = parse_generator("!const ~");
    assert_eq!(run(&vgen, 0), json!(null));
}

// ── !choose ────────────────────────────────────────────────────────────────

proptest! {
    // Every output must be one of the items supplied at construction time.
    #[test]
    fn choose_output_is_list_member(
        choices in prop::collection::vec("[a-z]{1,8}", 1..=8usize),
        seed in any::<u64>(),
    ) {
        let list = choices
            .iter()
            .map(|s| format!(r#""{s}""#))
            .collect::<Vec<_>>()
            .join(", ");
        let vgen = parse_generator(&format!("!choose [{list}]"));
        let val = run(&vgen, seed);
        let s = val.as_str().expect("not a JSON string");
        prop_assert!(choices.iter().any(|c| c == s), "{s:?} not in {choices:?}");
    }

    #[test]
    fn choose_single_item_always_returned(seed in any::<u64>()) {
        let vgen = parse_generator(r#"!choose ["only"]"#);
        prop_assert_eq!(run(&vgen, seed), json!("only"));
    }
}

// ── !range ─────────────────────────────────────────────────────────────────

proptest! {
    // min > max must be rejected at parse time so generation can never panic.
    #[test]
    fn range_rejects_inverted_bounds(
        min in 1i64..10_000i64,
        extra in 1i64..10_000i64,
    ) {
        let max = min - extra; // guaranteed < min
        let result = serde_yaml::from_str::<config::Generator>(
            &format!("!range {{ min: {min}, max: {max} }}")
        );
        prop_assert!(result.is_err(), "expected error for min={min} max={max}");
    }

    // The closed interval [min, max] must always contain the output.
    #[test]
    fn range_output_within_bounds(
        min in -10_000i64..10_000i64,
        extra in 0i64..10_000i64,
        seed in any::<u64>(),
    ) {
        let max = min + extra;
        let vgen = parse_generator(&format!("!range {{ min: {min}, max: {max} }}"));
        let v: i64 = run(&vgen, seed).as_i64().expect("not a JSON integer");
        prop_assert!(v >= min && v <= max, "{v} not in [{min}, {max}]");
    }

    // When the interval is a single point, the output is always that point.
    #[test]
    fn range_degenerate_interval_is_constant(v in any::<i64>(), seed in any::<u64>()) {
        let vgen = parse_generator(&format!("!range {{ min: {v}, max: {v} }}"));
        prop_assert_eq!(run(&vgen, seed), json!(v));
    }
}

// ── !weighted ──────────────────────────────────────────────────────────────

proptest! {
    // Every output must be one of the declared values, regardless of weight.
    #[test]
    fn weighted_output_is_declared_value(seed in any::<u64>()) {
        let vgen = parse_generator(
            "!weighted\n  - weight: 70\n    value: !const \"INFO\"\n  \
             - weight: 20\n    value: !const \"WARN\"\n  \
             - weight: 10\n    value: !const \"ERROR\"",
        );
        let out = run(&vgen, seed);
        prop_assert!(
            out == json!("INFO")
                || out == json!("WARN")
                || out == json!("ERROR"),
            "unexpected output: {out:?}"
        );
    }

    // A single-item list with any positive weight must always pick that item.
    #[test]
    fn weighted_single_item_always_selected(seed in any::<u64>()) {
        let vgen =
            parse_generator("!weighted\n  - weight: 1\n    value: !const \"only\"");
        prop_assert_eq!(run(&vgen, seed), json!("only"));
    }
}

// A 99:1 weighting should produce the heavy item the vast majority of the
// time across a broad sweep of seeds.
#[test]
fn weighted_heavy_item_dominates_distribution() {
    let vgen = parse_generator(
        "!weighted\n  - weight: 99\n    value: !const \"common\"\n  \
             - weight: 1\n    value: !const \"rare\"",
    );
    let n = 200u64;
    let common = (0..n)
        .filter(|&seed| run(&vgen, seed) == json!("common"))
        .count();
    assert!(common > 150, "expected >150/200 common, got {common}");
}

// ── !format ────────────────────────────────────────────────────────────────

proptest! {
    // Every {} placeholder must be consumed; none should survive in the output.
    #[test]
    fn format_consumes_all_placeholders(
        a in "[a-z]{1,6}",
        b in "[a-z]{1,6}",
        seed in any::<u64>(),
    ) {
        let yaml = format!(
            "!format\n  template: \"{{}}-{{}}\"\n  \
             args:\n    - !const \"{a}\"\n    - !const \"{b}\""
        );
        let vgen = parse_generator(&yaml);
        let val = run(&vgen, seed);
        let s = val.as_str().expect("not a JSON string");
        prop_assert!(!s.contains("{}"), "unfilled placeholder in {s:?}");
        prop_assert!(s.contains(a.as_str()), "first arg missing from {s:?}");
        prop_assert!(s.contains(b.as_str()), "second arg missing from {s:?}");
    }

    // An empty args list must leave a placeholder-free template unchanged.
    #[test]
    fn format_no_args_is_identity(template in "[a-z ]{1,20}", seed in any::<u64>()) {
        let yaml = format!("!format\n  template: \"{template}\"\n  args: []");
        let vgen = parse_generator(&yaml);
        prop_assert_eq!(run(&vgen, seed), json!(template));
    }

    // A number arg (from !range) must appear as its decimal digits in the output.
    #[test]
    fn format_number_arg_renders_as_digits(
        n in 0i64..100_000i64,
        seed in any::<u64>(),
    ) {
        let yaml = format!(
            "!format\n  template: \"req-{{}}\"\n  args:\n    - !range {{ min: {n}, max: {n} }}"
        );
        let vgen = parse_generator(&yaml);
        let val = run(&vgen, seed);
        prop_assert_eq!(val, json!(format!("req-{n}")));
    }

    // An object arg must be embedded as its compact JSON representation inside
    // the resulting string value (the common "stringify" pattern).
    #[test]
    fn format_object_arg_is_stringified(
        k in "[a-z]{1,6}",
        v in "[a-z]{1,6}",
        seed in any::<u64>(),
    ) {
        let yaml = format!(
            "!format\n  template: \"{{}}\"\n  \
             args:\n    - !object\n        {k}: !const \"{v}\""
        );
        let vgen = parse_generator(&yaml);
        let val = run(&vgen, seed);
        // Result must be a string containing the embedded object's JSON.
        let s = val.as_str().expect("not a JSON string");
        let inner: serde_json::Value =
            serde_json::from_str(s).expect("embedded object is not valid JSON");
        prop_assert_eq!(inner[k.as_str()].as_str().unwrap_or(""), v.as_str());
    }
}

// ── !object ────────────────────────────────────────────────────────────────

proptest! {
    // Every declared field must be present with the correct value.
    #[test]
    fn object_contains_expected_fields(
        v_str in "[a-z]{1,8}",
        v_int in 0i64..1000i64,
        seed in any::<u64>(),
    ) {
        let yaml =
            format!("!object\n  alpha: !const \"{v_str}\"\n  beta: !const {v_int}");
        let vgen = parse_generator(&yaml);
        let obj = run(&vgen, seed);
        prop_assert!(obj.is_object());
        prop_assert_eq!(obj["alpha"].as_str().unwrap(), v_str.as_str());
        prop_assert_eq!(obj["beta"].as_i64().unwrap(), v_int);
    }
}

// ── !with / !var ───────────────────────────────────────────────────────────

proptest! {
    // A variable bound by !with must be readable by !var inside the body.
    #[test]
    fn with_var_binding_visible_in_body(value in "[a-z]{1,12}", seed in any::<u64>()) {
        let yaml = format!("!with\n  bind:\n    x: !const \"{value}\"\n  in: !var x");
        let vgen = parse_generator(&yaml);
        prop_assert_eq!(run(&vgen, seed), json!(value));
    }

    // Bindings must be removed when the !with scope exits; two sibling !with
    // blocks using the same key must not observe each other's values.
    #[test]
    fn with_binding_does_not_outlive_scope(seed in any::<u64>()) {
        let vgen = parse_generator(
            "!object\n  \
               a:\n    \
                 !with\n      \
                   bind:\n        k: !const \"first\"\n      \
                   in: !var k\n  \
               b:\n    \
                 !with\n      \
                   bind:\n        k: !const \"second\"\n      \
                   in: !var k",
        );
        let obj = run(&vgen, seed);
        prop_assert_eq!(&obj["a"], &json!("first"));
        prop_assert_eq!(&obj["b"], &json!("second"));
    }
}

// ── !reference ─────────────────────────────────────────────────────────────

proptest! {
    // !reference must produce exactly what the named definition would produce.
    #[test]
    fn reference_delegates_to_definition(seed in any::<u64>()) {
        let mut defs = FxHashMap::default();
        defs.insert(
            "my_def".to_string(),
            parse_generator(r#"!const "defined""#),
        );
        let vgen = parse_generator("!reference my_def");
        prop_assert_eq!(run_with_defs(&vgen, seed, &defs), json!("defined"));
    }
}

// ── !timestamp ─────────────────────────────────────────────────────────────

// Output must be a syntactically valid RFC-3339 timestamp at whole-second
// precision (no fractional-second component).
#[test]
fn timestamp_output_parses_as_rfc3339() {
    let vgen = parse_generator("!timestamp");
    let val = run(&vgen, 0);
    let s = val.as_str().expect("not a JSON string");
    time::OffsetDateTime::parse(s, &Rfc3339).expect("not a valid RFC-3339 timestamp");
    // Whole-second precision: no fractional seconds in the string.
    assert!(!s.contains('.'), "unexpected sub-second component in {s:?}");
}

proptest! {
    // Same seed must produce the same timestamp value (determinism).
    #[test]
    fn timestamp_is_deterministic(seed in any::<u64>()) {
        let vgen = parse_generator("!timestamp");
        prop_assert_eq!(run(&vgen, seed), run(&vgen, seed));
    }

    // A single !timestamp node must produce a non-decreasing sequence across
    // consecutive generate_line calls on the same TemplatedJson instance.
    #[test]
    fn timestamp_advances_monotonically(seed in any::<u64>()) {
        let yaml = "
definitions: {}
generator: !timestamp
";
        let config: TemplateConfig =
            serde_yaml::from_str(yaml).expect("parse failed");
        let mut tj = TemplatedJson::from_config(config).expect("from_config failed");
        let mut rng = SmallRng::seed_from_u64(seed);

        tj.generate_line(&mut rng).expect("first generate failed");
        let s0 = tj.line_buf.as_str().trim_matches('"').to_string();
        let t0 = time::OffsetDateTime::parse(&s0, &Rfc3339)
            .expect("t0 not RFC-3339");

        tj.generate_line(&mut rng).expect("second generate failed");
        let s1 = tj.line_buf.as_str().trim_matches('"').to_string();
        let t1 = time::OffsetDateTime::parse(&s1, &Rfc3339)
            .expect("t1 not RFC-3339");

        prop_assert!(t1 >= t0, "second timestamp ({t1}) < first ({t0})");
    }
}

// ── Determinism / no-panics ────────────────────────────────────────────────

// A full template that exercises every tag through the complete TemplatedJson
// pipeline: definitions, !reference, !with/!var, !weighted, !choose, !range,
// !const, !format, !object, !timestamp.
const DETERMINISTIC_TEMPLATE: &str = r#"
definitions:
  pick: !choose ["x", "y", "z"]
  num:  !range { min: 0, max: 9 }

generator:
  !with
    bind:
      p: !reference pick
      n: !reference num
    in: !object
      const_str:  !const "fixed"
      const_int:  !const 99
      const_bool: !const false
      const_null: !const ~
      chosen:     !var p
      number:     !var n
      ts:         !timestamp
      weighted:
        !weighted
          - weight: 3
            value: !const "common"
          - weight: 1
            value: !const "rare"
      formatted:
        !format
          template: "{}-{}"
          args:
            - !var p
            - !var n
      nested_with:
        !with
          bind:
            inner: !reference pick
          in: !var inner
"#;

proptest! {
    // Lading's core requirement: identical seed must produce identical output.
    // This verifies the generator holds no cross-call mutable state and that
    // SmallRng is fully determined by its seed.
    #[test]
    fn same_seed_produces_same_output(seed in any::<u64>()) {
        prop_assert_eq!(
            run_template(DETERMINISTIC_TEMPLATE, seed),
            run_template(DETERMINISTIC_TEMPLATE, seed),
        );
    }
}
