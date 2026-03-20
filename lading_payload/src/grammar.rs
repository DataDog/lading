//! Grammar-based payload generation via [barkus](https://github.com/DataDog/barkus).
//!
//! This module wraps barkus — a structure-aware grammar-based data generator —
//! as a lading payload type. It accepts EBNF, PEG, or ANTLR v4 grammar files
//! and generates conforming structured output.

use std::{io::Write, path::PathBuf};

use rand::Rng;
use rand_0_8::SeedableRng;
use serde::Deserialize;
use tracing::warn;

use crate::Error;

/// Maximum number of consecutive generation failures (empty output or budget
/// exhaustion) before we stop retrying and return what we have so far.
const MAX_CONSECUTIVE_FAILURES: u32 = 1_000;

/// Grammar format understood by the parser.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum GrammarFormat {
    /// ISO/IEC 14977 Extended Backus-Naur Form
    Ebnf,
    /// Parsing Expression Grammar
    Peg,
    /// ANTLR v4 combined or parser grammar
    Antlr,
}

/// Default max recursion depth for grammar generation.
const fn default_max_depth() -> Option<u32> {
    Some(30)
}

/// Configuration for the grammar-based payload generator.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Config {
    /// Path to the grammar file (.ebnf, .peg, .g4).
    pub grammar_path: PathBuf,
    /// Grammar format.
    pub format: GrammarFormat,
    /// Max recursion depth (default 30).
    #[serde(default = "default_max_depth")]
    pub max_depth: Option<u32>,
    /// Max total AST nodes per sample (default 20000).
    #[serde(default)]
    pub max_total_nodes: Option<u32>,
}

/// Grammar-based payload generator backed by barkus.
pub struct Grammar {
    grammar_ir: barkus_core::ir::GrammarIr,
    profile: barkus_core::profile::Profile,
}

impl std::fmt::Debug for Grammar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grammar")
            .field("start", &self.grammar_ir.start)
            .field("num_productions", &self.grammar_ir.productions.len())
            .finish_non_exhaustive()
    }
}

impl Grammar {
    /// Construct a new grammar payload generator from configuration.
    ///
    /// Reads and compiles the grammar file, then builds a generation profile
    /// with any config overrides applied. Validates that the grammar's start
    /// production is reachable within the configured depth budget.
    ///
    /// # Errors
    ///
    /// Returns an error if the grammar file cannot be read, parsed, or if
    /// the start production's minimum depth exceeds the configured max depth.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let source = std::fs::read_to_string(&config.grammar_path)?;
        let grammar_ir = match config.format {
            GrammarFormat::Ebnf => {
                barkus_ebnf::compile(&source).map_err(|e| Error::Grammar(e.to_string()))?
            }
            GrammarFormat::Peg => {
                barkus_peg::compile(&source).map_err(|e| Error::Grammar(e.to_string()))?
            }
            GrammarFormat::Antlr => {
                barkus_antlr::compile(&source).map_err(|e| Error::Grammar(e.to_string()))?
            }
        };

        let mut builder = barkus_core::profile::Profile::builder();
        if let Some(depth) = config.max_depth {
            builder = builder.max_depth(depth);
        }
        if let Some(nodes) = config.max_total_nodes {
            builder = builder.max_total_nodes(nodes);
        }
        let profile = builder.build();

        // Validate that the start production can be reached within the depth
        // budget. Without this check a grammar whose min_depth exceeds
        // max_depth would cause every generation attempt to fail with
        // BudgetExhausted, wasting time during block cache construction.
        let start_min_depth = grammar_ir.productions[grammar_ir.start].attrs.min_depth;
        if start_min_depth > profile.max_depth {
            return Err(Error::Grammar(format!(
                "start production requires minimum depth {start_min_depth} \
                 but max_depth is {}",
                profile.max_depth,
            )));
        }

        Ok(Self {
            grammar_ir,
            profile,
        })
    }
}

impl crate::Serialize for Grammar {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if max_bytes == 0 {
            return Ok(());
        }

        // Bridge rand 0.9 (lading) -> rand 0.8 (barkus) by seeding a local
        // SmallRng from the lading RNG. Each call gets a unique seed,
        // preserving determinism when the outer RNG is seeded.
        // NOTE: retry seeds also come from `rng`, so the number of retries
        // affects the outer RNG state. Determinism is preserved as long as
        // both the seed and the grammar behaviour are identical.
        let seed: u64 = rng.random();
        let mut barkus_rng = rand_0_8::rngs::SmallRng::seed_from_u64(seed);

        let mut bytes_remaining = max_bytes;
        let mut consecutive_failures: u32 = 0;

        loop {
            match barkus_core::generate::generate(
                &self.grammar_ir,
                &self.profile,
                &mut barkus_rng,
            ) {
                Ok((ast, _tape, _tape_map)) => {
                    let sample = ast.serialize();
                    if sample.is_empty() {
                        consecutive_failures += 1;
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                            break;
                        }
                        continue;
                    }
                    consecutive_failures = 0;
                    // +1 for the trailing newline
                    let needed = sample.len() + 1;
                    let Some(remainder) = bytes_remaining.checked_sub(needed) else {
                        break;
                    };
                    writer.write_all(&sample)?;
                    writer.write_all(b"\n")?;
                    bytes_remaining = remainder;
                }
                Err(barkus_core::error::GenerateError::BudgetExhausted { .. }) => {
                    // Budget exhaustion is expected for complex grammars —
                    // retry with a fresh seed so we don't get stuck.
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        break;
                    }
                    let retry_seed: u64 = rng.random();
                    barkus_rng = rand_0_8::rngs::SmallRng::seed_from_u64(retry_seed);
                }
            }
        }
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES && bytes_remaining == max_bytes {
            warn!(
                "grammar generator hit {MAX_CONSECUTIVE_FAILURES} consecutive failures \
                 without producing any output"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Serialize as _;
    use proptest::prelude::*;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    /// Helper: write `source` to a temp file with the given extension and
    /// return a `Config` pointing to it.
    fn config_from_source(
        source: &str,
        format: GrammarFormat,
        ext: &str,
    ) -> (tempfile::TempDir, Config) {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join(format!("grammar.{ext}"));
        std::fs::write(&path, source).expect("failed to write grammar file");
        let config = Config {
            grammar_path: path,
            format,
            max_depth: Some(10),
            max_total_nodes: Some(1000),
        };
        (dir, config)
    }

    // ── EBNF ────────────────────────────────────────────────────────────

    #[test]
    fn ebnf_generates_expected_output() {
        let source = "greeting = \"hello\" | \"world\" ;";
        let (_dir, config) = config_from_source(source, GrammarFormat::Ebnf, "ebnf");

        let mut grammar = Grammar::new(&config).unwrap();
        let mut rng = SmallRng::seed_from_u64(42);
        let mut buf = Vec::new();
        grammar.to_bytes(&mut rng, 1024, &mut buf).unwrap();

        assert!(!buf.is_empty(), "grammar should produce non-empty output");
        let output = String::from_utf8_lossy(&buf);
        assert!(
            output.lines().count() > 1,
            "expected multiple lines in 1024 byte budget"
        );
        for line in output.lines() {
            assert!(
                line == "hello" || line == "world",
                "unexpected output line: {line:?}"
            );
        }
    }

    // ── PEG ─────────────────────────────────────────────────────────────

    #[test]
    fn peg_generates_expected_output() {
        let source = "greeting <- \"hello\" / \"world\"";
        let (_dir, config) = config_from_source(source, GrammarFormat::Peg, "peg");

        let mut grammar = Grammar::new(&config).unwrap();
        let mut rng = SmallRng::seed_from_u64(42);
        let mut buf = Vec::new();
        grammar.to_bytes(&mut rng, 1024, &mut buf).unwrap();

        assert!(!buf.is_empty(), "PEG grammar should produce non-empty output");
        for line in String::from_utf8_lossy(&buf).lines() {
            assert!(
                line == "hello" || line == "world",
                "unexpected PEG output: {line:?}"
            );
        }
    }

    // ── ANTLR ───────────────────────────────────────────────────────────

    #[test]
    fn antlr_generates_expected_output() {
        let source = "grammar Test;\ngreeting : 'hello' | 'world' ;";
        let (_dir, config) = config_from_source(source, GrammarFormat::Antlr, "g4");

        let mut grammar = Grammar::new(&config).unwrap();
        let mut rng = SmallRng::seed_from_u64(42);
        let mut buf = Vec::new();
        grammar.to_bytes(&mut rng, 1024, &mut buf).unwrap();

        assert!(
            !buf.is_empty(),
            "ANTLR grammar should produce non-empty output"
        );
        for line in String::from_utf8_lossy(&buf).lines() {
            assert!(
                line == "hello" || line == "world",
                "unexpected ANTLR output: {line:?}"
            );
        }
    }

    // ── Error paths ─────────────────────────────────────────────────────

    #[test]
    fn missing_grammar_file_returns_error() {
        let config = Config {
            grammar_path: PathBuf::from("/nonexistent/path/grammar.ebnf"),
            format: GrammarFormat::Ebnf,
            max_depth: Some(10),
            max_total_nodes: Some(1000),
        };
        assert!(Grammar::new(&config).is_err());
    }

    #[test]
    fn invalid_grammar_source_returns_error() {
        let source = "this is not valid EBNF @@@ !!!";
        let (_dir, config) = config_from_source(source, GrammarFormat::Ebnf, "ebnf");
        assert!(Grammar::new(&config).is_err());
    }

    #[test]
    fn depth_budget_too_small_returns_error() {
        // A recursive grammar that needs more depth than we allow.
        let source = "a = b ; b = c ; c = d ; d = e ; e = \"x\" ;";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deep.ebnf");
        std::fs::write(&path, source).unwrap();
        let config = Config {
            grammar_path: path,
            format: GrammarFormat::Ebnf,
            max_depth: Some(1), // too shallow for the chain a->b->c->d->e
            max_total_nodes: Some(1000),
        };
        assert!(Grammar::new(&config).is_err());
    }

    // ── Determinism ─────────────────────────────────────────────────────

    #[test]
    fn same_seed_produces_same_output() {
        let source = "item = \"a\" | \"b\" | \"c\" | \"d\" ;";
        let (_dir, config) = config_from_source(source, GrammarFormat::Ebnf, "ebnf");

        let mut grammar1 = Grammar::new(&config).unwrap();
        let mut grammar2 = Grammar::new(&config).unwrap();

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        grammar1
            .to_bytes(SmallRng::seed_from_u64(123), 2048, &mut buf1)
            .unwrap();
        grammar2
            .to_bytes(SmallRng::seed_from_u64(123), 2048, &mut buf2)
            .unwrap();

        assert_eq!(buf1, buf2, "same seed must produce identical output");
    }

    // ── Property tests ──────────────────────────────────────────────────

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes in 0..4096u16) {
            let max_bytes = max_bytes as usize;
            let source = "item = \"abcdefghij\" ;";
            let (_dir, config) = config_from_source(source, GrammarFormat::Ebnf, "ebnf");
            let mut grammar = Grammar::new(&config).unwrap();
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut buf = Vec::new();
            grammar.to_bytes(&mut rng, max_bytes, &mut buf).unwrap();
            prop_assert!(
                buf.len() <= max_bytes,
                "output {} bytes exceeds budget of {max_bytes}",
                buf.len()
            );
        }
    }

    // ── Zero budget ─────────────────────────────────────────────────────

    #[test]
    fn zero_budget_produces_empty_output() {
        let source = "item = \"hello\" ;";
        let (_dir, config) = config_from_source(source, GrammarFormat::Ebnf, "ebnf");
        let mut grammar = Grammar::new(&config).unwrap();
        let mut buf = Vec::new();
        grammar
            .to_bytes(SmallRng::seed_from_u64(1), 0, &mut buf)
            .unwrap();
        assert!(buf.is_empty());
    }
}
