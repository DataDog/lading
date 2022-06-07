use std::io::Write;

use rand::Rng;
use regex_syntax::ast::{parse::Parser, Ast, Class};

use crate::payload::{Error, Serialize};

pub(crate) struct Pattern {
    ast: Ast,
}

impl Pattern {
    #[must_use]
    pub(crate) fn new(pattern: &str) -> Self {
        let mut parser: Parser = Parser::new();

        let ast = parser.parse(pattern).unwrap();
        Self { ast }
    }
}

impl Serialize for Pattern {
    #[allow(unused_assignments)]
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut buffer: Vec<u8> = vec![0; 128];

        let mut bytes_remaining = max_bytes;
        match &self.ast {
            Ast::Empty(_) => unimplemented!(),
            Ast::Flags(_set_flags) => unimplemented!(),
            Ast::Literal(literal) => {
                let s = literal.c.encode_utf8(&mut buffer);
                if bytes_remaining > s.len() {
                    bytes_remaining -= s.len();
                    writer.write_all(s.as_bytes())?;
                }
            }
            Ast::Dot(_) => {
                // NOTE this is almost the same implementation as Literal
                let c = rng.gen::<char>();
                let s = c.encode_utf8(&mut buffer);
                if bytes_remaining > s.len() {
                    bytes_remaining -= s.len();
                    writer.write_all(s.as_bytes())?;
                }
            }
            Ast::Assertion(_assertion) => unimplemented!(),
            Ast::Class(ref class) => match class {
                Class::Unicode(_unicode) => unimplemented!(),
                Class::Perl(_perl) => unimplemented!(),
                Class::Bracketed(_bracketed) => unimplemented!(),
            },
            Ast::Repetition(repetition) => {}
            Ast::Group(_group) => unimplemented!(),
            Ast::Alternation(_alternation) => unimplemented!(),
            Ast::Concat(_concat) => unimplemented!(),
        }

        // // Read lines from `static_path` until such time as the total byte
        // // length of the lines read exceeds `bytes_max`. If the path contains
        // // more bytes than `bytes_max` the tail of the file will be chopped off.
        // let file = std::fs::OpenOptions::new().read(true).open(self.path)?;
        // let mut reader = std::io::BufReader::new(file);

        // let mut bytes_remaining = max_bytes;
        // let mut line = String::new();
        // while bytes_remaining > 0 {
        //     let len = reader.read_line(&mut line)?;
        //     if len > bytes_remaining {
        //         break;
        //     }

        //     writer.write_all(line.as_bytes())?;
        //     bytes_remaining = bytes_remaining.saturating_sub(line.len());
        // }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // Will have here a list of almost-identical tests. Each has a regular
    // expression pattern and we assert that the generated instances all parse
    // correctly by the regex crate.

    use crate::payload::Serialize;
    use proptest::prelude::*;
    use rand::{prelude::SmallRng, SeedableRng};

    macro_rules! pattern_parse {
        ( $slug:ident, $pattern:expr ) => {
            proptest! {
                #[test]
                fn $slug(seed: u64, max_bytes: u16, checks: u8) {
                    let max_bytes = max_bytes as usize;
                    let rng = SmallRng::seed_from_u64(seed);

                    let pattern = $pattern;
                    let pattern_gen = super::Pattern::new(pattern);
                    let regex = ::regex::bytes::Regex::new(pattern).unwrap();

                    let mut bytes = Vec::with_capacity(max_bytes);
                    for _ in 0..checks {
                        pattern_gen.to_bytes(rng.clone(), max_bytes, &mut bytes).unwrap();

                        prop_assert!(bytes.len() <= max_bytes);
                        prop_assert!(regex.is_match(&bytes));
                        bytes.clear();
                    }
                }
            }
        };
    }

    pattern_parse!(simple_literal, "a");
    pattern_parse!(simple_dot, ".");
}
