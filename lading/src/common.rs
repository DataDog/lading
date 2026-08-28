use std::{fmt, fs, ops::Deref, path::PathBuf, process::Stdio, str};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

/// A seed for random number generation.
///
/// Accepts either a 32-element byte array (e.g. `[1, 2, 3, ...]`) or a plain
/// string. When a string is given, its SHA-256 hash is used as the seed bytes,
/// so any two invocations with the same string produce identical output.
///
/// # YAML examples
///
/// ```yaml
/// seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
///        59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
/// ```
///
/// ```yaml
/// seed: "black cat"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Seed([u8; 32]);

impl Default for Seed {
    fn default() -> Self {
        Self([0u8; 32])
    }
}

impl Deref for Seed {
    type Target = [u8; 32];

    fn deref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for Seed {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl From<Seed> for [u8; 32] {
    fn from(value: Seed) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for Seed {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Bytes([u8; 32]),
            String(String),
        }

        match Helper::deserialize(deserializer)? {
            Helper::Bytes(arr) => Ok(Seed(arr)),
            Helper::String(s) => {
                let hash = Sha256::digest(s.as_bytes());
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&hash);
                Ok(Seed(arr))
            }
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
/// Defines how sub-process stderr and stdout are handled.
pub struct Output {
    #[serde(default)]
    /// Determines how stderr is routed.
    pub stderr: Behavior,
    #[serde(default)]
    /// Determines how stderr is routed.
    pub stdout: Behavior,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
/// Defines the [`Output`] behavior for stderr and stdout.
pub enum Behavior {
    /// Redirect stdout, stderr to /dev/null
    Quiet,
    /// Write to a location on-disk.
    Log(PathBuf),
}

impl Default for Behavior {
    fn default() -> Self {
        Self::Quiet
    }
}

impl fmt::Display for Behavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Behavior::Quiet => write!(f, "/dev/null")?,
            Behavior::Log(path) => write!(f, "{}", path.display())?,
        }
        Ok(())
    }
}

impl str::FromStr for Behavior {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut path = PathBuf::new();
        path.push(input);
        Ok(Behavior::Log(path))
    }
}

#[cfg(test)]
mod tests {
    use super::Seed;

    #[test]
    fn seed_from_byte_array() {
        let yaml = "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]";
        let seed: Seed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            *seed,
            [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31, 32
            ]
        );
    }

    #[test]
    fn seed_from_string_is_deterministic() {
        let a: Seed = serde_yaml::from_str("\"black cat\"").unwrap();
        let b: Seed = serde_yaml::from_str("\"black cat\"").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn seed_different_strings_differ() {
        let a: Seed = serde_yaml::from_str("\"black cat\"").unwrap();
        let b: Seed = serde_yaml::from_str("\"umbrella\"").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn seed_roundtrips_via_serde() {
        let original: Seed = serde_yaml::from_str("\"hello world\"").unwrap();
        let serialized = serde_yaml::to_string(&original).unwrap();
        let roundtripped: Seed = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn seed_default_is_zeroes() {
        let s = Seed::default();
        assert_eq!(*s, [0u8; 32]);
    }
}

pub(crate) fn stdio(behavior: &Behavior) -> Stdio {
    match behavior {
        Behavior::Quiet => Stdio::null(),
        Behavior::Log(path) => {
            let fp = fs::File::create(path).unwrap_or_else(|_| {
                panic!(
                    "Full directory path does not exist: {path}",
                    path = path.display()
                );
            });
            Stdio::from(fp)
        }
    }
}
