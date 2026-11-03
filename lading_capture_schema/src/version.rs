//! Parse capture metadata without choosing a consumer's supported versions.

use std::{fmt, str::FromStr};

/// Footer key that identifies the capture contract independently of producer releases.
pub const CAPTURE_VERSION_KEY: &str = "capture.version";

/// A capture contract version with canonical major.minor spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureVersion {
    /// The major component identifies reader-visible contract changes.
    pub major: u64,
    /// The minor component identifies compatible additions.
    pub minor: u64,
}

impl CaptureVersion {
    /// Construct a capture version from its two numeric components.
    #[must_use]
    pub const fn new(major: u64, minor: u64) -> Self {
        Self { major, minor }
    }
}

impl fmt::Display for CaptureVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{major}.{minor}",
            major = self.major,
            minor = self.minor
        )
    }
}

impl FromStr for CaptureVersion {
    type Err = MetadataError;

    /// Parse two canonical unsigned 64-bit decimal components.
    ///
    /// # Errors
    ///
    /// Return an error for invalid major.minor syntax or a component outside its numeric bounds.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || MetadataError::InvalidVersion(value.to_string());
        let (major, minor) = value.split_once('.').ok_or_else(invalid)?;
        Ok(Self::new(
            parse_component(major).ok_or_else(invalid)?,
            parse_component(minor).ok_or_else(invalid)?,
        ))
    }
}

fn parse_component(value: &str) -> Option<u64> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    value.parse().ok()
}

/// Contract implemented by the current Lading Parquet writer.
pub const CURRENT_CAPTURE_VERSION: CaptureVersion = CaptureVersion::new(2, 0);

/// Invalid capture metadata, distinct from a valid but unsupported format.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// Duplicate keys make version selection ambiguous.
    #[error("capture.version appears more than once")]
    DuplicateVersion,
    /// A present key must carry a version string.
    #[error("capture.version has no value")]
    MissingValue,
    /// The version value does not follow canonical major.minor syntax.
    #[error("invalid capture.version {0:?}: expected major.minor with unsigned 64-bit components")]
    InvalidVersion(String),
}

/// Parse raw footer entries without reading rows or enforcing decoder support.
///
/// Return `None` only when the key is absent. A valid unknown version remains a
/// value so consumers can decide whether to process it. Inspect raw entries
/// before converting metadata to a map, which could hide duplicate keys.
///
/// # Errors
///
/// Return an error for duplicate keys, missing values, or invalid version syntax.
pub fn parse_capture_version<'a>(
    metadata: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Option<CaptureVersion>, MetadataError> {
    let mut values = metadata
        .into_iter()
        .filter_map(|(key, value)| (key == CAPTURE_VERSION_KEY).then_some(value));
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(MetadataError::DuplicateVersion);
    }
    Ok(Some(value.ok_or(MetadataError::MissingValue)?.parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod parse_capture_version {
        use super::*;
        use proptest::{collection, prelude::any, prop_assert, prop_assert_eq, proptest};

        proptest! {
            /// For all major/minor values: parse(display(version)) = version.
            #[test]
            fn version_round_trip(major: u64, minor: u64) {
                let version = CaptureVersion::new(major, minor);
                let encoded = version.to_string();
                prop_assert_eq!(encoded.parse::<CaptureVersion>()?, version);
                let parsed = parse_capture_version([(CAPTURE_VERSION_KEY, Some(encoded.as_str()))])?;
                prop_assert_eq!(parsed, Some(version));
            }

            /// Unrelated metadata and entry order do not change the declared version.
            #[test]
            fn metadata_order_independent(
                keys in collection::vec("[a-z]{1,12}", 0..12),
                insertion in any::<usize>(),
                major: u64,
                minor: u64,
            ) {
                let version = CaptureVersion::new(major, minor);
                let encoded = version.to_string();
                let mut entries: Vec<_> = keys.iter().map(|key| (key.as_str(), Some("ignored"))).collect();
                entries.insert(insertion % (entries.len() + 1), (CAPTURE_VERSION_KEY, Some(encoded.as_str())));
                prop_assert_eq!(parse_capture_version(entries.iter().copied())?, Some(version));
                entries.reverse();
                prop_assert_eq!(parse_capture_version(entries)?, Some(version));
            }

            /// For every valid version, adding a patch component or suffix fails.
            #[test]
            fn extra_components_fail(major: u64, minor: u64) {
                let version = CaptureVersion::new(major, minor);
                for suffix in [".0", ".1", "-alpha", "+build"] {
                    let value = format!("{version}{suffix}");
                    prop_assert!(value.parse::<CaptureVersion>().is_err());
                }
            }
        }

        #[test]
        fn absent_is_not_a_default_version() -> Result<(), MetadataError> {
            assert_eq!(parse_capture_version([])?, None);
            assert_eq!(
                parse_capture_version([("ARROW:schema", Some("ignored"))])?,
                None
            );
            Ok(())
        }

        #[test]
        fn unknown_versions_remain_values() -> Result<(), MetadataError> {
            for version in [
                CaptureVersion::new(0, 0),
                CaptureVersion::new(99, 12),
                CaptureVersion::new(u64::MAX, u64::MAX),
            ] {
                let encoded = version.to_string();
                assert_eq!(
                    parse_capture_version([(CAPTURE_VERSION_KEY, Some(encoded.as_str()))])?,
                    Some(version)
                );
            }
            Ok(())
        }

        #[test]
        fn rejects_missing_and_invalid_values() {
            assert!(matches!(
                parse_capture_version([(CAPTURE_VERSION_KEY, None)]),
                Err(MetadataError::MissingValue)
            ));
            for value in [
                "",
                "1",
                "1.",
                ".0",
                "1.0.0",
                "1.0.1",
                "01.0",
                "1.00",
                "+1.0",
                "-1.0",
                "1.+0",
                "v1.0",
                " 1.0",
                "1.0 ",
                "1.0\n",
                "1.0-alpha",
                "1.0+build",
                "\u{ff11}.0",
                "18446744073709551616.0",
                "1.18446744073709551616",
            ] {
                assert!(
                    matches!(
                        parse_capture_version([(CAPTURE_VERSION_KEY, Some(value))]),
                        Err(MetadataError::InvalidVersion(_))
                    ),
                    "accepted {value:?}"
                );
            }
        }

        #[test]
        fn duplicates_fail_before_value_selection() {
            for values in [
                [Some("1.0"), Some("1.0")],
                [Some("1.0"), Some("99.0")],
                [None, Some("1.0")],
                [Some("invalid"), None],
            ] {
                for pair in [values, [values[1], values[0]]] {
                    let entries = pair.map(|value| (CAPTURE_VERSION_KEY, value));
                    assert!(matches!(
                        parse_capture_version(entries),
                        Err(MetadataError::DuplicateVersion)
                    ));
                }
            }
        }
    }
}
