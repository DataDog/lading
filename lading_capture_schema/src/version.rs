//! Parse capture metadata without choosing a consumer's supported versions.

use semver::Version;

/// Footer key that identifies the capture contract independently of producer releases.
pub const CAPTURE_VERSION_KEY: &str = "capture.version";

/// Contract implemented by the current Lading Parquet writer.
pub const CURRENT_CAPTURE_VERSION: Version = Version::new(1, 0, 0);

/// Invalid capture metadata, distinct from a valid but unsupported format.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// Duplicate keys make version selection ambiguous.
    #[error("capture.version appears more than once")]
    DuplicateVersion,
    /// A present key must carry a version string.
    #[error("capture.version has no value")]
    MissingValue,
    /// The value does not follow canonical `SemVer` syntax.
    #[error("invalid capture.version: {0}")]
    InvalidVersion(#[from] semver::Error),
    /// Capture contracts do not use patch revisions or suffixes.
    #[error("capture.version {version} must use patch zero without prerelease or build suffixes")]
    DisallowedComponents {
        /// Parsed value that violates the capture-version policy.
        version: Version,
    },
}

/// Parse raw footer entries without reading rows or enforcing decoder support.
///
/// Return `None` only when the key is absent. A valid unknown version remains a
/// value so consumers can decide whether to process it. Inspect raw entries
/// before converting metadata to a map, which could hide duplicate keys.
///
/// # Errors
///
/// Return an error for duplicate keys, missing values, invalid `SemVer`, or disallowed components.
pub fn parse_capture_version<'a>(
    metadata: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Option<Version>, MetadataError> {
    let mut values = metadata
        .into_iter()
        .filter_map(|(key, value)| (key == CAPTURE_VERSION_KEY).then_some(value));
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(MetadataError::DuplicateVersion);
    }
    let version = Version::parse(value.ok_or(MetadataError::MissingValue)?)?;
    if version.patch != 0 || !version.pre.is_empty() || !version.build.is_empty() {
        return Err(MetadataError::DisallowedComponents { version });
    }
    Ok(Some(version))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod parse_capture_version {
        use super::*;
        use proptest::{collection, prelude::any, prop_assert_eq, proptest};

        proptest! {
            /// For all major/minor values: parse(display(version)) = version.
            #[test]
            fn version_round_trip(major: u64, minor: u64) {
                let version = Version::new(major, minor, 0);
                let encoded = version.to_string();
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
                let version = Version::new(major, minor, 0);
                let encoded = version.to_string();
                let mut entries: Vec<_> = keys.iter().map(|key| (key.as_str(), Some("ignored"))).collect();
                entries.insert(insertion % (entries.len() + 1), (CAPTURE_VERSION_KEY, Some(encoded.as_str())));
                prop_assert_eq!(parse_capture_version(entries.iter().copied())?, Some(version.clone()));
                entries.reverse();
                prop_assert_eq!(parse_capture_version(entries)?, Some(version));
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
                Version::new(0, 0, 0),
                Version::new(99, 12, 0),
                Version::new(u64::MAX, u64::MAX, 0),
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
                "1.0",
                "1.0.0.0",
                "01.0.0",
                "1.00.0",
                "+1.0.0",
                "v1.0.0",
                " 1.0.0",
                "1.0.0 ",
                "1.0.1",
                "1.0.0-alpha",
                "1.0.0+build",
                "18446744073709551616.0.0",
            ] {
                assert!(
                    parse_capture_version([(CAPTURE_VERSION_KEY, Some(value))]).is_err(),
                    "accepted {value:?}"
                );
            }
        }

        #[test]
        fn duplicates_fail_before_value_selection() {
            for values in [
                [Some("1.0.0"), Some("1.0.0")],
                [Some("1.0.0"), Some("99.0.0")],
                [None, Some("1.0.0")],
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
