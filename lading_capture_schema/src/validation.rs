//! Check named contract fields without imposing file I/O or reader policy.

use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, Schema};

/// Schema disagreement with a declared capture contract.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaError {
    /// Name-based lookup must not choose between duplicate fields.
    #[error("duplicate capture field {0}")]
    DuplicateField(String),
    /// Nullable values do not make a mandatory column optional.
    #[error("missing capture field {0}")]
    MissingField(String),
    /// Coercion must not hide a changed field representation.
    #[error("capture field {field} has type {actual}, expected {expected}")]
    TypeMismatch {
        /// Contract field path.
        field: String,
        /// Observed type.
        actual: String,
        /// Declared contract type.
        expected: String,
    },
    /// The declared nullability is part of the known field contract.
    #[error("capture field {field} has nullable={actual}, expected {expected}")]
    NullabilityMismatch {
        /// Contract field path.
        field: String,
        /// Observed nullability.
        actual: bool,
        /// Declared contract nullability.
        expected: bool,
    },
}

/// Check mandatory fields against a consumer-selected known capture schema.
///
/// Match top-level columns by name, but preserve ordered roles inside the label map.
/// Allow extra top-level columns and ignore unrelated schema/field metadata.
/// This checks declarations, not row values or decoder support.
///
/// # Errors
///
/// Return an error for ambiguous names or changes to mandatory fields and nested structure.
pub fn validate_schema(actual: &Schema, expected: &Schema) -> Result<(), SchemaError> {
    let mut names = BTreeSet::new();
    for field in actual.fields() {
        if !names.insert(field.name()) {
            return Err(SchemaError::DuplicateField(field.name().clone()));
        }
    }
    for field in expected.fields() {
        let observed = actual
            .fields()
            .iter()
            .find(|candidate| candidate.name() == field.name())
            .ok_or_else(|| SchemaError::MissingField(field.name().clone()))?;
        if observed.is_nullable() != field.is_nullable() {
            return Err(SchemaError::NullabilityMismatch {
                field: field.name().clone(),
                actual: observed.is_nullable(),
                expected: field.is_nullable(),
            });
        }
        let actual_type = observed.data_type();
        let expected_type = field.data_type();
        if !types_match(actual_type, expected_type) {
            return Err(SchemaError::TypeMismatch {
                field: field.name().clone(),
                actual: format!("{actual_type:?}"),
                expected: format!("{expected_type:?}"),
            });
        }
    }
    Ok(())
}

fn types_match(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::Map(actual, actual_sorted), DataType::Map(expected, expected_sorted)) => {
            actual_sorted == expected_sorted && fields_match(actual, expected)
        }
        (DataType::Struct(actual), DataType::Struct(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| fields_match(actual, expected))
        }
        _ => actual == expected,
    }
}

fn fields_match(actual: &Field, expected: &Field) -> bool {
    actual.name() == expected.name()
        && actual.is_nullable() == expected.is_nullable()
        && types_match(actual.data_type(), expected.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod validate_schema {
        use super::*;
        use crate::{LabelEncoding, capture_schema, capture_schema_for_major, columns};
        use arrow_schema::Fields;
        use proptest::{collection, prelude::any, prop_assert, proptest};
        use std::sync::Arc;

        proptest! {
            /// Any permutation and ignorable top-level addition preserves the mandatory contract.
            #[test]
            fn names_not_positions(
                order in collection::vec(any::<u64>(), 9),
                insertion in 0usize..10,
            ) {
                let expected = capture_schema();
                let mut fields: Vec<_> = order.into_iter().zip(expected.fields().iter().cloned()).collect();
                fields.sort_by_key(|(key, _)| *key);
                let mut fields: Vec<_> = fields.into_iter().map(|(_, field)| field).collect();
                fields.insert(insertion, Arc::new(Field::new("diagnostic_note", DataType::Utf8, true)));
                prop_assert!(validate_schema(&Schema::new(fields), &expected).is_ok());
            }
        }

        fn replace_field(schema: &Schema, name: &str, replacement: Field) -> Schema {
            let replacement = Arc::new(replacement);
            Schema::new(
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if field.name() == name {
                            Arc::clone(&replacement)
                        } else {
                            Arc::clone(field)
                        }
                    })
                    .collect::<Vec<_>>(),
            )
        }

        #[test]
        fn metadata_does_not_define_compatibility() -> Result<(), SchemaError> {
            let expected = capture_schema();
            let mut fields: Vec<_> = expected
                .fields()
                .iter()
                .map(|field| {
                    let mut field = (**field).clone();
                    field
                        .metadata_mut()
                        .insert("writer.note".to_string(), "ignored".to_string());
                    field
                })
                .collect();
            let label_field = fields
                .iter_mut()
                .find(|field| field.name() == columns::LABELS);
            if let Some(field) = label_field
                && let DataType::Map(entry, sorted) = field.data_type()
            {
                let mut entry = (**entry).clone();
                entry
                    .metadata_mut()
                    .insert("writer.note".to_string(), "ignored".to_string());
                *field = field
                    .clone()
                    .with_data_type(DataType::Map(Arc::new(entry), *sorted));
            }
            let mut actual = Schema::new(fields);
            actual
                .metadata
                .insert("writer.release".to_string(), "unrelated".to_string());
            validate_schema(&actual, &expected)
        }

        #[test]
        fn missing_nullable_column_still_fails() {
            let expected = capture_schema();
            let fields: Vec<_> = expected
                .fields()
                .iter()
                .filter(|field| field.name() != columns::VALUE_FLOAT)
                .cloned()
                .collect();
            assert!(matches!(
                validate_schema(&Schema::new(fields), &expected),
                Err(SchemaError::MissingField(_))
            ));
        }

        #[test]
        fn types_and_nullability_do_not_coerce() {
            let expected = capture_schema();
            let actual = replace_field(
                &expected,
                columns::FETCH_INDEX,
                Field::new(columns::FETCH_INDEX, DataType::Int64, false),
            );
            assert!(matches!(
                validate_schema(&actual, &expected),
                Err(SchemaError::TypeMismatch { .. })
            ));
            let actual = replace_field(
                &expected,
                columns::METRIC_NAME,
                Field::new(columns::METRIC_NAME, DataType::Utf8, true),
            );
            assert!(matches!(
                validate_schema(&actual, &expected),
                Err(SchemaError::NullabilityMismatch { .. })
            ));
        }

        #[test]
        fn duplicate_names_are_ambiguous() {
            let expected = capture_schema();
            let mut fields = expected.fields().to_vec();
            fields.push(Arc::new(Field::new(
                columns::METRIC_NAME,
                DataType::Utf8,
                false,
            )));
            assert!(matches!(
                validate_schema(&Schema::new(fields), &expected),
                Err(SchemaError::DuplicateField(_))
            ));
        }

        /// For all majors M in {1, 2}: `validate_schema` accepts the label map of M only when its
        /// entry fields are (key, value), in this order.
        #[test]
        fn label_map_roles_are_ordered() -> Result<(), &'static str> {
            for (major, label_type) in [
                (1, DataType::Utf8),
                (2, LabelEncoding::Dictionary.data_type()),
            ] {
                let expected = capture_schema_for_major(major).ok_or("missing schema")?;
                label_map_roles_are_ordered_for(&expected, &label_type);
            }
            Ok(())
        }

        fn label_map_roles_are_ordered_for(expected: &Schema, label_type: &DataType) {
            for names in [
                [columns::LABEL_KEY, columns::LABEL_VALUE],
                [columns::LABEL_VALUE, columns::LABEL_KEY],
            ] {
                let fields: Vec<_> = names
                    .iter()
                    .map(|name| Field::new(*name, label_type.clone(), false))
                    .collect();
                let entry = Field::new(
                    columns::LABEL_ENTRIES,
                    DataType::Struct(Fields::from(fields)),
                    false,
                );
                let actual = replace_field(
                    expected,
                    columns::LABELS,
                    Field::new(
                        columns::LABELS,
                        DataType::Map(Arc::new(entry), false),
                        false,
                    ),
                );
                assert_eq!(
                    validate_schema(&actual, expected).is_ok(),
                    names == [columns::LABEL_KEY, columns::LABEL_VALUE],
                );
            }
        }

        /// For all majors M in {1, 2} and label maps with mixed key and value types:
        /// `validate_schema(actual, M)` returns `TypeMismatch`.
        #[test]
        fn nested_label_types_are_part_of_the_contract() -> Result<(), &'static str> {
            for major in [1, 2] {
                let expected = capture_schema_for_major(major).ok_or("missing schema")?;
                for (key, value) in [
                    (DataType::Utf8, LabelEncoding::Dictionary.data_type()),
                    (LabelEncoding::Dictionary.data_type(), DataType::Utf8),
                ] {
                    let entry = Field::new(
                        columns::LABEL_ENTRIES,
                        DataType::Struct(Fields::from(vec![
                            Field::new(columns::LABEL_KEY, key, false),
                            Field::new(columns::LABEL_VALUE, value, false),
                        ])),
                        false,
                    );
                    let actual = replace_field(
                        &expected,
                        columns::LABELS,
                        Field::new(
                            columns::LABELS,
                            DataType::Map(Arc::new(entry), false),
                            false,
                        ),
                    );
                    assert!(matches!(
                        validate_schema(&actual, &expected),
                        Err(SchemaError::TypeMismatch { .. })
                    ));
                }
            }
            Ok(())
        }
    }
}
