#![forbid(unsafe_code)]

//! Exact, client-constructible operation identity.
//!
//! This crate deliberately knows nothing about workflow DAGs or semantic
//! equivalence. It maps one validated descriptor to one stable BLAKE3-256 ID.

use punah_types::{ArtifactId, OperationId};
use std::fmt;

const MAGIC: &[u8; 8] = b"PUNAHOP\0";
pub const FORMAT_VERSION: u16 = 1;
pub const MAX_KIND_BYTES: usize = 256;
pub const MAX_IMPLEMENTATION_BYTES: usize = 4 * 1024;
pub const MAX_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DEPENDENCIES: usize = 65_536;
pub const MAX_NAMED_FIELDS: usize = 1_024;
pub const MAX_FIELD_NAME_BYTES: usize = 256;
pub const MAX_FIELD_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_ENCODED_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedField {
    name: Vec<u8>,
    value: Vec<u8>,
}

impl NamedField {
    #[must_use]
    pub fn new(name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationDescriptor {
    operation_kind: Vec<u8>,
    implementation: Vec<u8>,
    dependencies: Vec<ArtifactId>,
    arguments: Vec<u8>,
    environment_fingerprint: [u8; 32],
    schema_fingerprint: [u8; 32],
    named_fields: Vec<NamedField>,
}

impl OperationDescriptor {
    /// Builds a descriptor after checking all canonical-form invariants.
    ///
    /// Named fields must be strictly ordered by their byte names. Punah does
    /// not silently sort them because doing so could conceal inconsistent
    /// canonicalization between client implementations.
    ///
    /// # Errors
    ///
    /// Returns an error when a required field is empty, a limit is exceeded,
    /// or named fields are not strictly sorted and unique.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_kind: impl Into<Vec<u8>>,
        implementation: impl Into<Vec<u8>>,
        dependencies: Vec<ArtifactId>,
        arguments: impl Into<Vec<u8>>,
        environment_fingerprint: [u8; 32],
        schema_fingerprint: [u8; 32],
        named_fields: Vec<NamedField>,
    ) -> Result<Self, DescriptorError> {
        let descriptor = Self {
            operation_kind: operation_kind.into(),
            implementation: implementation.into(),
            dependencies,
            arguments: arguments.into(),
            environment_fingerprint,
            schema_fingerprint,
            named_fields,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Encodes the descriptor in its stable, versioned canonical form.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor violates a field or total-size
    /// limit or is not in canonical named-field order.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DescriptorError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        push_bytes(&mut encoded, &self.operation_kind)?;
        push_bytes(&mut encoded, &self.implementation)?;
        push_count(&mut encoded, self.dependencies.len())?;
        for dependency in &self.dependencies {
            encoded.extend_from_slice(dependency.as_bytes());
        }
        push_bytes(&mut encoded, &self.arguments)?;
        encoded.extend_from_slice(&self.environment_fingerprint);
        encoded.extend_from_slice(&self.schema_fingerprint);
        push_count(&mut encoded, self.named_fields.len())?;
        for field in &self.named_fields {
            push_bytes(&mut encoded, field.name())?;
            push_bytes(&mut encoded, field.value())?;
        }
        if encoded.len() > MAX_ENCODED_BYTES {
            return Err(DescriptorError::EncodedTooLarge {
                actual: encoded.len(),
                maximum: MAX_ENCODED_BYTES,
            });
        }
        Ok(encoded)
    }

    /// Computes BLAKE3-256 over the complete canonical encoding.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor cannot be canonically encoded.
    pub fn operation_id(&self) -> Result<OperationId, DescriptorError> {
        let bytes = self.canonical_bytes()?;
        Ok(OperationId::new(*blake3::hash(&bytes).as_bytes()))
    }

    /// Verifies that `claimed` identifies this exact descriptor.
    ///
    /// # Errors
    ///
    /// Returns an invalid-descriptor error when canonical encoding fails, or a
    /// mismatch containing both IDs when the digest differs.
    pub fn verify_claimed_id(&self, claimed: OperationId) -> Result<(), VerifyError> {
        let actual = self
            .operation_id()
            .map_err(VerifyError::InvalidDescriptor)?;
        if actual == claimed {
            Ok(())
        } else {
            Err(VerifyError::Mismatch { claimed, actual })
        }
    }

    fn validate(&self) -> Result<(), DescriptorError> {
        check_nonempty("operation kind", &self.operation_kind, MAX_KIND_BYTES)?;
        check_nonempty(
            "implementation",
            &self.implementation,
            MAX_IMPLEMENTATION_BYTES,
        )?;
        check_max("arguments", self.arguments.len(), MAX_ARGUMENT_BYTES)?;
        check_max("dependencies", self.dependencies.len(), MAX_DEPENDENCIES)?;
        check_max("named fields", self.named_fields.len(), MAX_NAMED_FIELDS)?;
        let mut previous: Option<&[u8]> = None;
        for field in &self.named_fields {
            check_nonempty("field name", field.name(), MAX_FIELD_NAME_BYTES)?;
            check_max("field value", field.value().len(), MAX_FIELD_VALUE_BYTES)?;
            if let Some(prior) = previous
                && prior >= field.name()
            {
                return Err(DescriptorError::FieldsNotStrictlySorted);
            }
            previous = Some(field.name());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescriptorError {
    Empty {
        field: &'static str,
    },
    TooLarge {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    FieldsNotStrictlySorted,
    EncodedTooLarge {
        actual: usize,
        maximum: usize,
    },
}

impl fmt::Display for DescriptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DescriptorError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VerifyError {
    InvalidDescriptor(DescriptorError),
    Mismatch {
        claimed: OperationId,
        actual: OperationId,
    },
}

impl fmt::Display for VerifyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for VerifyError {}

fn check_nonempty(
    field: &'static str,
    value: &[u8],
    maximum: usize,
) -> Result<(), DescriptorError> {
    if value.is_empty() {
        return Err(DescriptorError::Empty { field });
    }
    check_max(field, value.len(), maximum)
}

fn check_max(field: &'static str, actual: usize, maximum: usize) -> Result<(), DescriptorError> {
    if actual > maximum {
        return Err(DescriptorError::TooLarge {
            field,
            actual,
            maximum,
        });
    }
    Ok(())
}

fn push_count(output: &mut Vec<u8>, count: usize) -> Result<(), DescriptorError> {
    let count = u32::try_from(count).map_err(|_| DescriptorError::EncodedTooLarge {
        actual: count,
        maximum: u32::MAX as usize,
    })?;
    output.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), DescriptorError> {
    push_count(output, bytes.len())?;
    output.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(fields: Vec<NamedField>) -> OperationDescriptor {
        OperationDescriptor::new(
            b"tool.call".to_vec(),
            b"compiler/rustc/1.97.1".to_vec(),
            vec![ArtifactId::new([0x11; 32]), ArtifactId::new([0x22; 32])],
            br#"{"target":"workspace"}"#.to_vec(),
            [0x33; 32],
            [0x44; 32],
            fields,
        )
        .unwrap()
    }

    #[test]
    fn canonical_vector_is_stable() {
        let value = descriptor(vec![
            NamedField::new("attempt", [1]),
            NamedField::new("profile", b"release"),
        ]);
        assert_eq!(
            value.operation_id().unwrap().to_hex(),
            "b5a0b6fa919bf9dd93069efe3b81e73932b8d5990f8b73307791245bbfa73c91"
        );
    }

    #[test]
    fn component_boundaries_are_unambiguous() {
        let left =
            OperationDescriptor::new("a", "bc", vec![], [], [0; 32], [0; 32], vec![]).unwrap();
        let right =
            OperationDescriptor::new("ab", "c", vec![], [], [0; 32], [0; 32], vec![]).unwrap();
        assert_ne!(
            left.canonical_bytes().unwrap(),
            right.canonical_bytes().unwrap()
        );
        assert_ne!(left.operation_id().unwrap(), right.operation_id().unwrap());
    }

    #[test]
    fn dependencies_are_order_significant() {
        let a = ArtifactId::new([1; 32]);
        let b = ArtifactId::new([2; 32]);
        let left =
            OperationDescriptor::new("x", "y", vec![a, b], [], [0; 32], [0; 32], vec![]).unwrap();
        let right =
            OperationDescriptor::new("x", "y", vec![b, a], [], [0; 32], [0; 32], vec![]).unwrap();
        assert_ne!(left.operation_id().unwrap(), right.operation_id().unwrap());
    }

    #[test]
    fn named_fields_must_be_sorted_and_unique() {
        let unsorted = vec![NamedField::new("z", []), NamedField::new("a", [])];
        let duplicate = vec![NamedField::new("a", []), NamedField::new("a", [1])];
        for fields in [unsorted, duplicate] {
            assert_eq!(
                OperationDescriptor::new("x", "y", vec![], [], [0; 32], [0; 32], fields),
                Err(DescriptorError::FieldsNotStrictlySorted)
            );
        }
    }

    #[test]
    fn size_boundaries_are_enforced() {
        assert!(
            OperationDescriptor::new(
                vec![b'x'; MAX_KIND_BYTES],
                "y",
                vec![],
                vec![0; MAX_ARGUMENT_BYTES],
                [0; 32],
                [0; 32],
                vec![]
            )
            .is_ok()
        );
        assert_eq!(
            OperationDescriptor::new(
                vec![b'x'; MAX_KIND_BYTES + 1],
                "y",
                vec![],
                [],
                [0; 32],
                [0; 32],
                vec![]
            ),
            Err(DescriptorError::TooLarge {
                field: "operation kind",
                actual: MAX_KIND_BYTES + 1,
                maximum: MAX_KIND_BYTES
            })
        );
    }

    #[test]
    fn claimed_identity_is_verified() {
        let value = descriptor(vec![]);
        let actual = value.operation_id().unwrap();
        assert_eq!(value.verify_claimed_id(actual), Ok(()));
        assert!(matches!(
            value.verify_claimed_id(OperationId::new([9; 32])),
            Err(VerifyError::Mismatch { .. })
        ));
    }
}
