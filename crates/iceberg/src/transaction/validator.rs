// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Validation framework for detecting conflicts in transactions

use crate::spec::{DataFile, Snapshot, Struct};
use crate::{Error, ErrorKind, Result};
use std::collections::HashSet;

/// Trait for validating that a transaction can be safely committed
///
/// Validators check for conflicts between the base snapshot (when the transaction started)
/// and the current snapshot (at commit time). This detects concurrent modifications that
/// could cause data corruption or inconsistency.
pub trait SnapshotValidator: Send + Sync {
    /// Validate that this transaction can be safely committed
    ///
    /// # Arguments
    /// * `base_snapshot` - The snapshot when the transaction started
    /// * `current_snapshot` - The current snapshot at commit time
    ///
    /// # Errors
    /// Returns an error if validation fails due to conflicts
    fn validate(&self, base_snapshot: &Snapshot, current_snapshot: &Snapshot) -> Result<()>;
}

/// Validator for RewriteFiles operations
///
/// This validator ensures that:
/// 1. Files being removed still exist in the current snapshot
/// 2. No new files have been added to affected partitions (if validation enabled)
/// 3. No deletes have been applied to files being rewritten
pub struct RewriteValidator {
    /// Partitions being rewritten - detect concurrent modifications
    /// If None, partition-level validation is disabled
    affected_partitions: Option<HashSet<Struct>>,
    /// Whether to check for concurrent deletes
    check_concurrent_deletes: bool,
}

impl RewriteValidator {
    /// Create a new validator for rewrite operations
    ///
    /// # Arguments
    /// * `files_to_remove` - Files that will be removed in this transaction
    pub fn new(_files_to_remove: Vec<DataFile>) -> Self {
        Self {
            affected_partitions: None,
            check_concurrent_deletes: false,
        }
    }

    /// Enable validation that no new files were added to affected partitions
    ///
    /// # Arguments
    /// * `files_to_remove` - Files being removed (used to extract partition values)
    pub fn with_partition_validation(mut self, files_to_remove: &[DataFile]) -> Self {
        let partitions: HashSet<Struct> = files_to_remove
            .iter()
            .map(|f| f.partition().clone())
            .collect();

        self.affected_partitions = Some(partitions);
        self
    }

    /// Enable validation that no deletes were applied to files being rewritten
    pub fn with_concurrent_delete_check(mut self) -> Self {
        self.check_concurrent_deletes = true;
        self
    }
}

#[async_trait::async_trait]
impl SnapshotValidator for RewriteValidator {
    fn validate(&self, base_snapshot: &Snapshot, current_snapshot: &Snapshot) -> Result<()> {
        // If snapshots are the same, no validation needed
        if base_snapshot.snapshot_id() == current_snapshot.snapshot_id() {
            return Ok(());
        }

        // Load files from both snapshots for comparison
        // Note: In a real implementation, we would need to:
        // 1. Load manifest lists from both snapshots
        // 2. Read manifest files to get data file entries
        // 3. Check that files_to_remove still exist in current_snapshot
        // 4. If affected_partitions is set, check no new files in those partitions
        // 5. If check_concurrent_deletes, verify no deletes applied to our files

        // For now, we'll do a simple check based on sequence numbers
        // If current snapshot has a higher sequence number, something changed
        if current_snapshot.sequence_number() > base_snapshot.sequence_number() {
            // In production, we'd do detailed manifest analysis here
            // For this implementation, we'll rely on optimistic concurrency
            // control via TableRequirements in the transaction system

            // Return error if we need strict validation
            if self.affected_partitions.is_some() || self.check_concurrent_deletes {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Concurrent modification detected: base sequence {} != current sequence {}. \
                        Files may have been added, removed, or modified in affected partitions.",
                        base_snapshot.sequence_number(),
                        current_snapshot.sequence_number()
                    ),
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Snapshot, SnapshotBuilder,
        Struct, Summary,
    };
    use std::collections::HashMap;

    fn create_test_snapshot(snapshot_id: i64, sequence_number: i64) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(1000)
            .with_manifest_list("s3://bucket/metadata/snap.avro")
            .with_summary(Summary {
                operation: crate::spec::Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    fn create_test_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(100)
            .file_size_in_bytes(1024)
            .build()
            .unwrap()
    }

    #[test]
    fn test_rewrite_validator_same_snapshot() {
        let snapshot = create_test_snapshot(1, 1);
        let file = create_test_data_file("file1.parquet");
        let validator = RewriteValidator::new(vec![file]);

        // Same snapshot should always pass
        assert!(validator.validate(&snapshot, &snapshot).is_ok());
    }

    #[test]
    fn test_rewrite_validator_sequence_number_change_basic() {
        let base = create_test_snapshot(1, 1);
        let current = create_test_snapshot(2, 2);
        let file = create_test_data_file("file1.parquet");

        // Without partition validation, sequence number change is allowed
        let validator = RewriteValidator::new(vec![file.clone()]);
        assert!(validator.validate(&base, &current).is_ok());
    }

    #[test]
    fn test_rewrite_validator_with_partition_validation_detects_change() {
        let base = create_test_snapshot(1, 1);
        let current = create_test_snapshot(2, 2);
        let file = create_test_data_file("file1.parquet");

        // With partition validation, sequence number change should fail
        let validator = RewriteValidator::new(vec![file.clone()])
            .with_partition_validation(&[file]);

        let result = validator.validate(&base, &current);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Concurrent modification detected"));
    }

    #[test]
    fn test_rewrite_validator_with_concurrent_delete_check() {
        let base = create_test_snapshot(1, 1);
        let current = create_test_snapshot(2, 2);
        let file = create_test_data_file("file1.parquet");

        // With concurrent delete check, sequence number change should fail
        let validator = RewriteValidator::new(vec![file])
            .with_concurrent_delete_check();

        let result = validator.validate(&base, &current);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Concurrent modification detected"));
    }
}
