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

//! RewriteFiles transaction action for atomically replacing data files

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    ManifestWriterBuilder, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::validator::{RewriteValidator, SnapshotValidator};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// RewriteFilesAction atomically removes and adds data files in a single transaction.
///
/// This action enables operations like:
/// - Compaction: Combine many small files into fewer large files
/// - File format optimization: Change compression, encoding, etc.
/// - Sort order changes and Z-ordering
/// - Any operation that replaces existing data files with new ones
///
/// # Example
/// ```no_run
/// # use iceberg::transaction::Transaction;
/// # use iceberg::spec::DataFile;
/// # async fn example(table: iceberg::table::Table, old_files: Vec<DataFile>, new_file: DataFile) -> iceberg::Result<()> {
/// let tx = Transaction::new(&table);
/// let action = tx
///     .rewrite_files()
///     .delete_files(old_files)
///     .add_files(vec![new_file])
///     .validate_from_snapshot(table.metadata().current_snapshot_id());
///
/// let tx = action.apply(tx)?;
/// tx.commit(&catalog).await?;
/// # Ok(())
/// # }
/// ```
pub struct RewriteFilesAction {
    files_to_delete: Vec<DataFile>,
    files_to_add: Vec<DataFile>,
    snapshot_properties: HashMap<String, String>,
    validator: Option<Box<dyn SnapshotValidator>>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    validate_from_snapshot_id: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            files_to_delete: vec![],
            files_to_add: vec![],
            snapshot_properties: HashMap::default(),
            validator: None,
            commit_uuid: None,
            key_metadata: None,
            validate_from_snapshot_id: None,
        }
    }

    /// Specify files to delete from the table
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files_to_delete.extend(files);
        self
    }

    /// Specify files to add to the table
    pub fn add_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files_to_add.extend(files);
        self
    }

    /// Set custom properties for the snapshot summary
    pub fn set_snapshot_properties(mut self, props: HashMap<String, String>) -> Self {
        self.snapshot_properties = props;
        self
    }

    /// Set commit UUID for the snapshot
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Enable validation from a specific snapshot ID
    ///
    /// This validates that no concurrent modifications have occurred since the
    /// specified snapshot. The validation checks that:
    /// - Files being deleted still exist
    /// - No new files were added to affected partitions
    /// - No deletes were applied to files being rewritten
    pub fn validate_from_snapshot(mut self, snapshot_id: Option<i64>) -> Self {
        self.validate_from_snapshot_id = snapshot_id;

        if snapshot_id.is_some() && !self.files_to_delete.is_empty() {
            let validator = RewriteValidator::new(self.files_to_delete.clone())
                .with_partition_validation(&self.files_to_delete)
                .with_concurrent_delete_check();
            self.validator = Some(Box::new(validator));
        }

        self
    }

    /// Enable validation that no concurrent deletes have been applied
    pub fn validate_no_concurrent_deletes(mut self) -> Self {
        if !self.files_to_delete.is_empty() {
            let validator = RewriteValidator::new(self.files_to_delete.clone())
                .with_concurrent_delete_check();
            self.validator = Some(Box::new(validator));
        }
        self
    }

    /// Validate the rewrite operation
    fn validate(&self, table: &Table) -> Result<()> {
        // Ensure we have files to delete and add
        if self.files_to_delete.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RewriteFiles requires at least one file to delete",
            ));
        }

        if self.files_to_add.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RewriteFiles requires at least one file to add",
            ));
        }

        // Validate that files to delete exist in the table
        // This is done via the validator at commit time

        // Validate added files using standard validation
        for data_file in &self.files_to_add {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for rewrite files",
                ));
            }

            if table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
        }

        // If validator is set, run snapshot validation
        if let Some(validator) = &self.validator {
            if let Some(base_snapshot_id) = self.validate_from_snapshot_id {
                if let Some(base_snapshot) = table.metadata().snapshot_by_id(base_snapshot_id) {
                    if let Some(current_snapshot) = table.metadata().current_snapshot() {
                        validator.validate(base_snapshot, current_snapshot)?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate the rewrite operation
        self.validate(table)?;

        // Create a combined list of files for the snapshot producer
        // The files_to_add will be marked as Added
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.files_to_add.clone(),
        );

        // Validate added files
        snapshot_producer.validate_added_data_files()?;

        // Create the rewrite operation with files to delete
        let rewrite_operation = RewriteOperation {
            files_to_delete_paths: self
                .files_to_delete
                .iter()
                .map(|f| f.file_path().to_string())
                .collect(),
        };

        snapshot_producer
            .commit(rewrite_operation, DefaultManifestProcess)
            .await
    }
}

/// Implementation of SnapshotProduceOperation for rewrite operations
struct RewriteOperation {
    files_to_delete_paths: HashSet<String>,
}

impl SnapshotProduceOperation for RewriteOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // This method is not currently used by SnapshotProducer
        // We handle deletes in existing_manifest instead
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            // No existing snapshot, so no manifests to carry forward
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let mut result_manifests = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Load the manifest to check if it contains files we're deleting
            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let mut has_deletes = false;
            let mut modified_entries = Vec::new();

            for entry in manifest.entries() {
                let file_path = entry.file_path();

                if self.files_to_delete_paths.contains(file_path) {
                    // This file is being deleted
                    if entry.is_alive() {
                        // Mark it as deleted
                        has_deletes = true;
                        let deleted_entry = ManifestEntry {
                            status: ManifestStatus::Deleted,
                            snapshot_id: Some(entry.snapshot_id().unwrap_or(snapshot.snapshot_id())),
                            sequence_number: entry.sequence_number,
                            file_sequence_number: entry.file_sequence_number,
                            data_file: entry.data_file().clone(),
                        };
                        modified_entries.push(deleted_entry);
                    }
                    // If already deleted, skip it
                } else if entry.is_alive() {
                    // Keep existing entries that aren't being deleted
                    modified_entries.push((**entry).clone());
                }
            }

            if has_deletes {
                // Need to create a new manifest with the deleted entries
                if !modified_entries.is_empty() {
                    let new_manifest = self
                        .write_manifest_with_entries(
                            snapshot_produce,
                            modified_entries,
                            manifest_file.content,
                        )
                        .await?;
                    result_manifests.push(new_manifest);
                }
                // If all entries are deleted, don't include the manifest
            } else if manifest_file.has_added_files() || manifest_file.has_existing_files() {
                // No files being deleted in this manifest, keep it as-is
                result_manifests.push(manifest_file.clone());
            }
        }

        Ok(result_manifests)
    }
}

impl RewriteOperation {
    /// Write a new manifest file with the given entries
    async fn write_manifest_with_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        entries: Vec<ManifestEntry>,
        content_type: ManifestContentType,
    ) -> Result<ManifestFile> {
        // Create a new manifest writer
        let new_manifest_path = format!(
            "{}/metadata/{}-m{}.avro",
            snapshot_produce.table.metadata().location(),
            Uuid::new_v4(),
            chrono::Utc::now().timestamp_millis(),
        );

        let output_file = snapshot_produce
            .table
            .file_io()
            .new_output(new_manifest_path)?;

        let builder = ManifestWriterBuilder::new(
            output_file,
            None, // snapshot_id will be inherited
            None, // key_metadata - use None for rewritten manifests
            snapshot_produce.table.metadata().current_schema().clone(),
            snapshot_produce
                .table
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone(),
        );

        let mut writer = match snapshot_produce.table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match content_type {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match content_type {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        for entry in entries {
            writer.add_entry(entry)?;
        }

        writer.write_manifest_file().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct, MAIN_BRANCH,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    fn create_test_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_rewrite_files_requires_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let new_file = create_test_data_file("test/new.parquet");

        // Should fail - no files to delete
        let action = tx.rewrite_files().add_files(vec![new_file]);
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("at least one file to delete"));
    }

    #[tokio::test]
    async fn test_rewrite_files_requires_adds() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let old_file = create_test_data_file("test/old.parquet");

        // Should fail - no files to add
        let action = tx.rewrite_files().delete_files(vec![old_file]);
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("at least one file to add"));
    }

    #[tokio::test]
    async fn test_rewrite_files_action_basic() {
        let table = make_v2_minimal_table();

        // Create files for rewrite
        let old_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(512)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let new_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/new.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![old_file])
            .add_files(vec![new_file.clone()]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Check updates and requirements
        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );

        // Check that operation is Replace
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Replace);

        // Verify requirements include UUID and ref match
        assert!(requirements.iter().any(|r| matches!(
            r,
            TableRequirement::UuidMatch { uuid } if *uuid == table.metadata().uuid()
        )));
        assert!(requirements.iter().any(|r| matches!(
            r,
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id }
            if r#ref == MAIN_BRANCH && *snapshot_id == table.metadata().current_snapshot_id()
        )));
    }

    #[tokio::test]
    async fn test_rewrite_files_with_snapshot_properties() {
        let table = make_v2_minimal_table();

        // Create files for rewrite
        let old_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(512)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let new_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/new.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("compaction.reason".to_string(), "small-files".to_string());

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![old_file])
            .add_files(vec![new_file])
            .set_snapshot_properties(snapshot_properties);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("compaction.reason")
                .unwrap(),
            "small-files"
        );
    }
}
