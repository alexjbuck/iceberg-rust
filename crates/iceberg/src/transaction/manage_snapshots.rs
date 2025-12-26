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

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{SnapshotReference, SnapshotRetention};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// ManageSnapshotsAction is a transaction action for managing snapshot references (tags and branches).
///
/// This action allows atomically creating, updating, and deleting snapshot references as part of a transaction,
/// which is essential for preventing race conditions where snapshots could be expired before references are created.
///
/// # Examples
///
/// ```rust,no_run
/// # use iceberg::transaction::Transaction;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let table = todo!();
/// # let catalog = todo!();
/// # let snapshot_id = 12345i64;
/// // Create a tag atomically with data commit
/// let tx = Transaction::new(&table);
/// let action = tx
///     .manage_snapshots()
///     .create_tag("my-checkpoint", snapshot_id);
/// let tx = action.apply(tx)?;
/// tx.commit(&catalog).await?;
/// # Ok(())
/// # }
/// ```
pub struct ManageSnapshotsAction {
    operations: Vec<SnapshotRefOperation>,
}

/// Internal enum representing different snapshot reference operations
#[derive(Debug, Clone)]
enum SnapshotRefOperation {
    /// Create a new tag pointing to a snapshot
    CreateTag {
        name: String,
        snapshot_id: i64,
        max_ref_age_ms: Option<i64>,
    },
    /// Create a new branch pointing to a snapshot
    CreateBranch {
        name: String,
        snapshot_id: i64,
        max_snapshot_age_ms: Option<i64>,
        min_snapshots_to_keep: Option<i32>,
        max_ref_age_ms: Option<i64>,
    },
    /// Remove an existing tag
    RemoveTag {
        name: String,
    },
    /// Remove an existing branch
    RemoveBranch {
        name: String,
    },
    /// Replace an existing branch's snapshot reference
    ReplaceBranch {
        name: String,
        snapshot_id: i64,
    },
    /// Fast-forward a branch to a newer snapshot
    /// This validates that the target snapshot is a descendant of the current branch head
    FastForward {
        name: String,
        to_snapshot_id: i64,
    },
}

impl ManageSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self {
            operations: vec![],
        }
    }

    /// Create a new tag pointing to the specified snapshot.
    ///
    /// # Arguments
    /// * `name` - The name of the tag to create
    /// * `snapshot_id` - The ID of the snapshot to tag
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// # let snapshot_id = 12345i64;
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots().create_tag("v1.0.0", snapshot_id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_tag(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.operations.push(SnapshotRefOperation::CreateTag {
            name: name.into(),
            snapshot_id,
            max_ref_age_ms: None,
        });
        self
    }

    /// Create a new tag with retention policy.
    ///
    /// # Arguments
    /// * `name` - The name of the tag to create
    /// * `snapshot_id` - The ID of the snapshot to tag
    /// * `max_ref_age_ms` - Maximum age in milliseconds before the tag reference expires
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// # let snapshot_id = 12345i64;
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots()
    ///     .create_tag_with_retention("temp-tag", snapshot_id, 86400000); // 1 day
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_tag_with_retention(
        mut self,
        name: impl Into<String>,
        snapshot_id: i64,
        max_ref_age_ms: i64,
    ) -> Self {
        self.operations.push(SnapshotRefOperation::CreateTag {
            name: name.into(),
            snapshot_id,
            max_ref_age_ms: Some(max_ref_age_ms),
        });
        self
    }

    /// Create a new branch pointing to the specified snapshot.
    ///
    /// # Arguments
    /// * `name` - The name of the branch to create
    /// * `snapshot_id` - The ID of the snapshot where the branch should start
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// # let snapshot_id = 12345i64;
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots().create_branch("feature-branch", snapshot_id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_branch(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.operations.push(SnapshotRefOperation::CreateBranch {
            name: name.into(),
            snapshot_id,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
            max_ref_age_ms: None,
        });
        self
    }

    /// Create a new branch with full retention policy.
    ///
    /// # Arguments
    /// * `name` - The name of the branch to create
    /// * `snapshot_id` - The ID of the snapshot where the branch should start
    /// * `max_snapshot_age_ms` - Maximum age of snapshots to keep in the branch
    /// * `min_snapshots_to_keep` - Minimum number of snapshots to keep in the branch
    /// * `max_ref_age_ms` - Maximum age of the branch reference itself
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// # let snapshot_id = 12345i64;
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots()
    ///     .create_branch_with_retention(
    ///         "experiment",
    ///         snapshot_id,
    ///         Some(86400000), // Keep snapshots for 1 day
    ///         Some(5),         // Keep at least 5 snapshots
    ///         Some(604800000)  // Branch expires in 7 days
    ///     );
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_branch_with_retention(
        mut self,
        name: impl Into<String>,
        snapshot_id: i64,
        max_snapshot_age_ms: Option<i64>,
        min_snapshots_to_keep: Option<i32>,
        max_ref_age_ms: Option<i64>,
    ) -> Self {
        self.operations.push(SnapshotRefOperation::CreateBranch {
            name: name.into(),
            snapshot_id,
            max_snapshot_age_ms,
            min_snapshots_to_keep,
            max_ref_age_ms,
        });
        self
    }

    /// Remove an existing tag.
    ///
    /// # Arguments
    /// * `name` - The name of the tag to remove
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots().remove_tag("old-tag");
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_tag(mut self, name: impl Into<String>) -> Self {
        self.operations.push(SnapshotRefOperation::RemoveTag {
            name: name.into(),
        });
        self
    }

    /// Remove an existing branch.
    ///
    /// # Arguments
    /// * `name` - The name of the branch to remove
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots().remove_branch("old-branch");
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_branch(mut self, name: impl Into<String>) -> Self {
        self.operations.push(SnapshotRefOperation::RemoveBranch {
            name: name.into(),
        });
        self
    }

    /// Replace an existing branch to point to a different snapshot.
    ///
    /// # Arguments
    /// * `name` - The name of the branch to update
    /// * `snapshot_id` - The ID of the new snapshot the branch should point to
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// # let new_snapshot_id = 12345i64;
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots().replace_branch("main", new_snapshot_id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn replace_branch(mut self, name: impl Into<String>, snapshot_id: i64) -> Self {
        self.operations.push(SnapshotRefOperation::ReplaceBranch {
            name: name.into(),
            snapshot_id,
        });
        self
    }

    /// Fast-forward a branch to a newer snapshot.
    /// This operation validates that the target snapshot is a descendant of the current branch head.
    ///
    /// # Arguments
    /// * `name` - The name of the branch to fast-forward
    /// * `to_snapshot_id` - The ID of the snapshot to fast-forward to
    ///
    /// # Example
    /// ```rust,no_run
    /// # use iceberg::transaction::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let table = todo!();
    /// # let newer_snapshot_id = 12345i64;
    /// let tx = Transaction::new(&table);
    /// let action = tx.manage_snapshots().fast_forward("main", newer_snapshot_id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn fast_forward(mut self, name: impl Into<String>, to_snapshot_id: i64) -> Self {
        self.operations.push(SnapshotRefOperation::FastForward {
            name: name.into(),
            to_snapshot_id,
        });
        self
    }

    /// Validates that a snapshot exists in the table's snapshot history
    fn validate_snapshot_exists(&self, table: &Table, snapshot_id: i64) -> Result<()> {
        let metadata = table.metadata();

        if metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Snapshot with ID {} does not exist in table history", snapshot_id),
            ));
        }

        Ok(())
    }

    /// Validates that a snapshot is a descendant of another snapshot (for fast-forward operations)
    fn validate_fast_forward(&self, table: &Table, from_snapshot_id: i64, to_snapshot_id: i64) -> Result<()> {
        let metadata = table.metadata();

        // Get the target snapshot
        let to_snapshot = metadata.snapshot_by_id(to_snapshot_id)
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                format!("Target snapshot {} does not exist", to_snapshot_id),
            ))?;

        // Walk back from target to verify it's a descendant
        let mut current_snapshot = to_snapshot;
        loop {
            if current_snapshot.snapshot_id() == from_snapshot_id {
                // Found the from_snapshot in the ancestry - valid fast-forward
                return Ok(());
            }

            match current_snapshot.parent_snapshot_id {
                Some(parent_id) => {
                    current_snapshot = metadata.snapshot_by_id(parent_id)
                        .ok_or_else(|| Error::new(
                            ErrorKind::DataInvalid,
                            format!("Parent snapshot {} not found in history", parent_id),
                        ))?;
                }
                None => {
                    // Reached the root without finding from_snapshot
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot fast-forward: snapshot {} is not an ancestor of {}",
                            from_snapshot_id, to_snapshot_id
                        ),
                    ));
                }
            }
        }
    }
}

#[async_trait]
impl TransactionAction for ManageSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut updates = Vec::new();
        let metadata = table.metadata();

        // Validate and process each operation
        for operation in &self.operations {
            match operation {
                SnapshotRefOperation::CreateTag {
                    name,
                    snapshot_id,
                    max_ref_age_ms,
                } => {
                    // Validate snapshot exists
                    self.validate_snapshot_exists(table, *snapshot_id)?;

                    // Check if tag already exists
                    if let Some(existing_ref) = metadata.refs.get(name) {
                        if !matches!(existing_ref.retention, SnapshotRetention::Tag { .. }) {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!("Reference '{}' already exists as a branch, cannot create tag", name),
                            ));
                        }
                    }

                    let retention = SnapshotRetention::Tag {
                        max_ref_age_ms: *max_ref_age_ms,
                    };

                    updates.push(TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference::new(*snapshot_id, retention),
                    });
                }

                SnapshotRefOperation::CreateBranch {
                    name,
                    snapshot_id,
                    max_snapshot_age_ms,
                    min_snapshots_to_keep,
                    max_ref_age_ms,
                } => {
                    // Validate snapshot exists
                    self.validate_snapshot_exists(table, *snapshot_id)?;

                    // Check if branch already exists
                    if let Some(existing_ref) = metadata.refs.get(name) {
                        if !matches!(existing_ref.retention, SnapshotRetention::Branch { .. }) {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!("Reference '{}' already exists as a tag, cannot create branch", name),
                            ));
                        }
                    }

                    let retention = SnapshotRetention::branch(
                        *min_snapshots_to_keep,
                        *max_snapshot_age_ms,
                        *max_ref_age_ms,
                    );

                    updates.push(TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference::new(*snapshot_id, retention),
                    });
                }

                SnapshotRefOperation::RemoveTag { name } => {
                    // Verify that the reference exists and is a tag
                    if let Some(existing_ref) = metadata.refs.get(name) {
                        if !matches!(existing_ref.retention, SnapshotRetention::Tag { .. }) {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!("Reference '{}' is not a tag, cannot remove as tag", name),
                            ));
                        }
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Tag '{}' does not exist", name),
                        ));
                    }

                    updates.push(TableUpdate::RemoveSnapshotRef {
                        ref_name: name.clone(),
                    });
                }

                SnapshotRefOperation::RemoveBranch { name } => {
                    // Verify that the reference exists and is a branch
                    if let Some(existing_ref) = metadata.refs.get(name) {
                        if !matches!(existing_ref.retention, SnapshotRetention::Branch { .. }) {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!("Reference '{}' is not a branch, cannot remove as branch", name),
                            ));
                        }
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Branch '{}' does not exist", name),
                        ));
                    }

                    updates.push(TableUpdate::RemoveSnapshotRef {
                        ref_name: name.clone(),
                    });
                }

                SnapshotRefOperation::ReplaceBranch { name, snapshot_id } => {
                    // Validate snapshot exists
                    self.validate_snapshot_exists(table, *snapshot_id)?;

                    // Verify that the reference exists and is a branch
                    let existing_ref = metadata.refs.get(name)
                        .ok_or_else(|| Error::new(
                            ErrorKind::DataInvalid,
                            format!("Branch '{}' does not exist", name),
                        ))?;

                    if !matches!(existing_ref.retention, SnapshotRetention::Branch { .. }) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Reference '{}' is not a branch, cannot replace", name),
                        ));
                    }

                    // Keep the existing retention policy
                    updates.push(TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference::new(*snapshot_id, existing_ref.retention.clone()),
                    });
                }

                SnapshotRefOperation::FastForward { name, to_snapshot_id } => {
                    // Validate target snapshot exists
                    self.validate_snapshot_exists(table, *to_snapshot_id)?;

                    // Get the current branch reference
                    let existing_ref = metadata.refs.get(name)
                        .ok_or_else(|| Error::new(
                            ErrorKind::DataInvalid,
                            format!("Branch '{}' does not exist", name),
                        ))?;

                    if !matches!(existing_ref.retention, SnapshotRetention::Branch { .. }) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Reference '{}' is not a branch, cannot fast-forward", name),
                        ));
                    }

                    // Validate that this is a valid fast-forward
                    self.validate_fast_forward(table, existing_ref.snapshot_id, *to_snapshot_id)?;

                    // Keep the existing retention policy
                    updates.push(TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference::new(*to_snapshot_id, existing_ref.retention.clone()),
                    });
                }
            }
        }

        // Add requirement to ensure table UUID matches (optimistic concurrency)
        let requirements = vec![TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::SnapshotRetention;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_create_tag() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // Get the current snapshot ID
        let snapshot_id = table.metadata().current_snapshot_id.unwrap();

        let action = tx.manage_snapshots().create_tag("my-tag", snapshot_id);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Verify updates
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef { ref_name, reference } => {
                assert_eq!(ref_name, "my-tag");
                assert_eq!(reference.snapshot_id, snapshot_id);
                assert!(matches!(reference.retention, SnapshotRetention::Tag { .. }));
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }

        // Verify requirements
        assert_eq!(requirements.len(), 1);
        assert!(matches!(requirements[0], TableRequirement::UuidMatch { .. }));
    }

    #[tokio::test]
    async fn test_create_tag_with_retention() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let snapshot_id = table.metadata().current_snapshot_id.unwrap();
        let max_age = 86400000i64; // 1 day

        let action = tx.manage_snapshots()
            .create_tag_with_retention("temp-tag", snapshot_id, max_age);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        match &updates[0] {
            TableUpdate::SetSnapshotRef { ref_name, reference } => {
                assert_eq!(ref_name, "temp-tag");
                match &reference.retention {
                    SnapshotRetention::Tag { max_ref_age_ms } => {
                        assert_eq!(*max_ref_age_ms, Some(max_age));
                    }
                    _ => panic!("Expected Tag retention"),
                }
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_create_branch() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let snapshot_id = table.metadata().current_snapshot_id.unwrap();

        let action = tx.manage_snapshots().create_branch("feature-branch", snapshot_id);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef { ref_name, reference } => {
                assert_eq!(ref_name, "feature-branch");
                assert_eq!(reference.snapshot_id, snapshot_id);
                assert!(matches!(reference.retention, SnapshotRetention::Branch { .. }));
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_create_branch_with_retention() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let snapshot_id = table.metadata().current_snapshot_id.unwrap();

        let action = tx.manage_snapshots().create_branch_with_retention(
            "experiment",
            snapshot_id,
            Some(86400000),  // max_snapshot_age_ms
            Some(5),          // min_snapshots_to_keep
            Some(604800000), // max_ref_age_ms
        );
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        match &updates[0] {
            TableUpdate::SetSnapshotRef { ref_name, reference } => {
                assert_eq!(ref_name, "experiment");
                match &reference.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep,
                        max_snapshot_age_ms,
                        max_ref_age_ms,
                    } => {
                        assert_eq!(*min_snapshots_to_keep, Some(5));
                        assert_eq!(*max_snapshot_age_ms, Some(86400000));
                        assert_eq!(*max_ref_age_ms, Some(604800000));
                    }
                    _ => panic!("Expected Branch retention"),
                }
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_multiple_operations() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let snapshot_id = table.metadata().current_snapshot_id.unwrap();

        let action = tx.manage_snapshots()
            .create_tag("tag1", snapshot_id)
            .create_tag("tag2", snapshot_id)
            .create_branch("branch1", snapshot_id);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should have 3 updates
        assert_eq!(updates.len(), 3);
    }

    #[tokio::test]
    async fn test_validation_snapshot_not_exists() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let non_existent_snapshot_id = 999999i64;

        let action = tx.manage_snapshots().create_tag("my-tag", non_existent_snapshot_id);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("does not exist"));
        }
    }
}

/// Integration tests that use a real catalog
#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, SnapshotRetention, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_atomic_append_and_tag() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // First, create an initial snapshot
        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/initial.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file1]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Get the snapshot ID from the first commit
        let first_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Now create a second transaction that:
        // 1. Appends more data (creating a new snapshot)
        // 2. Tags the FIRST snapshot (atomically in same commit)
        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file2]);
        let tx = action.apply(tx).unwrap();

        // Tag the previous snapshot atomically with the new append
        let action = tx.manage_snapshots().create_tag("checkpoint-1", first_snapshot_id);
        let tx = action.apply(tx).unwrap();

        // Commit atomically
        let table = tx.commit(&catalog).await.unwrap();

        // Verify the new snapshot exists (different from the tagged one)
        let current_snapshot = table.metadata().current_snapshot().unwrap();
        assert_ne!(current_snapshot.snapshot_id(), first_snapshot_id);

        // Verify tag exists and points to the FIRST snapshot
        let tag_ref = table.metadata().refs.get("checkpoint-1").unwrap();
        assert_eq!(tag_ref.snapshot_id, first_snapshot_id);
        assert!(matches!(tag_ref.retention, SnapshotRetention::Tag { .. }));
    }

    #[tokio::test]
    async fn test_atomic_append_with_multiple_tags_and_branches() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Create initial snapshot
        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/initial2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file1]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Create transaction with append and multiple tags/branches
        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(200)
            .record_count(20)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file2]);
        let tx = action.apply(tx).unwrap();

        // Add multiple tags and branches for the existing snapshot
        let action = tx
            .manage_snapshots()
            .create_tag("tag-1", snapshot_id)
            .create_tag("tag-2", snapshot_id)
            .create_branch("branch-1", snapshot_id)
            .create_branch_with_retention(
                "branch-2",
                snapshot_id,
                Some(86400000),
                Some(5),
                Some(604800000),
            );
        let tx = action.apply(tx).unwrap();

        // Commit atomically
        let table = tx.commit(&catalog).await.unwrap();

        // Verify all references exist
        assert!(table.metadata().refs.contains_key("tag-1"));
        assert!(table.metadata().refs.contains_key("tag-2"));
        assert!(table.metadata().refs.contains_key("branch-1"));
        assert!(table.metadata().refs.contains_key("branch-2"));

        // Verify retention policies
        let branch_2_ref = table.metadata().refs.get("branch-2").unwrap();
        match &branch_2_ref.retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
            } => {
                assert_eq!(*min_snapshots_to_keep, Some(5));
                assert_eq!(*max_snapshot_age_ms, Some(86400000));
                assert_eq!(*max_ref_age_ms, Some(604800000));
            }
            _ => panic!("Expected branch retention"),
        }
    }

    #[tokio::test]
    async fn test_create_tag_with_snapshot_properties() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Create a data file with snapshot properties (e.g., Kafka offsets)
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/kafka_data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(150)
            .record_count(15)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        // Add Kafka offset metadata
        let mut snapshot_props = HashMap::new();
        snapshot_props.insert(
            "kafka.offsets".to_string(),
            r#"{"topic1:0": 12345}"#.to_string(),
        );

        // Create transaction with append and snapshot properties
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file])
            .set_snapshot_properties(snapshot_props);
        let tx = action.apply(tx).unwrap();

        // Commit to get the snapshot ID
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Now create another transaction to tag this snapshot
        let tx = Transaction::new(&table);
        let action = tx
            .manage_snapshots()
            .create_tag("kafka-checkpoint", snapshot_id);
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&catalog).await.unwrap();

        // Verify tag exists
        let tag_ref = table.metadata().refs.get("kafka-checkpoint").unwrap();
        assert_eq!(tag_ref.snapshot_id, snapshot_id);

        // Verify snapshot has the properties
        let snapshot = table.metadata().snapshot_by_id(snapshot_id).unwrap();
        assert!(snapshot
            .summary()
            .additional_properties
            .contains_key("kafka.offsets"));
    }

    #[tokio::test]
    async fn test_remove_tag_and_branch() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Create initial snapshot with tags
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/remove_test.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![data_file]);
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&catalog).await.unwrap();

        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Create tags and branches for this snapshot
        let tx = Transaction::new(&table);
        let action = tx
            .manage_snapshots()
            .create_tag("temp-tag", snapshot_id)
            .create_branch("temp-branch", snapshot_id);
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&catalog).await.unwrap();

        // Verify references exist
        assert!(table.metadata().refs.contains_key("temp-tag"));
        assert!(table.metadata().refs.contains_key("temp-branch"));

        // Now remove them
        let tx = Transaction::new(&table);
        let action = tx
            .manage_snapshots()
            .remove_tag("temp-tag")
            .remove_branch("temp-branch");
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&catalog).await.unwrap();

        // Verify references are gone
        assert!(!table.metadata().refs.contains_key("temp-tag"));
        assert!(!table.metadata().refs.contains_key("temp-branch"));
    }
}
