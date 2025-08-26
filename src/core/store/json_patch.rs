use crate::error::{FeoxError, Result};

use super::FeoxStore;

impl FeoxStore {
    /// Apply a JSON patch to a value.
    ///
    /// Uses RFC 6902 JSON Patch format to modify specific fields in a JSON document.
    /// Both the existing value and the patch must be valid JSON.
    ///
    /// # Arguments
    ///
    /// * `key` - The key containing the JSON document to patch
    /// * `patch` - JSON Patch operations in RFC 6902 format
    /// * `timestamp` - Optional timestamp for conflict resolution
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the update was applied.
    ///
    /// # Errors
    ///
    /// * `KeyNotFound` - Key does not exist
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    /// * `JsonPatchError` - Invalid JSON document or patch format
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// // Insert initial JSON value
    /// let initial = br#"{"name":"Alice","age":30}"#;
    /// store.insert(b"user:1", initial)?;
    ///
    /// // Apply JSON patch to update age
    /// let patch = br#"[{"op":"replace","path":"/age","value":31}]"#;
    /// store.json_patch(b"user:1", patch)?;
    ///
    /// // Value now has age updated to 31
    /// let updated = store.get(b"user:1")?;
    /// assert_eq!(updated.len(), initial.len()); // Same length, just age changed
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_patch(&self, key: &[u8], patch: &[u8]) -> Result<()> {
        self.json_patch_with_timestamp(key, patch, None)
    }

    /// Apply JSON patch with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control.
    /// Most users should use `json_patch()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key whose value to patch
    /// * `patch` - JSON Patch array (RFC 6902)
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn json_patch_with_timestamp(
        &self,
        key: &[u8],
        patch: &[u8],
        timestamp: Option<u64>,
    ) -> Result<()> {
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };
        self.validate_key(key)?;

        // Get the value and release the lock immediately
        let current_value = {
            let record = self.hash_table.get(key).ok_or(FeoxError::KeyNotFound)?;

            if timestamp < record.timestamp {
                return Err(FeoxError::OlderTimestamp);
            }

            if let Some(val) = record.get_value() {
                val.to_vec()
            } else {
                self.load_value_from_disk(&record)?
            }
        };

        let new_value = crate::utils::json_patch::apply_json_patch(&current_value, patch)?;

        // Now update without holding any references
        self.insert_with_timestamp(key, &new_value, Some(timestamp))
    }
}
