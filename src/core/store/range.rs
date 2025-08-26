use crate::constants::MAX_KEY_SIZE;
use crate::error::{FeoxError, Result};

use super::FeoxStore;

impl FeoxStore {
    /// Perform a range query on the store.
    ///
    /// Returns all key-value pairs where the key is >= `start_key` and <= `end_key`.
    /// Both bounds are inclusive.
    ///
    /// # Arguments
    ///
    /// * `start_key` - Inclusive lower bound
    /// * `end_key` - Inclusive upper bound
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Returns a vector of (key, value) pairs in sorted order.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// store.insert(b"user:001", b"Alice")?;
    /// store.insert(b"user:002", b"Bob")?;
    /// store.insert(b"user:003", b"Charlie")?;
    /// store.insert(b"user:004", b"David")?;
    ///
    /// // Get users 001 through 003 (inclusive)
    /// let results = store.range_query(b"user:001", b"user:003", 10)?;
    /// assert_eq!(results.len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_query(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start_key.len() > MAX_KEY_SIZE || end_key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        let mut results = Vec::new();

        for entry in self.tree.range(start_key.to_vec()..=end_key.to_vec()) {
            if results.len() >= limit {
                break;
            }

            let record = entry.value();
            let value = if let Some(val) = record.get_value() {
                val.to_vec()
            } else {
                self.load_value_from_disk(record)?
            };

            results.push((entry.key().clone(), value));
        }

        Ok(results)
    }
}
