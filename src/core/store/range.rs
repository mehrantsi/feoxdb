use crossbeam_epoch as epoch;
use std::ops::Bound;

use crate::constants::MAX_KEY_SIZE;
use crate::error::{FeoxError, Result};

use super::FeoxStore;

const RANGE_PREALLOC_LIMIT: usize = 1024;
const RANGE_REPIN_INTERVAL: usize = 256;

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

        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut results = Vec::with_capacity(limit.min(self.tree.len()).min(RANGE_PREALLOC_LIMIT));

        let mut guard = epoch::pin();
        let mut entries_since_repin = 0;
        let mut cursor = self.tree.lower_bound(Bound::Included(start_key));

        while let Some(entry) = cursor {
            if results.len() >= limit || entry.key().as_slice() > end_key {
                break;
            }

            let value = {
                let record = entry.value().load(&guard);
                self.resolve_value_ref(entry.key(), record)
            };
            entries_since_repin += 1;
            if entries_since_repin == RANGE_REPIN_INTERVAL {
                guard.repin();
                entries_since_repin = 0;
            }
            let value = match value {
                Ok(value) => value.to_vec(),
                Err(FeoxError::StaleExtent) | Err(FeoxError::KeyNotFound) => {
                    cursor = entry.next();
                    continue;
                }
                Err(error) => return Err(error),
            };

            results.push((entry.key().clone(), value));
            cursor = entry.next();
        }

        Ok(results)
    }
}
