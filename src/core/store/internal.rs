use scc::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::constants::Operation;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};
use crate::storage::write_buffer::WriteBuffer;

use super::FeoxStore;

impl FeoxStore {
    pub(super) fn update_record_with_ttl(
        &self,
        old_record: &Record,
        value: &[u8],
        timestamp: u64,
        ttl_expiry: u64,
    ) -> Result<()> {
        let new_record = if ttl_expiry > 0 && self.enable_ttl {
            Arc::new(Record::new_with_timestamp_ttl(
                old_record.key.clone(),
                value.to_vec(),
                timestamp,
                ttl_expiry,
            ))
        } else {
            Arc::new(Record::new(
                old_record.key.clone(),
                value.to_vec(),
                timestamp,
            ))
        };

        let old_value_len = old_record.value_len;
        let old_size = old_record.calculate_size();
        let new_size = self.calculate_record_size(old_record.key.len(), value.len());

        let old_record_arc =
            if let Some(entry) = self.hash_table.read(&old_record.key, |_, v| v.clone()) {
                entry
            } else {
                return Err(FeoxError::KeyNotFound);
            };

        let key_vec = new_record.key.clone();

        self.hash_table
            .upsert(key_vec.clone(), Arc::clone(&new_record));
        self.tree.insert(key_vec.clone(), Arc::clone(&new_record));

        if new_size > old_size {
            self.stats
                .memory_usage
                .fetch_add(new_size - old_size, Ordering::AcqRel);
        } else {
            self.stats
                .memory_usage
                .fetch_sub(old_size - new_size, Ordering::AcqRel);
        }

        // Only do cache and persistence operations if not in memory-only mode
        if !self.memory_only {
            if self.enable_caching {
                if let Some(ref cache) = self.cache {
                    cache.remove(&key_vec);
                }
            }

            if let Some(ref wb) = self.write_buffer {
                if let Err(e) =
                    wb.add_write(Operation::Update, Arc::clone(&new_record), old_value_len)
                {
                    // Data operation succeeded in memory
                    let _ = e;
                }

                if let Err(e) = wb.add_write(Operation::Delete, old_record_arc, old_value_len) {
                    // Data operation succeeded in memory
                    let _ = e;
                }
            }
        }

        Ok(())
    }

    /// Get access to hash table (for TTL cleaner)
    pub(crate) fn get_hash_table(&self) -> &HashMap<Vec<u8>, Arc<Record>> {
        &self.hash_table
    }

    /// Remove from tree (for TTL cleaner)
    pub(crate) fn remove_from_tree(&self, key: &[u8]) {
        self.tree.remove(key);
    }

    /// Get write buffer (for TTL cleaner)
    pub(crate) fn get_write_buffer(&self) -> Option<&Arc<WriteBuffer>> {
        self.write_buffer.as_ref()
    }
}
