use ahash::RandomState;
use bytes::Bytes;
use scc::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::constants::Operation;
use crate::core::record::{Record, TreeSlot};
use crate::error::{FeoxError, Result};
use crate::storage::write_buffer::WriteBuffer;

use super::FeoxStore;

impl FeoxStore {
    pub(super) fn update_record_with_ttl(
        &self,
        old_record: &Record,
        value: &[u8],
        timestamp: u64,
        explicit_timestamp: bool,
        ttl_expiry: u64,
    ) -> Result<bool> {
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

        let new_size = self.calculate_record_size(old_record.key.len(), value.len());

        let key_vec = new_record.key.clone();

        let old_record_arc = match self.hash_table.entry(key_vec.clone()) {
            scc::hash_map::Entry::Occupied(mut entry) => {
                let old_record_arc = Arc::clone(entry.get());
                if !std::ptr::eq(old_record, old_record_arc.as_ref())
                    && timestamp <= old_record.retirement_timestamp()
                {
                    return Err(FeoxError::OlderTimestamp);
                }
                if timestamp <= old_record_arc.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }
                let old_size = old_record_arc.calculate_size();
                let reservation = self.reserve_memory(new_size.saturating_sub(old_size))?;
                old_record_arc.link_successor(&new_record);
                old_record_arc.refcount.store(0, Ordering::Release);
                entry.insert(Arc::clone(&new_record));
                self.publish_to_tree(&key_vec, Arc::clone(&new_record));
                self.observe_published_timestamp(&key_vec, timestamp, explicit_timestamp);
                reservation.commit();
                if old_size > new_size {
                    self.release_memory(old_size - new_size);
                }
                self.note_ttl_transition(
                    old_record_arc.ttl_expiry.load(Ordering::Acquire),
                    new_record.ttl_expiry.load(Ordering::Acquire),
                );
                old_record_arc
            }
            scc::hash_map::Entry::Vacant(_entry) => {
                if timestamp <= old_record.retirement_timestamp() {
                    return Err(FeoxError::OlderTimestamp);
                }
                return Err(FeoxError::KeyNotFound);
            }
        };

        if !self.memory_only {
            if self.enable_caching {
                if let Some(ref cache) = self.cache {
                    cache.remove_for_record(&key_vec, &old_record_arc);
                }
            }

            if let Some(ref wb) = self.write_buffer {
                wb.add_replacement(new_record, old_record_arc)?;
            }
        }

        Ok(false)
    }

    /// Update an existing record with a new value (Bytes version for zero-copy).
    pub(super) fn update_record_with_ttl_bytes(
        &self,
        old_record: &Record,
        value: Bytes,
        timestamp: u64,
        explicit_timestamp: bool,
        ttl_expiry: u64,
    ) -> Result<bool> {
        let new_record = if ttl_expiry > 0 && self.enable_ttl {
            Arc::new(Record::new_from_bytes_with_ttl(
                old_record.key.clone(),
                value,
                timestamp,
                ttl_expiry,
            ))
        } else {
            Arc::new(Record::new_from_bytes(
                old_record.key.clone(),
                value,
                timestamp,
            ))
        };

        let new_size = new_record.calculate_size();

        let key_vec = new_record.key.clone();

        let old_record_arc = match self.hash_table.entry(key_vec.clone()) {
            scc::hash_map::Entry::Occupied(mut entry) => {
                let old_record_arc = Arc::clone(entry.get());
                if !std::ptr::eq(old_record, old_record_arc.as_ref())
                    && timestamp <= old_record.retirement_timestamp()
                {
                    return Err(FeoxError::OlderTimestamp);
                }
                if timestamp <= old_record_arc.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }
                let old_size = old_record_arc.calculate_size();
                let reservation = self.reserve_memory(new_size.saturating_sub(old_size))?;
                old_record_arc.link_successor(&new_record);
                old_record_arc.refcount.store(0, Ordering::Release);
                entry.insert(Arc::clone(&new_record));
                self.publish_to_tree(&key_vec, Arc::clone(&new_record));
                self.observe_published_timestamp(&key_vec, timestamp, explicit_timestamp);
                reservation.commit();
                if old_size > new_size {
                    self.release_memory(old_size - new_size);
                }
                self.note_ttl_transition(
                    old_record_arc.ttl_expiry.load(Ordering::Acquire),
                    new_record.ttl_expiry.load(Ordering::Acquire),
                );
                old_record_arc
            }
            scc::hash_map::Entry::Vacant(_entry) => {
                if timestamp <= old_record.retirement_timestamp() {
                    return Err(FeoxError::OlderTimestamp);
                }
                return Err(FeoxError::KeyNotFound);
            }
        };

        if !self.memory_only {
            if self.enable_caching {
                if let Some(ref cache) = self.cache {
                    cache.remove_for_record(&key_vec, &old_record_arc);
                }
            }

            if let Some(ref wb) = self.write_buffer {
                wb.add_replacement(new_record, old_record_arc)?;
            }
        }

        Ok(false)
    }

    /// Get access to hash table (for TTL cleaner)
    pub(crate) fn get_hash_table(&self) -> &HashMap<Vec<u8>, Arc<Record>, RandomState> {
        &self.hash_table
    }

    #[inline]
    pub(super) fn publish_to_tree(&self, key: &[u8], record: Arc<Record>) {
        self.tree
            .get(key)
            .expect("missing ordered index entry")
            .value()
            .store(record);
    }

    #[inline]
    pub(super) fn insert_into_tree(&self, key: Vec<u8>, record: Arc<Record>) {
        self.tree.insert(key, TreeSlot::new(record));
    }

    /// Remove from tree (for TTL cleaner)
    pub(crate) fn remove_from_tree(&self, key: &[u8]) {
        self.tree.remove(key);
    }

    /// Get write buffer (for TTL cleaner)
    pub(crate) fn get_write_buffer(&self) -> Option<&Arc<WriteBuffer>> {
        self.write_buffer.as_ref()
    }

    pub(crate) fn remove_cached(&self, key: &[u8], record: &Arc<Record>) {
        if !self.memory_only && self.enable_caching {
            if let Some(ref cache) = self.cache {
                cache.remove_for_record(key, record);
            }
        }
    }

    pub(crate) fn note_expired_record(&self, record_size: usize) {
        self.stats.record_count.fetch_sub(1, Ordering::Relaxed);
        self.stats
            .memory_usage
            .fetch_sub(record_size, Ordering::Relaxed);
        let _ =
            self.stats
                .keys_with_ttl
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    Some(count.saturating_sub(1))
                });
        self.stats
            .ttl_expired_active
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn retire_expired_if_current(
        &self,
        key: &[u8],
        expected: &Arc<Record>,
        now: u64,
    ) -> Result<bool> {
        let retired = match self.hash_table.entry(key.to_vec()) {
            scc::hash_map::Entry::Occupied(entry) => {
                let record = entry.get();
                let expiry = record.ttl_expiry.load(Ordering::Acquire);
                if !Arc::ptr_eq(record, expected) || expiry == 0 || expiry >= now {
                    return Ok(false);
                }

                let record = Arc::clone(record);
                let record_size = record.calculate_size();
                self.version_clock.observe(key, now);
                record.retired_at.store(now, Ordering::Release);
                record.refcount.store(0, Ordering::Release);
                self.tree.remove(key);
                self.note_expired_record(record_size);
                let _ = entry.remove();
                record
            }
            scc::hash_map::Entry::Vacant(_) => return Ok(false),
        };

        let record = retired;
        self.remove_cached(key, &record);
        if let Some(write_buffer) = self.write_buffer.as_ref() {
            write_buffer.add_write(Operation::Delete, record, 0)?;
        }
        Ok(true)
    }
}
