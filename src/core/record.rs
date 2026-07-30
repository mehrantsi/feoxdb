use bytes::Bytes;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use crate::constants::*;

const EXTENT_RETIRED: u32 = 1 << 31;
const EXTENT_READERS: u32 = !EXTENT_RETIRED;

/// The ordered index stores one slot per key rather than one node per record
/// generation. An update swaps the slot's pointer under the hash bucket guard
/// instead of removing and reinserting a skiplist node, so a key is never
/// briefly absent from a range query, the two indexes cannot drift apart, and a
/// range scan reads its values here rather than paying a hash lookup per key.
#[derive(Debug)]
pub(crate) struct TreeSlot {
    record: Atomic<Arc<Record>>,
}

impl TreeSlot {
    pub(crate) fn new(record: Arc<Record>) -> Self {
        Self {
            record: Atomic::new(record),
        }
    }

    #[inline]
    pub(crate) fn load<'g>(&'g self, guard: &'g Guard) -> &'g Arc<Record> {
        let record = self.record.load(Ordering::Acquire, guard);
        debug_assert!(!record.is_null());
        unsafe { record.deref() }
    }

    #[inline]
    pub(crate) fn store(&self, record: Arc<Record>) {
        let guard = &epoch::pin();
        let previous = self
            .record
            .swap(Owned::new(record), Ordering::AcqRel, guard);
        if !previous.is_null() {
            unsafe {
                guard.defer_destroy(previous);
            }
        }
    }
}

impl Drop for TreeSlot {
    fn drop(&mut self) {
        let record = mem::replace(&mut self.record, Atomic::null());
        unsafe {
            drop(record.into_owned());
        }
    }
}

pub(crate) struct ExtentReadGuard<'a>(&'a AtomicU32);

impl Drop for ExtentReadGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: parking_lot::RwLock<Option<Bytes>>,
    pub ttl_expiry: AtomicU64,
    pub timestamp: u64,
    pub value_len: usize,
    pub sector: AtomicU64,
    pub refcount: AtomicU32,
    pub key_len: u16,
    pub hash_link: AtomicLink,
    pub cache_ref_bit: AtomicU32,
    pub cache_access_time: AtomicU64,
    pub(crate) retired_at: AtomicU64,
    successor: OnceLock<Arc<Record>>,
    value_source: Option<Weak<Record>>,
    successor_safe: AtomicBool,
    extent_state: AtomicU32,
}

// Custom atomic link for lock-free hash table
pub struct AtomicLink {
    pub next: Atomic<Record>,
}

impl std::fmt::Debug for AtomicLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicLink")
            .field("next", &"<atomic>")
            .finish()
    }
}

impl Default for AtomicLink {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicLink {
    pub fn new() -> Self {
        Self {
            next: Atomic::null(),
        }
    }

    pub fn load<'g>(&self, guard: &'g Guard) -> Option<Shared<'g, Record>> {
        let ptr = self.next.load(Ordering::Acquire, guard);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }

    pub fn store(&self, record: Option<Shared<Record>>, _guard: &Guard) {
        let ptr = record.unwrap_or(Shared::null());
        self.next.store(ptr, Ordering::Release);
    }

    pub fn compare_exchange<'g>(
        &self,
        current: Shared<'g, Record>,
        new: Shared<'g, Record>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Record>, Shared<'g, Record>> {
        self.next
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire, guard)
            .map_err(|e| e.current)
    }
}

unsafe impl Send for Record {}
unsafe impl Sync for Record {}

impl Record {
    pub fn new(key: Vec<u8>, value: Vec<u8>, timestamp: u64) -> Self {
        let key_len = key.len() as u16;
        let value_len = value.len();
        let value_bytes = Bytes::from(value);

        Self {
            key,
            value: parking_lot::RwLock::new(Some(value_bytes)),
            ttl_expiry: AtomicU64::new(0),
            timestamp,
            value_len,
            sector: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
            key_len,
            hash_link: AtomicLink::new(),
            cache_ref_bit: AtomicU32::new(0),
            cache_access_time: AtomicU64::new(0),
            retired_at: AtomicU64::new(0),
            successor: OnceLock::new(),
            value_source: None,
            successor_safe: AtomicBool::new(false),
            extent_state: AtomicU32::new(0),
        }
    }

    pub fn new_with_timestamp(key: Vec<u8>, value: Vec<u8>, timestamp: u64) -> Self {
        Self::new(key, value, timestamp)
    }

    pub fn new_with_timestamp_ttl(
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: u64,
        ttl_expiry: u64,
    ) -> Self {
        let record = Self::new(key, value, timestamp);
        record.ttl_expiry.store(ttl_expiry, Ordering::Release);
        record
    }

    /// Create a new record from a Bytes value (zero-copy)
    pub fn new_from_bytes(key: Vec<u8>, value: Bytes, timestamp: u64) -> Self {
        let key_len = key.len() as u16;
        let value_len = value.len();

        Self {
            key,
            value: parking_lot::RwLock::new(Some(value)),
            ttl_expiry: AtomicU64::new(0),
            timestamp,
            value_len,
            sector: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
            key_len,
            hash_link: AtomicLink::new(),
            cache_ref_bit: AtomicU32::new(0),
            cache_access_time: AtomicU64::new(0),
            retired_at: AtomicU64::new(0),
            successor: OnceLock::new(),
            value_source: None,
            successor_safe: AtomicBool::new(false),
            extent_state: AtomicU32::new(0),
        }
    }

    /// Create a new record from Bytes with TTL
    pub fn new_from_bytes_with_ttl(
        key: Vec<u8>,
        value: Bytes,
        timestamp: u64,
        ttl_expiry: u64,
    ) -> Self {
        let record = Self::new_from_bytes(key, value, timestamp);
        record.ttl_expiry.store(ttl_expiry, Ordering::Release);
        record
    }

    pub(crate) fn new_deferred_with_ttl(
        predecessor: &Arc<Record>,
        timestamp: u64,
        ttl_expiry: u64,
    ) -> Self {
        let key = predecessor.key.clone();
        let key_len = key.len() as u16;
        Self {
            key,
            value: parking_lot::RwLock::new(None),
            ttl_expiry: AtomicU64::new(ttl_expiry),
            timestamp,
            value_len: predecessor.value_len,
            sector: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
            key_len,
            hash_link: AtomicLink::new(),
            cache_ref_bit: AtomicU32::new(0),
            cache_access_time: AtomicU64::new(0),
            retired_at: AtomicU64::new(0),
            successor: OnceLock::new(),
            value_source: Some(Arc::downgrade(predecessor)),
            successor_safe: AtomicBool::new(false),
            extent_state: AtomicU32::new(0),
        }
    }

    pub fn calculate_size(&self) -> usize {
        mem::size_of::<Self>() + self.key.capacity() + self.value_len
    }

    pub fn calculate_disk_size(&self) -> usize {
        let record_size = SECTOR_HEADER_SIZE
            + mem::size_of::<u16>()
            + self.key.len()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + self.value_len;

        record_size.div_ceil(FEOX_BLOCK_SIZE) * FEOX_BLOCK_SIZE
    }

    /// Get value - returns None if value has been offloaded to disk
    #[inline]
    pub fn get_value(&self) -> Option<Bytes> {
        self.value.read().clone()
    }

    /// Clear value from memory
    #[inline]
    pub fn clear_value(&self) {
        *self.value.write() = None;
        std::sync::atomic::fence(Ordering::Release);
    }

    pub(crate) fn value_source(&self) -> Option<Arc<Record>> {
        self.value_source.as_ref().and_then(Weak::upgrade)
    }

    pub fn inc_ref(&self) {
        self.refcount.fetch_add(1, Ordering::AcqRel);
    }

    pub fn dec_ref(&self) -> u32 {
        let old = self.refcount.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Record refcount underflow");
        old - 1
    }

    pub fn ref_count(&self) -> u32 {
        self.refcount.load(Ordering::Acquire)
    }

    pub(crate) fn link_successor(&self, successor: &Arc<Record>) {
        let result = self.successor.set(Arc::clone(successor));
        debug_assert!(result.is_ok());
    }

    pub(crate) fn retirement_timestamp(&self) -> u64 {
        let mut retired_at = self.retired_at.load(Ordering::Acquire);
        let Some(mut current) = self.successor.get().cloned() else {
            return retired_at;
        };

        loop {
            retired_at = retired_at.max(current.retired_at.load(Ordering::Acquire));
            let Some(successor) = current.successor.get().cloned() else {
                return retired_at;
            };
            current = successor;
        }
    }

    pub(crate) fn successor_is_durable_or_deleted(&self) -> bool {
        if self.successor_safe.load(Ordering::Acquire) {
            return true;
        }

        let Some(mut current) = self.successor.get().cloned() else {
            return true;
        };
        let mut path = Vec::new();

        loop {
            if current.sector.load(Ordering::Acquire) > 0
                || current.successor_safe.load(Ordering::Acquire)
            {
                break;
            }

            let Some(successor) = current.successor.get().cloned() else {
                if current.refcount.load(Ordering::Acquire) != 0 {
                    return false;
                }
                match current.successor.get().cloned() {
                    Some(successor) => {
                        path.push(current);
                        current = successor;
                        continue;
                    }
                    None => break,
                }
            };

            path.push(current);
            current = successor;
        }

        self.successor_safe.store(true, Ordering::Release);
        for record in path {
            record.successor_safe.store(true, Ordering::Release);
        }
        true
    }

    pub(crate) fn acquire_extent(&self) -> Option<ExtentReadGuard<'_>> {
        let mut state = self.extent_state.load(Ordering::Acquire);
        loop {
            if state & EXTENT_RETIRED != 0 {
                return None;
            }
            debug_assert!(state & EXTENT_READERS != EXTENT_READERS);
            match self.extent_state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(ExtentReadGuard(&self.extent_state)),
                Err(current) => state = current,
            }
        }
    }

    pub(crate) fn retire_extent(&self) {
        self.extent_state.fetch_or(EXTENT_RETIRED, Ordering::AcqRel);
    }

    pub(crate) fn extent_has_readers(&self) -> bool {
        self.extent_state.load(Ordering::Acquire) & EXTENT_READERS != 0
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        let mut successor = self.successor.take();
        while let Some(record) = successor {
            match Arc::try_unwrap(record) {
                Ok(mut record) => {
                    successor = record.successor.take();
                }
                Err(record) => {
                    drop(record);
                    break;
                }
            }
        }
    }
}
