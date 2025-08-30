use bytes::Bytes;
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::mem;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::constants::*;

#[repr(C)]
#[derive(Debug)]
pub struct Record {
    // Cache line 1 (64 bytes) - GET hot path
    // These 3 fields are accessed together on EVERY GET operation
    pub key: Vec<u8>,                              // 24 bytes
    pub value: parking_lot::RwLock<Option<Bytes>>, // ~40 bytes (32 + Option overhead)

    // Cache line 2 (64 bytes) - TTL and metadata
    pub ttl_expiry: AtomicU64, // 8 bytes - checked on every GET
    pub timestamp: u64,        // 8 bytes - checked on updates
    pub value_len: usize,      // 8 bytes - used for size calcs
    pub sector: AtomicU64,     // 8 bytes - for persistence
    pub refcount: AtomicU32,   // 4 bytes - memory management
    pub key_len: u16,          // 2 bytes - used with value_len
    // Total so far: 40 bytes

    // Still in cache line 2 or start of line 3 - cold fields
    pub hash_link: AtomicLink,        // 8 bytes - only for hash ops
    pub cache_ref_bit: AtomicU32,     // 4 bytes - rarely used
    pub cache_access_time: AtomicU64, // 8 bytes - rarely used
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
            // Cache line 1 - GET hot path
            key,
            value: parking_lot::RwLock::new(Some(value_bytes)),

            // Cache line 2 - TTL and metadata
            ttl_expiry: AtomicU64::new(0),
            timestamp,
            value_len,
            sector: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
            key_len,

            // Cold fields
            hash_link: AtomicLink::new(),
            cache_ref_bit: AtomicU32::new(0),
            cache_access_time: AtomicU64::new(0),
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
            // Cache line 1 - GET hot path
            key,
            value: parking_lot::RwLock::new(Some(value)),

            // Cache line 2 - TTL and metadata
            ttl_expiry: AtomicU64::new(0),
            timestamp,
            value_len,
            sector: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
            key_len,

            // Cache line 3 - cold fields
            hash_link: AtomicLink::new(),
            cache_ref_bit: AtomicU32::new(0),
            cache_access_time: AtomicU64::new(0),
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

    pub fn calculate_size(&self) -> usize {
        mem::size_of::<Self>() + self.key.capacity() + self.value_len
    }

    pub fn calculate_disk_size(&self) -> usize {
        let record_size = SECTOR_HEADER_SIZE +
                         mem::size_of::<u16>() + // key_len
                         self.key_len as usize +
                         mem::size_of::<usize>() + // value_len
                         mem::size_of::<u64>() + // timestamp
                         self.value_len;

        // Round up to block size
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
}
