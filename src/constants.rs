use std::time::Duration;

// Size units
pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;

// Size limits
pub const MAX_KEY_SIZE: usize = 100 * KB;
pub const MAX_VALUE_SIZE: usize = 4 * MB;
pub const DEFAULT_MAX_MEMORY: usize = 4 * GB;

// Memory alignment
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_MASK: usize = PAGE_SIZE - 1;
pub const MAX_CONNECTIONS: usize = 1000;

// Hash table configuration
pub const DEFAULT_HASH_BITS: u32 = 23;
pub const KV_STORE_BUCKETS: usize = 1 << DEFAULT_HASH_BITS; // 8M buckets
pub const HASH_SPINLOCKS_COUNT: usize = 256;
pub const CACHE_BUCKETS: usize = 16384;

// Block and sector sizes
pub const FEOX_BLOCK_SIZE: usize = 4096;
pub const SECTOR_SHIFT: usize = 9;
pub const SECTOR_SIZE: usize = 512;
pub const SECTOR_HEADER_SIZE: usize = 4;
pub const SECTOR_MARKER: u16 = 0xABCD;

// Metadata
pub const FEOX_SIGNATURE: &[u8; 8] = b"FEOX_SIG";
pub const FEOX_SIGNATURE_SIZE: usize = 8;
pub const FEOX_METADATA_BLOCK: u64 = 0;
pub const FEOX_METADATA_SIZE: usize = PAGE_SIZE;
pub const FEOX_DATA_START_BLOCK: u64 = 16;
pub const FEOX_WRITE_BUFFER_SIZE: usize = 16 * MB;
pub const FEOX_ALIGNMENT: usize = 512;

// Memory allocation thresholds
pub const KMALLOC_LIMIT: usize = 8192;
pub const LARGE_ALLOC_THRESHOLD: usize = 8192;

// Write buffer configuration
pub const WRITE_BUFFER_SIZE: usize = 1024;
pub const WRITE_BUFFER_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
pub const WORK_TIMEOUT_MS: u64 = 5000;
pub const WRITE_BUFFER_WORKER_RATIO: usize = 2;

// Cache configuration
pub const CACHE_HIGH_WATERMARK_MB: usize = 100;
pub const CACHE_LOW_WATERMARK_MB: usize = 50;
pub const CACHE_MAX_SIZE: usize = GB; // 1GB max cache
pub const CACHE_ADJUST_INTERVAL: Duration = Duration::from_secs(5);
pub const CACHE_CLOCK_HAND_ADVANCE: usize = 100;

// Tenant configuration
pub const TENANT_ID_SIZE: usize = 4;

// Device limits
pub const MAX_DEVICE_SIZE: u64 = 1u64 << 40; // 1TB
pub const DEFAULT_DEVICE_SIZE: u64 = GB as u64; // 1GB default

// io_uring configuration
pub const IOURING_QUEUE_SIZE: u32 = 256;
pub const IOURING_MAX_BATCH: usize = 128; // Safely under queue size
pub const IOURING_SQPOLL_IDLE_MS: u32 = 1000;

// Operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Update,
    Delete,
    Get,
    PartialUpdate,
}

// POSIX error codes
pub const EINVAL: i32 = 22;
pub const ENOMEM: i32 = 12;
pub const ENOENT: i32 = 2;
pub const EEXIST: i32 = 17;
pub const EAGAIN: i32 = 11;
pub const ENOSPC: i32 = 28;
pub const EMSGSIZE: i32 = 90;
pub const EOVERFLOW: i32 = 75;
pub const EIO: i32 = 5;
pub const EFAULT: i32 = 14;
pub const ENOTEMPTY: i32 = 39;
pub const ENODEV: i32 = 19;
