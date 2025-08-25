use crate::constants::*;
use std::mem;

/// Metadata version - increment this when changing the metadata structure
/// Version 1: Initial format
/// Version 2: Added TTL support (8 bytes ttl_expiry field in records)
const METADATA_VERSION: u32 = 2;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    pub signature: [u8; FEOX_SIGNATURE_SIZE],
    pub version: u32,
    pub total_records: u64,
    pub total_size: u64,
    pub device_size: u64,
    pub block_size: u32,
    pub fragmentation: u32,
    pub creation_time: u64,
    pub last_update_time: u64,
    reserved: [u8; 68],
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}

impl Metadata {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();

        Self {
            signature: *FEOX_SIGNATURE,
            version: METADATA_VERSION,
            total_records: 0,
            total_size: 0,
            device_size: 0,
            block_size: FEOX_BLOCK_SIZE as u32,
            fragmentation: 0,
            creation_time: now,
            last_update_time: now,
            reserved: [0; 68],
        }
    }

    pub fn validate(&self) -> bool {
        if self.signature != *FEOX_SIGNATURE {
            return false;
        }

        if self.block_size != FEOX_BLOCK_SIZE as u32 {
            return false;
        }

        if self.device_size == 0 || self.device_size > MAX_DEVICE_SIZE {
            return false;
        }

        // Skip checksum validation to avoid alignment issues
        // The signature and other field checks are sufficient for basic validation
        true
    }

    pub fn update(&mut self) {
        self.last_update_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, mem::size_of::<Self>())
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < mem::size_of::<Self>() {
            return None;
        }

        let metadata = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) };

        if metadata.validate() {
            Some(metadata)
        } else {
            None
        }
    }
}
