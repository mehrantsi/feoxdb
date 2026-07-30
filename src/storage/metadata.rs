use crate::constants::*;
use crate::error::{FeoxError, Result};
use crate::storage::seq_token::crc32c;

/// Metadata version - increment this when changing the metadata structure
/// Version 1: Initial format
/// Version 2: Added TTL support (8 bytes ttl_expiry field in records)
/// Version 3: Checksummed metadata and sector-bound record tokens
const METADATA_VERSION: u32 = 3;
const METADATA_ENCODED_SIZE: usize = 136;
const VERSION_OFFSET: usize = 8;
const TOTAL_RECORDS_OFFSET: usize = 16;
const TOTAL_SIZE_OFFSET: usize = 24;
const DEVICE_SIZE_OFFSET: usize = 32;
const BLOCK_SIZE_OFFSET: usize = 40;
const FRAGMENTATION_OFFSET: usize = 44;
const CREATION_TIME_OFFSET: usize = 48;
const LAST_UPDATE_TIME_OFFSET: usize = 56;
const RESERVED_OFFSET: usize = 64;
const RESERVED_SIZE: usize = 68;
const CHECKSUM_MAGIC: &[u8; 4] = b"FM3C";
const CHECKSUM_OFFSET: usize = 4;
const CHECKSUM_COMPLEMENT_OFFSET: usize = 8;
const CHECKSUM_DATA_OFFSET: usize = 12;
const GENERATION_OFFSET: usize = 12;

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
    reserved: [u8; RESERVED_SIZE],
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

        let mut metadata = Self {
            signature: *FEOX_SIGNATURE,
            version: METADATA_VERSION,
            total_records: 0,
            total_size: 0,
            device_size: 0,
            block_size: FEOX_BLOCK_SIZE as u32,
            fragmentation: 0,
            creation_time: now,
            last_update_time: now,
            reserved: [0; RESERVED_SIZE],
        };
        metadata.refresh_checksum();
        metadata
    }

    pub fn validate(&self) -> bool {
        if self.signature != *FEOX_SIGNATURE {
            return false;
        }

        if self.block_size != FEOX_BLOCK_SIZE as u32 {
            return false;
        }

        if self.version == 0 || self.version > METADATA_VERSION {
            return false;
        }

        if self.device_size == 0 || self.device_size > MAX_DEVICE_SIZE {
            return false;
        }

        let has_checksum = &self.reserved[..CHECKSUM_MAGIC.len()] == CHECKSUM_MAGIC;
        if self.version >= 3 && !has_checksum {
            return false;
        }
        if !has_checksum {
            return true;
        }

        let checksum = u32::from_le_bytes(
            self.reserved[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        let complement = u32::from_le_bytes(
            self.reserved[CHECKSUM_COMPLEMENT_OFFSET..CHECKSUM_COMPLEMENT_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        complement == !checksum && checksum == self.checksum()
    }

    pub fn update(&mut self) {
        self.last_update_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();
        self.refresh_checksum();
    }

    pub fn encode(&self) -> [u8; METADATA_ENCODED_SIZE] {
        let mut bytes = [0; METADATA_ENCODED_SIZE];
        bytes[..FEOX_SIGNATURE_SIZE].copy_from_slice(&self.signature);
        bytes[VERSION_OFFSET..VERSION_OFFSET + 4].copy_from_slice(&self.version.to_le_bytes());
        bytes[TOTAL_RECORDS_OFFSET..TOTAL_RECORDS_OFFSET + 8]
            .copy_from_slice(&self.total_records.to_le_bytes());
        bytes[TOTAL_SIZE_OFFSET..TOTAL_SIZE_OFFSET + 8]
            .copy_from_slice(&self.total_size.to_le_bytes());
        bytes[DEVICE_SIZE_OFFSET..DEVICE_SIZE_OFFSET + 8]
            .copy_from_slice(&self.device_size.to_le_bytes());
        bytes[BLOCK_SIZE_OFFSET..BLOCK_SIZE_OFFSET + 4]
            .copy_from_slice(&self.block_size.to_le_bytes());
        bytes[FRAGMENTATION_OFFSET..FRAGMENTATION_OFFSET + 4]
            .copy_from_slice(&self.fragmentation.to_le_bytes());
        bytes[CREATION_TIME_OFFSET..CREATION_TIME_OFFSET + 8]
            .copy_from_slice(&self.creation_time.to_le_bytes());
        bytes[LAST_UPDATE_TIME_OFFSET..LAST_UPDATE_TIME_OFFSET + 8]
            .copy_from_slice(&self.last_update_time.to_le_bytes());
        bytes[RESERVED_OFFSET..RESERVED_OFFSET + RESERVED_SIZE].copy_from_slice(&self.reserved);
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < METADATA_ENCODED_SIZE {
            return None;
        }

        let metadata = Self {
            signature: bytes[..FEOX_SIGNATURE_SIZE].try_into().ok()?,
            version: u32::from_le_bytes(bytes[VERSION_OFFSET..VERSION_OFFSET + 4].try_into().ok()?),
            total_records: u64::from_le_bytes(
                bytes[TOTAL_RECORDS_OFFSET..TOTAL_RECORDS_OFFSET + 8]
                    .try_into()
                    .ok()?,
            ),
            total_size: u64::from_le_bytes(
                bytes[TOTAL_SIZE_OFFSET..TOTAL_SIZE_OFFSET + 8]
                    .try_into()
                    .ok()?,
            ),
            device_size: u64::from_le_bytes(
                bytes[DEVICE_SIZE_OFFSET..DEVICE_SIZE_OFFSET + 8]
                    .try_into()
                    .ok()?,
            ),
            block_size: u32::from_le_bytes(
                bytes[BLOCK_SIZE_OFFSET..BLOCK_SIZE_OFFSET + 4]
                    .try_into()
                    .ok()?,
            ),
            fragmentation: u32::from_le_bytes(
                bytes[FRAGMENTATION_OFFSET..FRAGMENTATION_OFFSET + 4]
                    .try_into()
                    .ok()?,
            ),
            creation_time: u64::from_le_bytes(
                bytes[CREATION_TIME_OFFSET..CREATION_TIME_OFFSET + 8]
                    .try_into()
                    .ok()?,
            ),
            last_update_time: u64::from_le_bytes(
                bytes[LAST_UPDATE_TIME_OFFSET..LAST_UPDATE_TIME_OFFSET + 8]
                    .try_into()
                    .ok()?,
            ),
            reserved: bytes[RESERVED_OFFSET..RESERVED_OFFSET + RESERVED_SIZE]
                .try_into()
                .ok()?,
        };

        if metadata.validate() {
            Some(metadata)
        } else {
            None
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        u64::from_le_bytes(
            self.reserved[GENERATION_OFFSET..GENERATION_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }

    pub(crate) fn advance_generation(&mut self) -> Result<()> {
        let generation = self
            .generation()
            .checked_add(1)
            .ok_or(FeoxError::InvalidMetadata)?;
        self.reserved[GENERATION_OFFSET..GENERATION_OFFSET + 8]
            .copy_from_slice(&generation.to_le_bytes());
        self.refresh_checksum();
        Ok(())
    }

    fn refresh_checksum(&mut self) {
        self.reserved[..CHECKSUM_MAGIC.len()].copy_from_slice(CHECKSUM_MAGIC);
        let checksum = self.checksum();
        self.reserved[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]
            .copy_from_slice(&checksum.to_le_bytes());
        self.reserved[CHECKSUM_COMPLEMENT_OFFSET..CHECKSUM_COMPLEMENT_OFFSET + 4]
            .copy_from_slice(&(!checksum).to_le_bytes());
    }

    fn checksum(&self) -> u32 {
        let mut checksum = crc32c(0, &self.signature);
        checksum = crc32c(checksum, &self.version.to_le_bytes());
        checksum = crc32c(checksum, &self.total_records.to_le_bytes());
        checksum = crc32c(checksum, &self.total_size.to_le_bytes());
        checksum = crc32c(checksum, &self.device_size.to_le_bytes());
        checksum = crc32c(checksum, &self.block_size.to_le_bytes());
        checksum = crc32c(checksum, &self.fragmentation.to_le_bytes());
        checksum = crc32c(checksum, &self.creation_time.to_le_bytes());
        checksum = crc32c(checksum, &self.last_update_time.to_le_bytes());
        crc32c(checksum, &self.reserved[CHECKSUM_DATA_OFFSET..])
    }
}
