use crate::constants::*;
use crate::error::{FeoxError, Result};
use std::collections::BTreeMap;

/// Represents a contiguous range of free sectors on disk
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeSpace {
    pub start: u64, // Starting sector
    pub size: u64,  // Size in sectors
}

/// Free space manager using dual RB-trees for efficient allocation and coalescing
/// Matches the kernel implementation's design for correctness
pub struct FreeSpaceManager {
    /// Tree sorted by (size, start) for best-fit allocation
    /// Using composite key ensures uniqueness
    by_size: BTreeMap<(u64, u64), FreeSpace>,

    /// Tree sorted by start address for efficient merging
    by_start: BTreeMap<u64, FreeSpace>,

    /// Total free space in bytes
    total_free: u64,

    /// Device size in bytes (for validation)
    device_size: u64,

    /// Fragmentation percentage (0-100)
    fragmentation_percent: u32,
}

impl FreeSpaceManager {
    /// Create a new free space manager
    pub fn new() -> Self {
        Self {
            by_size: BTreeMap::new(),
            by_start: BTreeMap::new(),
            total_free: 0,
            device_size: 0,
            fragmentation_percent: 0,
        }
    }

    /// Initialize with device size and initial free space
    pub fn initialize(&mut self, device_size: u64) -> Result<()> {
        self.device_size = device_size;

        // Reserve first 16 sectors for metadata (matching FEOX_DATA_START_BLOCK)
        let metadata_sectors = 16;
        let total_sectors = device_size / FEOX_BLOCK_SIZE as u64;

        if total_sectors <= metadata_sectors {
            return Err(FeoxError::InvalidDevice);
        }

        // Add all space after metadata as free
        let free_sectors = total_sectors - metadata_sectors;
        self.insert_free_space(FreeSpace {
            start: metadata_sectors,
            size: free_sectors,
        })?;

        Ok(())
    }

    /// Set device size for bounds checking (used when rebuilding from scan)
    pub fn set_device_size(&mut self, device_size: u64) {
        self.device_size = device_size;
    }

    /// Allocate sectors using best-fit algorithm
    pub fn allocate_sectors(&mut self, sectors_needed: u64) -> Result<u64> {
        if sectors_needed == 0 {
            return Err(FeoxError::InvalidArgument);
        }

        // Find best-fit (smallest space that fits)
        let mut best_fit = None;

        // Search from smallest size upward
        for ((size, start), space) in &self.by_size {
            if *size >= sectors_needed {
                best_fit = Some((*size, *start, space.clone()));
                break; // First fit is best fit since we're sorted by size
            }
        }

        if let Some((size, start, space)) = best_fit {
            // Validate the space
            if !self.is_valid_free_space(&space) {
                return Err(FeoxError::CorruptedData);
            }

            // Remove from both trees
            self.by_size.remove(&(size, start));
            self.by_start.remove(&space.start);

            let allocated_start = space.start;

            // Handle remaining space if any
            if space.size > sectors_needed {
                let remaining = FreeSpace {
                    start: space.start + sectors_needed,
                    size: space.size - sectors_needed,
                };

                // Insert remaining space back
                if let Err(e) = self.insert_free_space(remaining) {
                    // Try to restore original space on error
                    let _ = self.insert_free_space(space);
                    return Err(e);
                }
            }

            self.total_free -= sectors_needed * FEOX_BLOCK_SIZE as u64;
            self.update_fragmentation();

            Ok(allocated_start)
        } else {
            Err(FeoxError::OutOfSpace)
        }
    }

    /// Release sectors back to free space pool with coalescing
    pub fn release_sectors(&mut self, start: u64, count: u64) -> Result<()> {
        if start == 0 || count == 0 {
            return Err(FeoxError::InvalidArgument);
        }

        // Validate bounds
        if !self.is_valid_sector_range(start, count) {
            return Err(FeoxError::InvalidArgument);
        }

        // Try to merge with adjacent spaces
        let merged = self.try_merge_spaces(start, count)?;

        // Insert the merged space (this will update total_free)
        self.insert_free_space(merged)?;

        self.update_fragmentation();

        Ok(())
    }

    /// Try to merge with adjacent free spaces
    fn try_merge_spaces(&mut self, start: u64, size: u64) -> Result<FreeSpace> {
        let end = start + size;
        let mut merged_start = start;
        let mut merged_size = size;

        // Find predecessor (space ending at our start)
        let mut prev = None;
        for (&s, space) in self.by_start.range(..start).rev().take(1) {
            if s + space.size == start {
                prev = Some(space.clone());
            }
        }

        // Find successor (space starting at our end)
        let next = self.by_start.get(&end).cloned();

        // Merge with predecessor if found
        if let Some(prev_space) = prev {
            // Remove from both trees
            self.by_size.remove(&(prev_space.size, prev_space.start));
            self.by_start.remove(&prev_space.start);

            // Subtract the removed space from total_free (will be re-added when inserting merged)
            self.total_free -= prev_space.size * FEOX_BLOCK_SIZE as u64;

            merged_start = prev_space.start;
            merged_size += prev_space.size;
        }

        // Merge with successor if found
        if let Some(next_space) = next {
            // Remove from both trees
            self.by_size.remove(&(next_space.size, next_space.start));
            self.by_start.remove(&next_space.start);

            // Subtract the removed space from total_free (will be re-added when inserting merged)
            self.total_free -= next_space.size * FEOX_BLOCK_SIZE as u64;

            merged_size += next_space.size;
        }

        Ok(FreeSpace {
            start: merged_start,
            size: merged_size,
        })
    }

    /// Insert a free space into both trees
    fn insert_free_space(&mut self, space: FreeSpace) -> Result<()> {
        if space.size == 0 {
            return Err(FeoxError::InvalidArgument);
        }

        // Validate the space
        if !self.is_valid_free_space(&space) {
            return Err(FeoxError::InvalidArgument);
        }

        // Check for duplicates
        if self.by_start.contains_key(&space.start) {
            return Err(FeoxError::DuplicateKey);
        }

        // Update total free space
        self.total_free += space.size * FEOX_BLOCK_SIZE as u64;

        // Insert into both trees
        self.by_size
            .insert((space.size, space.start), space.clone());
        self.by_start.insert(space.start, space);

        Ok(())
    }

    /// Check if a free space is valid
    fn is_valid_free_space(&self, space: &FreeSpace) -> bool {
        // Never allow sector 0 (reserved for metadata)
        if space.start == 0 {
            return false;
        }

        // Check device bounds
        if self.device_size > 0 {
            let device_sectors = self.device_size / FEOX_BLOCK_SIZE as u64;
            if space.start >= device_sectors {
                return false;
            }
            if space.start + space.size > device_sectors {
                return false;
            }
        }

        true
    }

    /// Check if a sector range is valid
    fn is_valid_sector_range(&self, start: u64, count: u64) -> bool {
        if start == 0 || count == 0 {
            return false;
        }

        if self.device_size > 0 {
            let device_sectors = self.device_size / FEOX_BLOCK_SIZE as u64;
            if start >= device_sectors {
                return false;
            }
            if start + count > device_sectors {
                return false;
            }
        }

        true
    }

    /// Update fragmentation metric
    fn update_fragmentation(&mut self) {
        if self.total_free == 0 {
            self.fragmentation_percent = 0;
            return;
        }

        let num_chunks = self.by_start.len() as u64;
        if num_chunks <= 1 {
            self.fragmentation_percent = 0;
            return;
        }

        // Find largest free chunk
        let largest = self
            .by_size
            .iter()
            .next_back()
            .map(|(_, space)| space.size * FEOX_BLOCK_SIZE as u64)
            .unwrap_or(0);

        // Calculate fragmentation as percentage of free space not in largest chunk
        if self.total_free > 0 {
            let fragmented = self.total_free - largest;
            self.fragmentation_percent = ((fragmented * 100) / self.total_free) as u32;
        }
    }

    /// Get total free space in bytes
    pub fn get_total_free(&self) -> u64 {
        self.total_free
    }

    /// Get fragmentation percentage
    pub fn get_fragmentation(&self) -> u32 {
        self.fragmentation_percent
    }

    /// Get number of free chunks
    pub fn get_free_chunks_count(&self) -> usize {
        self.by_start.len()
    }

    /// Get largest free chunk in bytes
    pub fn get_largest_free_chunk(&self) -> u64 {
        self.by_size
            .iter()
            .next_back()
            .map(|(_, space)| space.size * FEOX_BLOCK_SIZE as u64)
            .unwrap_or(0)
    }
}

impl Default for FreeSpaceManager {
    fn default() -> Self {
        Self::new()
    }
}
