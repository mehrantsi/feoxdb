use crate::constants::*;
use crate::error::FeoxError;
use crate::storage::free_space::FreeSpaceManager;

#[test]
fn test_initialization() {
    let mut manager = FreeSpaceManager::new();

    // Initialize with 1GB device
    let device_size = DEFAULT_DEVICE_SIZE;
    manager.initialize(device_size).unwrap();

    // Should have one free chunk starting at sector 16
    assert_eq!(manager.get_free_chunks_count(), 1);

    // Total free should be device size minus metadata (16 sectors)
    let expected_free = device_size - (16 * FEOX_BLOCK_SIZE as u64);
    assert_eq!(manager.get_total_free(), expected_free);
}

#[test]
fn test_allocate_and_release() {
    let mut manager = FreeSpaceManager::new();
    manager.initialize(DEFAULT_DEVICE_SIZE).unwrap();

    // Allocate 100 sectors
    let start = manager.allocate_sectors(100).unwrap();
    assert_eq!(start, 16); // Should start right after metadata

    // Allocate another 50 sectors
    let start2 = manager.allocate_sectors(50).unwrap();
    assert_eq!(start2, 116); // Should be contiguous

    // Release the first allocation
    manager.release_sectors(16, 100).unwrap();

    // Should have 2 free chunks now
    assert_eq!(manager.get_free_chunks_count(), 2);

    // Release the second allocation - should coalesce
    manager.release_sectors(116, 50).unwrap();

    // Should be back to 1 chunk
    assert_eq!(manager.get_free_chunks_count(), 1);
}

#[test]
fn test_best_fit_allocation() {
    let mut manager = FreeSpaceManager::new();

    // Manually add free spaces of different sizes
    manager.set_device_size(10000 * FEOX_BLOCK_SIZE as u64);

    // Add spaces: 100 sectors at 1000, 50 sectors at 2000, 75 sectors at 3000
    manager.release_sectors(1000, 100).unwrap();
    manager.release_sectors(2000, 50).unwrap();
    manager.release_sectors(3000, 75).unwrap();

    // Allocate 45 sectors - should use the 50-sector space (best fit)
    let allocated = manager.allocate_sectors(45).unwrap();
    assert_eq!(allocated, 2000);

    // Should have 3 chunks now (100, 75, and remaining 5)
    assert_eq!(manager.get_free_chunks_count(), 3);
}

#[test]
fn test_coalescing() {
    let mut manager = FreeSpaceManager::new();
    manager.set_device_size(10000 * FEOX_BLOCK_SIZE as u64);

    // Release adjacent sectors - should coalesce
    manager.release_sectors(100, 50).unwrap();
    manager.release_sectors(150, 50).unwrap();
    manager.release_sectors(200, 50).unwrap();

    // Should have just 1 chunk of 150 sectors
    assert_eq!(manager.get_free_chunks_count(), 1);
    assert_eq!(manager.get_total_free(), 150 * FEOX_BLOCK_SIZE as u64);
}

#[test]
fn test_invalid_operations() {
    let mut manager = FreeSpaceManager::new();
    manager.initialize(DEFAULT_DEVICE_SIZE).unwrap();

    // Try to allocate 0 sectors
    assert!(manager.allocate_sectors(0).is_err());

    // Try to release sector 0 (reserved)
    assert!(manager.release_sectors(0, 10).is_err());

    // Try to release 0 sectors
    assert!(manager.release_sectors(100, 0).is_err());

    // Try to allocate more than available
    let huge = DEFAULT_DEVICE_SIZE / FEOX_BLOCK_SIZE as u64 + 1;
    assert!(manager.allocate_sectors(huge).is_err());
}

#[test]
fn test_fragmentation_calculation() {
    let mut manager = FreeSpaceManager::new();
    manager.set_device_size(10000 * FEOX_BLOCK_SIZE as u64);

    // Single chunk - no fragmentation
    manager.release_sectors(100, 1000).unwrap();
    assert_eq!(manager.get_fragmentation(), 0);

    // Add another chunk - now fragmented
    manager.release_sectors(2000, 100).unwrap();
    assert!(manager.get_fragmentation() > 0);

    // More chunks = more fragmentation
    manager.release_sectors(3000, 50).unwrap();
    manager.release_sectors(4000, 25).unwrap();
    // With chunks of 1000, 100, 50, 25: largest is 1000, total is 1175
    // Fragmentation = (175/1175)*100 = ~15%
    assert!(manager.get_fragmentation() > 10); // Some fragmentation
}

#[test]
fn test_largest_free_chunk() {
    let mut manager = FreeSpaceManager::new();
    manager.set_device_size(10000 * FEOX_BLOCK_SIZE as u64);

    manager.release_sectors(100, 50).unwrap();
    manager.release_sectors(200, 100).unwrap();
    manager.release_sectors(400, 75).unwrap();

    assert_eq!(
        manager.get_largest_free_chunk(),
        100 * FEOX_BLOCK_SIZE as u64
    );
}

#[test]
fn test_out_of_space() {
    let mut manager = FreeSpaceManager::new();
    manager.initialize(1024 * FEOX_BLOCK_SIZE as u64).unwrap();

    // Allocate most of the space
    let allocated = manager.allocate_sectors(1000).unwrap();
    assert!(allocated >= 16); // After metadata

    // Try to allocate more than remaining
    let result = manager.allocate_sectors(100);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OutOfSpace));
}

#[test]
fn test_device_bounds_checking() {
    let mut manager = FreeSpaceManager::new();
    manager.set_device_size(1000 * FEOX_BLOCK_SIZE as u64);

    // Try to release sectors beyond device size
    let result = manager.release_sectors(2000, 10);
    assert!(result.is_err());

    // Try to release sectors that would extend beyond device
    let result = manager.release_sectors(995, 10);
    assert!(result.is_err());
}

#[test]
fn test_concurrent_allocation() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let manager = Arc::new(Mutex::new({
        let mut m = FreeSpaceManager::new();
        m.initialize(DEFAULT_DEVICE_SIZE).unwrap();
        m
    }));

    let mut handles = vec![];
    let allocated_sectors = Arc::new(Mutex::new(Vec::new()));

    // Multiple threads allocating
    for _ in 0..10 {
        let manager_clone = Arc::clone(&manager);
        let sectors_clone = Arc::clone(&allocated_sectors);

        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                if let Ok(sector) = manager_clone.lock().unwrap().allocate_sectors(10) {
                    sectors_clone.lock().unwrap().push(sector);
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All allocated sectors should be unique
    let sectors = allocated_sectors.lock().unwrap();
    let mut sorted = sectors.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), sectors.len()); // No duplicates
}

#[test]
fn test_coalesce_with_predecessor() {
    let mut manager = FreeSpaceManager::new();
    manager.set_device_size(10000 * FEOX_BLOCK_SIZE as u64);

    // Create two adjacent free spaces
    manager.release_sectors(100, 50).unwrap();
    manager.release_sectors(200, 50).unwrap();

    // Release space that connects them
    manager.release_sectors(150, 50).unwrap();

    // Should have coalesced into one
    assert_eq!(manager.get_free_chunks_count(), 1);
    assert_eq!(manager.get_total_free(), 150 * FEOX_BLOCK_SIZE as u64);
}

#[test]
fn test_coalesce_with_successor() {
    let mut manager = FreeSpaceManager::new();
    manager.set_device_size(10000 * FEOX_BLOCK_SIZE as u64);

    // Create free space
    manager.release_sectors(200, 50).unwrap();

    // Release adjacent space before it
    manager.release_sectors(150, 50).unwrap();

    // Should have coalesced
    assert_eq!(manager.get_free_chunks_count(), 1);
    assert_eq!(manager.get_total_free(), 100 * FEOX_BLOCK_SIZE as u64);
}

#[test]
fn test_no_metadata_allocation() {
    let mut manager = FreeSpaceManager::new();
    manager.initialize(DEFAULT_DEVICE_SIZE).unwrap();

    // First allocation should never be in metadata area (sectors 0-15)
    for _ in 0..100 {
        let sector = manager.allocate_sectors(1).unwrap();
        assert!(sector >= 16);
        manager.release_sectors(sector, 1).unwrap();
    }
}
