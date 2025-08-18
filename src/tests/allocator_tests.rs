use crate::constants::*;
use crate::utils::allocator::AlignedBuffer;

#[test]
fn test_aligned_buffer_creation() {
    let size = FEOX_BLOCK_SIZE;
    let buffer = AlignedBuffer::new(size).unwrap();

    assert_eq!(buffer.len(), 0);
    assert_eq!(buffer.capacity(), size);
}

#[test]
fn test_aligned_buffer_alignment() {
    let buffer = AlignedBuffer::new(FEOX_BLOCK_SIZE).unwrap();

    // Check that the buffer is properly aligned
    let ptr = buffer.as_ptr() as usize;
    assert_eq!(ptr % FEOX_ALIGNMENT, 0);
}

#[test]
fn test_aligned_buffer_write_read() {
    let mut buffer = AlignedBuffer::new(1024).unwrap();

    // Write some data manually
    let data = vec![0x42u8; 512];
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.as_mut_ptr(), data.len());
    }
    buffer.set_len(512);

    assert_eq!(buffer.len(), 512);
    assert_eq!(buffer.as_slice(), &data[..]);
}

#[test]
fn test_aligned_buffer_set_len() {
    let mut buffer = AlignedBuffer::new(1024).unwrap();

    // Initially empty
    assert_eq!(buffer.len(), 0);

    // Set length (unsafe - assumes data is initialized)
    buffer.set_len(100);
    assert_eq!(buffer.len(), 100);

    // Can reduce length
    buffer.set_len(50);
    assert_eq!(buffer.len(), 50);
}

#[test]
fn test_aligned_buffer_extend() {
    let mut buffer = AlignedBuffer::new(1024).unwrap();

    unsafe {
        std::ptr::copy_nonoverlapping([1u8, 2, 3].as_ptr(), buffer.as_mut_ptr(), 3);
    }
    buffer.set_len(3);
    assert_eq!(buffer.as_slice(), &[1, 2, 3]);

    unsafe {
        std::ptr::copy_nonoverlapping([4u8, 5, 6].as_ptr(), buffer.as_mut_ptr().add(3), 3);
    }
    buffer.set_len(6);
    assert_eq!(buffer.as_slice(), &[1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_aligned_buffer_clear() {
    let mut buffer = AlignedBuffer::new(1024).unwrap();

    unsafe {
        std::ptr::copy_nonoverlapping([1u8, 2, 3, 4, 5].as_ptr(), buffer.as_mut_ptr(), 5);
    }
    buffer.set_len(5);
    assert_eq!(buffer.len(), 5);

    buffer.clear();
    assert_eq!(buffer.len(), 0);
    assert!(buffer.capacity() >= 1024); // Capacity is at least what was requested
}

#[test]
fn test_aligned_buffer_mut_access() {
    let mut buffer = AlignedBuffer::new(1024).unwrap();

    unsafe {
        std::ptr::write_bytes(buffer.as_mut_ptr(), 0, 10);
    }
    buffer.set_len(10);

    // Modify through mutable slice
    let slice = buffer.as_mut_slice();
    slice[0] = 0xFF;
    slice[9] = 0xEE;

    assert_eq!(buffer.as_slice()[0], 0xFF);
    assert_eq!(buffer.as_slice()[9], 0xEE);
}

#[test]
fn test_multiple_alignments() {
    // Test different sizes to ensure alignment works
    let sizes = vec![512, 1024, 4096, 8192];

    for size in sizes {
        let buffer = AlignedBuffer::new(size).unwrap();
        let ptr = buffer.as_ptr() as usize;
        assert_eq!(ptr % FEOX_ALIGNMENT, 0, "Failed for size {}", size);
    }
}

#[test]
fn test_aligned_buffer_capacity_exact() {
    let size = 12345; // Non-power-of-2 size
    let buffer = AlignedBuffer::new(size).unwrap();

    assert!(buffer.capacity() >= size);
    // Capacity might be rounded up for alignment
}

#[test]
fn test_aligned_buffer_zero_size() {
    // Even zero-size buffer should work
    let buffer = AlignedBuffer::new(0).unwrap();
    assert_eq!(buffer.len(), 0);
    assert_eq!(buffer.capacity(), 0);
}

#[test]
fn test_aligned_buffer_large_size() {
    // Test with a large buffer
    let size = 1024 * 1024; // 1MB
    let mut buffer = AlignedBuffer::new(size).unwrap();

    // Fill with pattern
    let pattern = vec![0xAB; size];
    unsafe {
        std::ptr::copy_nonoverlapping(pattern.as_ptr(), buffer.as_mut_ptr(), size);
    }
    buffer.set_len(size);

    assert_eq!(buffer.len(), size);
    assert_eq!(buffer.as_slice(), &pattern[..]);
}

#[test]
fn test_aligned_buffer_reuse() {
    let mut buffer = AlignedBuffer::new(1024).unwrap();

    // Use buffer multiple times
    for i in 0..10 {
        buffer.clear();
        let data = [i as u8; 100];
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.as_mut_ptr(), 100);
        }
        buffer.set_len(100);
        assert_eq!(buffer.as_slice(), &data[..]);
    }
}
