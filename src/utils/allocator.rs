#[cfg(unix)]
use nix::sys::mman::{mmap_anonymous, munmap, MapFlags, ProtFlags};
use std::alloc::{alloc, dealloc, Layout};
#[cfg(unix)]
use std::os::raw::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::constants::*;
use crate::error::{FeoxError, Result};

static TOTAL_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct FeoxAllocator;

impl FeoxAllocator {
    pub fn allocate(size: usize) -> Result<NonNull<u8>> {
        let ptr = if size <= KMALLOC_LIMIT {
            Self::allocate_small(size)?
        } else {
            Self::allocate_large(size)?
        };

        TOTAL_ALLOCATED.fetch_add(size, Ordering::AcqRel);
        Ok(ptr)
    }

    /// Allocate memory with specific alignment (for O_DIRECT)
    pub fn allocate_aligned(size: usize, alignment: usize) -> Result<NonNull<u8>> {
        #[cfg(unix)]
        unsafe {
            let mut ptr: *mut libc::c_void = std::ptr::null_mut();
            let result = libc::posix_memalign(&mut ptr, alignment, size);
            if result != 0 {
                return Err(FeoxError::AllocationFailed);
            }
            TOTAL_ALLOCATED.fetch_add(size, Ordering::AcqRel);
            NonNull::new(ptr as *mut u8).ok_or(FeoxError::AllocationFailed)
        }

        #[cfg(not(unix))]
        {
            // For non-Unix, use standard aligned allocation
            let layout = Layout::from_size_align(size, alignment)
                .map_err(|_| FeoxError::AllocationFailed)?;
            unsafe {
                let ptr = alloc(layout);
                TOTAL_ALLOCATED.fetch_add(size, Ordering::AcqRel);
                NonNull::new(ptr).ok_or(FeoxError::AllocationFailed)
            }
        }
    }

    pub fn deallocate(ptr: NonNull<u8>, size: usize) {
        if size <= KMALLOC_LIMIT {
            Self::deallocate_small(ptr, size);
        } else {
            Self::deallocate_large(ptr, size);
        }

        TOTAL_ALLOCATED.fetch_sub(size, Ordering::AcqRel);
    }

    /// Deallocate aligned memory
    pub fn deallocate_aligned(ptr: NonNull<u8>, size: usize, alignment: usize) {
        #[cfg(unix)]
        {
            let _ = alignment; // Not needed on Unix, posix_memalign memory is freed with free()
            unsafe {
                libc::free(ptr.as_ptr() as *mut libc::c_void);
            }
        }

        #[cfg(not(unix))]
        {
            // For non-Unix, must use dealloc with the same layout used for allocation
            let layout = Layout::from_size_align(size, alignment).unwrap();
            unsafe {
                dealloc(ptr.as_ptr(), layout);
            }
        }

        TOTAL_ALLOCATED.fetch_sub(size, Ordering::AcqRel);
    }

    fn allocate_small(size: usize) -> Result<NonNull<u8>> {
        let layout = Layout::from_size_align(size, 8).map_err(|_| FeoxError::AllocationFailed)?;

        unsafe {
            let ptr = alloc(layout);
            NonNull::new(ptr).ok_or(FeoxError::AllocationFailed)
        }
    }

    fn allocate_large(size: usize) -> Result<NonNull<u8>> {
        let aligned_size = (size + PAGE_MASK) & !PAGE_MASK;

        #[cfg(unix)]
        unsafe {
            // aligned_size is guaranteed to be non-zero (rounded up from size)
            let non_zero_size = std::num::NonZeroUsize::new_unchecked(aligned_size);
            let ptr = mmap_anonymous(
                None,
                non_zero_size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE,
            )
            .map_err(|_| FeoxError::AllocationFailed)?;

            Ok(ptr.cast())
        }

        #[cfg(not(unix))]
        {
            // Use aligned allocation for large buffers on non-Unix
            let layout = Layout::from_size_align(aligned_size, PAGE_SIZE)
                .map_err(|_| FeoxError::AllocationFailed)?;

            unsafe {
                let ptr = alloc(layout);
                NonNull::new(ptr).ok_or(FeoxError::AllocationFailed)
            }
        }
    }

    fn deallocate_small(ptr: NonNull<u8>, size: usize) {
        let layout = Layout::from_size_align(size, 8).unwrap();
        unsafe {
            dealloc(ptr.as_ptr(), layout);
        }
    }

    fn deallocate_large(ptr: NonNull<u8>, size: usize) {
        let aligned_size = (size + PAGE_MASK) & !PAGE_MASK;

        #[cfg(unix)]
        unsafe {
            let ptr_void = ptr.cast::<c_void>();
            let _ = munmap(ptr_void, aligned_size);
        }

        #[cfg(not(unix))]
        {
            let layout = Layout::from_size_align(aligned_size, PAGE_SIZE).unwrap();
            unsafe {
                dealloc(ptr.as_ptr(), layout);
            }
        }
    }

    pub fn get_allocated() -> usize {
        TOTAL_ALLOCATED.load(Ordering::Acquire)
    }
}

pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    capacity: usize,
    is_aligned: bool,
    alignment: usize,
}

impl AlignedBuffer {
    pub fn new(capacity: usize) -> Result<Self> {
        // For O_DIRECT, we need alignment to FEOX_BLOCK_SIZE (4096)
        // Round up capacity to block size multiple
        let alignment = FEOX_BLOCK_SIZE;
        let aligned_capacity = capacity.div_ceil(alignment) * alignment;

        // Use posix_memalign for proper alignment
        let ptr = FeoxAllocator::allocate_aligned(aligned_capacity, alignment)?;

        Ok(Self {
            ptr,
            size: 0,
            capacity: aligned_capacity,
            is_aligned: true,
            alignment,
        })
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity);
        self.size = new_len;
    }

    pub fn clear(&mut self) {
        self.size = 0;
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.is_aligned {
            FeoxAllocator::deallocate_aligned(self.ptr, self.capacity, self.alignment);
        } else {
            FeoxAllocator::deallocate(self.ptr, self.capacity);
        }
    }
}
