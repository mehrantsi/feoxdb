use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static DROPS: AtomicUsize = AtomicUsize::new(0);

struct DropCounter;

impl Drop for DropCounter {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn registry_scopes_and_pins_indeterminate_file_identity() {
    let marked = FileIdentity {
        device: u64::MAX,
        inode: u64::MAX,
    };
    let other = FileIdentity {
        device: u64::MAX,
        inode: u64::MAX - 1,
    };
    let file = Arc::new(tempfile::tempfile().unwrap());
    let pinned = Arc::downgrade(&file);

    assert!(!file_is_indeterminate(marked));
    assert!(!file_is_indeterminate(other));
    mark_file_indeterminate(marked, &file);
    drop(file);
    assert!(file_is_indeterminate(marked));
    assert!(!file_is_indeterminate(other));
    assert!(pinned.upgrade().is_some());
}

#[test]
fn only_completed_and_unqueued_buffers_are_released() {
    DROPS.store(0, Ordering::Relaxed);
    {
        let mut buffers = InFlightBuffers::new(vec![DropCounter, DropCounter, DropCounter]);
        buffers.mark_in_flight(0);
        buffers.mark_in_flight(1);
        buffers.mark_in_flight(2);
        buffers.mark_unqueued(2);
        assert!(buffers.mark_complete(0));
        assert!(!buffers.mark_complete(0));
        let _ = buffers.get(2);
    }
    assert_eq!(DROPS.load(Ordering::Relaxed), 2);
}

#[test]
fn completion_validation_rejects_errors_and_short_writes() {
    let kernel_error = validate_write_completion(-libc::EIO, 4096).unwrap_err();
    assert_eq!(kernel_error.raw_os_error(), Some(libc::EIO));

    let short_write = validate_write_completion(2048, 4096).unwrap_err();
    assert_eq!(short_write.kind(), io::ErrorKind::WriteZero);
    validate_write_completion(4096, 4096).unwrap();
}

#[test]
fn adjacent_extents_are_coalesced() {
    assert_eq!(
        coalesce_extents(&[(20, 2), (16, 4), (24, 1)]).unwrap(),
        vec![(16, 6), (24, 1)]
    );
}

#[test]
fn overlapping_extents_are_rejected() {
    assert!(matches!(
        coalesce_extents(&[(16, 4), (18, 1)]),
        Err(FeoxError::InvalidArgument)
    ));
}
