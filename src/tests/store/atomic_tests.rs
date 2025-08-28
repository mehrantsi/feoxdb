use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_atomic_increment_basic() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"counter";

    // Initialize counter
    let zero: i64 = 0;
    store.insert(key, &zero.to_le_bytes()).unwrap();

    // Increment
    let val = store.atomic_increment(key, 1).unwrap();
    assert_eq!(val, 1);

    let val = store.atomic_increment(key, 5).unwrap();
    assert_eq!(val, 6);

    // Decrement
    let val = store.atomic_increment(key, -2).unwrap();
    assert_eq!(val, 4);
}

#[test]
fn test_atomic_increment_create_if_not_exists() {
    let store = FeoxStore::new(None).unwrap();

    // Increment non-existent key creates it
    let val = store.atomic_increment(b"new_counter", 100).unwrap();
    assert_eq!(val, 100);

    let val = store.atomic_increment(b"new_counter", 50).unwrap();
    assert_eq!(val, 150);
}

#[test]
fn test_atomic_increment_invalid_value() {
    let store = FeoxStore::new(None).unwrap();

    // Insert non-numeric value
    store.insert(b"not_a_counter", b"text").unwrap();

    // Try to increment
    let result = store.atomic_increment(b"not_a_counter", 1);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::InvalidOperation));
}

#[test]
fn test_atomic_increment_saturation() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"saturate";

    // Set to max value
    let max: i64 = i64::MAX - 1;
    store.insert(key, &max.to_le_bytes()).unwrap();

    // Increment should saturate, not overflow
    let val = store.atomic_increment(key, 10).unwrap();
    assert_eq!(val, i64::MAX);
}

#[test]
fn test_cas_basic() {
    let store = FeoxStore::new(None).unwrap();

    store.insert(b"key", b"value1").unwrap();

    // Successful CAS
    let swapped = store
        .compare_and_swap(b"key", b"value1", b"value2")
        .unwrap();
    assert!(swapped);
    assert_eq!(store.get(b"key").unwrap(), b"value2");

    // Failed CAS - wrong expected value
    let swapped = store
        .compare_and_swap(b"key", b"value1", b"value3")
        .unwrap();
    assert!(!swapped);
    assert_eq!(store.get(b"key").unwrap(), b"value2");

    // CAS on non-existent key
    let swapped = store.compare_and_swap(b"missing", b"any", b"new").unwrap();
    assert!(!swapped);
}

#[test]
fn test_cas_concurrent() {
    let store = Arc::new(FeoxStore::new(None).unwrap());
    let num_threads = 10;
    let iterations = 100;

    store.insert(b"counter", b"0").unwrap();

    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let store_clone = Arc::clone(&store);
        let barrier_clone = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier_clone.wait();

            let mut successful_swaps = 0;
            for _ in 0..iterations {
                loop {
                    let current = store_clone.get(b"counter").unwrap();
                    let num: u32 = String::from_utf8_lossy(&current).parse().unwrap();
                    let next = (num + 1).to_string();

                    if store_clone
                        .compare_and_swap(b"counter", &current, next.as_bytes())
                        .unwrap()
                    {
                        successful_swaps += 1;
                        break;
                    }
                }
            }
            successful_swaps
        }));
    }

    let total_swaps: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total_swaps, num_threads * iterations);

    let final_value = store.get(b"counter").unwrap();
    let final_num: u32 = String::from_utf8_lossy(&final_value).parse().unwrap();
    assert_eq!(final_num, (num_threads * iterations) as u32);
}

#[test]
fn test_cas_timestamp_conflict() {
    let store = FeoxStore::new(None).unwrap();

    store.insert(b"key", b"value1").unwrap();

    // CAS with older timestamp should fail
    let result = store.compare_and_swap_with_timestamp(b"key", b"value1", b"value2", Some(1));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));
}

#[test]
fn test_cas_single_byte_value() {
    let store = FeoxStore::new(None).unwrap();

    // Test with minimal single-byte values
    store.insert(b"key", b"a").unwrap();

    // CAS with single byte expected value
    let swapped = store.compare_and_swap(b"key", b"a", b"b").unwrap();
    assert!(swapped);
    assert_eq!(store.get(b"key").unwrap(), b"b");

    // CAS to another single byte
    let swapped = store.compare_and_swap(b"key", b"b", b"c").unwrap();
    assert!(swapped);
    assert_eq!(store.get(b"key").unwrap(), b"c");
}

#[test]
fn test_cas_large_value() {
    let store = FeoxStore::new(None).unwrap();

    let large_value1 = vec![b'a'; 1024 * 64]; // 64KB
    let large_value2 = vec![b'b'; 1024 * 64];

    store.insert(b"key", &large_value1).unwrap();

    let swapped = store
        .compare_and_swap(b"key", &large_value1, &large_value2)
        .unwrap();
    assert!(swapped);
    assert_eq!(store.get(b"key").unwrap(), large_value2);
}

#[test]
fn test_cas_binary_data() {
    let store = FeoxStore::new(None).unwrap();

    let binary1 = vec![0u8, 1, 2, 3, 255, 254, 253];
    let binary2 = vec![255u8, 0, 128, 64, 32, 16, 8];

    store.insert(b"key", &binary1).unwrap();

    let swapped = store.compare_and_swap(b"key", &binary1, &binary2).unwrap();
    assert!(swapped);
    assert_eq!(store.get(b"key").unwrap(), binary2);
}

#[test]
fn test_cas_race_condition_simulation() {
    // This test simulates the scenario where a value changes between
    // the initial check and the actual CAS operation
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store.insert(b"key", b"initial").unwrap();

    let store1 = Arc::clone(&store);
    let store2 = Arc::clone(&store);

    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let b2 = Arc::clone(&barrier);

    let handle1 = thread::spawn(move || {
        b1.wait();
        // Try to CAS from initial to value1
        let mut attempts = 0;
        loop {
            attempts += 1;
            if store1
                .compare_and_swap(b"key", b"initial", b"value1")
                .unwrap()
            {
                return (true, attempts);
            }
            if attempts > 100 {
                return (false, attempts);
            }
            thread::yield_now();
        }
    });

    let handle2 = thread::spawn(move || {
        b2.wait();
        // Also try to CAS from initial to value2
        let mut attempts = 0;
        loop {
            attempts += 1;
            if store2
                .compare_and_swap(b"key", b"initial", b"value2")
                .unwrap()
            {
                return (true, attempts);
            }
            if attempts > 100 {
                return (false, attempts);
            }
            thread::yield_now();
        }
    });

    let (success1, _attempts1) = handle1.join().unwrap();
    let (success2, _attempts2) = handle2.join().unwrap();

    // Exactly one should succeed
    assert!(success1 ^ success2, "Exactly one CAS should succeed");

    let final_value = store.get(b"key").unwrap();
    assert!(final_value == b"value1" || final_value == b"value2");
}

#[test]
fn test_cas_rapid_updates() {
    // Test rapid successive CAS operations
    let store = FeoxStore::new(None).unwrap();

    store.insert(b"version", b"v0").unwrap();

    for i in 0..100 {
        let current = format!("v{}", i);
        let next = format!("v{}", i + 1);

        let swapped = store
            .compare_and_swap(b"version", current.as_bytes(), next.as_bytes())
            .unwrap();
        assert!(swapped, "CAS failed at iteration {}", i);
    }

    assert_eq!(store.get(b"version").unwrap(), b"v100");
}
