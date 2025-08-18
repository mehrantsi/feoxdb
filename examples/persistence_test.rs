use feoxdb::{FeoxStore, Result};
use std::fs;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    println!("{}", "═".repeat(72));
    println!("FeOxDB Persistence Test");
    println!("{}", "═".repeat(72));

    let db_path = "/tmp/feox_persistence_test.db";

    // Clean up any existing test file
    let _ = fs::remove_file(db_path);

    println!("\n>>> Phase 1: Writing test data");
    println!("Database path: {}", db_path);

    // Create test data with various sizes
    let test_data = vec![
        ("tiny", b"x".to_vec()),
        ("small", b"Hello, World!".to_vec()),
        ("medium", vec![b'M'; 100]),
        ("large", vec![b'L'; 1024]),
        ("xlarge", vec![b'X'; 10 * 1024]), // 10KB
        (
            "json",
            br#"{"name":"Test","values":[1,2,3,4,5],"nested":{"deep":true}}"#.to_vec(),
        ),
        ("binary", (0u8..=255).collect::<Vec<u8>>()),
        ("unicode", "Hello 世界 🌍 Hej мир".as_bytes().to_vec()),
        ("counter", 42i64.to_le_bytes().to_vec()),
    ];

    // Phase 1: Write data
    {
        let store = FeoxStore::new(Some(db_path.to_string()))?;

        for (key, value) in &test_data {
            let key_bytes = format!("test:{}", key).into_bytes();
            store.insert(&key_bytes, value, None)?;
            println!("  ✓ Inserted '{}' ({} bytes)", key, value.len());
        }

        // Add some range query test data
        for i in 0..10 {
            let key = format!("range:{:03}", i);
            let value = format!("value_{}", i);
            store.insert(key.as_bytes(), value.as_bytes(), None)?;
        }
        println!("  ✓ Inserted 10 range query test keys");

        // Force flush to disk
        println!("\n>>> Flushing to disk...");
        store.flush();
        thread::sleep(Duration::from_secs(5));
        println!(">>> Flush complete");

        // Verify we can read while store is still open
        println!("\n>>> Verifying immediate read...");
        for (key, expected_value) in &test_data {
            let key_bytes = format!("test:{}", key).into_bytes();
            let value = store.get(&key_bytes)?;
            assert_eq!(value, *expected_value, "Value mismatch for key: {}", key);
            println!("  ✓ Verified '{}'", key);
        }

        println!("\n>>> Store statistics before closing:");
        println!("  Total keys: {}", store.len());
        println!("  Memory usage: {} bytes", store.memory_usage());

        // Store will be dropped here, triggering final flush
    }

    println!("\n{}", "═".repeat(72));
    println!(">>> Phase 2: Reloading database and adding more data");
    println!("{}", "═".repeat(72));

    // Store phase 2 data for later verification
    let phase2_data = vec![
        ("phase2_small", b"Phase 2 small data".to_vec()),
        ("phase2_medium", vec![b'P'; 500]),
        ("phase2_large", vec![b'2'; 5 * 1024]), // 5KB
        (
            "phase2_json",
            br#"{"phase":2,"test":"persistence","array":[10,20,30]}"#.to_vec(),
        ),
        ("phase2_binary", (0u8..100).collect::<Vec<u8>>()),
    ];

    // Phase 2: Reload, verify phase 1 data, and add new data
    {
        println!("\n>>> Opening existing database...");
        let store = FeoxStore::new(Some(db_path.to_string()))?;

        // Give it time to load from disk
        thread::sleep(Duration::from_secs(1));

        println!(">>> Verifying all Phase 1 data persisted correctly...");

        // Verify all phase 1 test data
        for (key, expected_value) in &test_data {
            let key_bytes = format!("test:{}", key).into_bytes();

            // Check existence first
            if !store.contains_key(&key_bytes) {
                panic!("Key '{}' not found after reload!", key);
            }

            // Get and verify value
            let value = store.get(&key_bytes)?;
            assert_eq!(
                value, *expected_value,
                "Value mismatch for key '{}' after reload",
                key
            );

            let display_value = if *key == "xlarge" {
                format!("{} bytes of 'X'", value.len())
            } else if *key == "binary" {
                format!("{} bytes (binary)", value.len())
            } else if value.len() > 50 {
                format!("{} bytes", value.len())
            } else {
                String::from_utf8_lossy(&value).to_string()
            };

            println!("  ✓ {} = {}", key, display_value);
        }

        // Verify range query data
        println!("\n>>> Verifying range query data...");
        let range_results = store.range_query(b"range:000", b"range:009", 100)?;
        assert_eq!(range_results.len(), 10, "Expected 10 range results");
        println!("  ✓ Range query returned {} results", range_results.len());

        for (i, (key, value)) in range_results.iter().enumerate() {
            let expected_key = format!("range:{:03}", i);
            let expected_value = format!("value_{}", i);
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(value, expected_value.as_bytes());
        }
        println!("  ✓ All range values verified");

        // Test atomic counter persistence
        println!("\n>>> Testing atomic counter persistence...");
        let counter_value = store.atomic_increment(b"test:counter", 8, None)?;
        assert_eq!(counter_value, 50); // 42 + 8
        println!("  ✓ Counter incremented from 42 to 50");

        // Now add Phase 2 data
        println!("\n>>> Adding new data in Phase 2...");
        for (key, value) in &phase2_data {
            let key_bytes = format!("test:{}", key).into_bytes();
            store.insert(&key_bytes, value, None)?;
            println!(
                "  ✓ Inserted {} ({} bytes)",
                key.replace("phase2_", ""),
                value.len()
            );
        }

        // Add more range query test data for phase 2
        for i in 10..20 {
            let key = format!("range:{:03}", i);
            let value = format!("phase2_value_{}", i);
            store.insert(key.as_bytes(), value.as_bytes(), None)?;
        }
        println!("  ✓ Inserted 10 more range query test keys");

        // Force flush and wait
        println!("\n>>> Flushing Phase 2 data to disk...");
        store.flush();
        thread::sleep(Duration::from_secs(5));
        println!(">>> Flush complete");

        // Verify immediate read of phase 2 data
        println!("\n>>> Verifying Phase 2 data immediate read...");
        for (key, expected_value) in &phase2_data {
            let key_bytes = format!("test:{}", key).into_bytes();
            let value = store.get(&key_bytes)?;
            assert_eq!(
                value, *expected_value,
                "Phase 2 value mismatch for key: {}",
                key
            );
            println!("  ✓ Verified {}", key.replace("phase2_", ""));
        }

        println!("\n>>> Store statistics after Phase 2:");
        println!("  Total keys: {}", store.len());
        println!("  Memory usage: {} bytes", store.memory_usage());
    }

    println!("\n{}", "═".repeat(72));
    println!(">>> Phase 3: Final verification and cleanup");
    println!("{}", "═".repeat(72));

    // Phase 3: Final reload to verify ALL data from both phases persisted
    {
        println!("\n>>> Opening database for final verification...");
        let store = FeoxStore::new(Some(db_path.to_string()))?;

        // Give it time to load and rebuild indexes
        thread::sleep(Duration::from_secs(1));

        println!(">>> Verifying ALL data from Phase 1 and Phase 2...");

        // Verify all Phase 1 data
        println!("\n>>> Phase 1 data:");
        for (key, expected_value) in &test_data {
            let key_bytes = format!("test:{}", key).into_bytes();

            if !store.contains_key(&key_bytes) {
                panic!("Phase 1 key '{}' not found in Phase 3!", key);
            }

            let value = store.get(&key_bytes)?;

            // Special case for counter - it was incremented in Phase 2
            if *key == "counter" {
                let counter = i64::from_le_bytes(value.clone().try_into().unwrap());
                assert_eq!(counter, 50, "Counter should be 50 after Phase 2 increment");
            } else {
                assert_eq!(
                    value, *expected_value,
                    "Phase 1 value mismatch in Phase 3 for key: {}",
                    key
                );
            }

            let display = if *key == "xlarge" || value.len() > 30 {
                format!("{} bytes", value.len())
            } else {
                String::from_utf8_lossy(&value).to_string()
            };
            println!("  ✓ {} = {}", key, display);
        }

        // Verify all Phase 2 data
        println!("\n>>> Phase 2 data:");
        for (key, expected_value) in &phase2_data {
            let key_bytes = format!("test:{}", key).into_bytes();

            if !store.contains_key(&key_bytes) {
                panic!("Phase 2 key '{}' not found in Phase 3!", key);
            }

            let value = store.get(&key_bytes)?;
            assert_eq!(
                value, *expected_value,
                "Phase 2 value mismatch in Phase 3 for key: {}",
                key
            );

            let display = if value.len() > 30 {
                format!("{} bytes", value.len())
            } else {
                String::from_utf8_lossy(&value).to_string()
            };
            println!("  ✓ {} = {}", key.replace("phase2_", ""), display);
        }

        // Verify range query data from both phases
        println!("\n>>> Verifying all range query data (20 keys total)...");
        let range_results = store.range_query(b"range:000", b"range:999", 100)?;
        assert_eq!(
            range_results.len(),
            20,
            "Expected 20 range results (10 from each phase)"
        );
        println!("  ✓ Range query returned {} results", range_results.len());

        // Verify counter value
        let counter_value = store.get(b"test:counter")?;
        let counter = i64::from_le_bytes(counter_value.try_into().unwrap());
        assert_eq!(counter, 50);
        println!("  ✓ Counter value is {}", counter);

        println!("\n>>> Final store statistics:");
        println!("  Total keys: {}", store.len());
        println!("  Memory usage: {} bytes", store.memory_usage());

        // Test deletion to ensure free space management works
        println!("\n>>> Testing deletion and free space management...");
        store.delete(b"test:tiny", None)?;
        store.delete(b"test:phase2_small", None)?;
        println!("  ✓ Deleted keys from both phases");

        assert!(!store.contains_key(b"test:tiny"));
        assert!(!store.contains_key(b"test:phase2_small"));
        assert!(store.contains_key(b"test:medium"));
        assert!(store.contains_key(b"test:phase2_medium"));
        println!("  ✓ Deletion verified");
    }

    // Cleanup
    println!("\n>>> Cleaning up test database...");
    fs::remove_file(db_path).map_err(feoxdb::FeoxError::IoError)?;
    println!("  ✓ Test database removed");

    println!("\n{}", "═".repeat(72));
    println!("✅ All persistence tests passed!");
    println!("{}", "═".repeat(72));

    Ok(())
}
