use feoxdb::FeoxStore;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("FeOxDB Basic Usage Example\n");

    // Create an in-memory store
    let store = FeoxStore::new(None)?;

    // Insert some data
    println!("Inserting 1000 key-value pairs...");
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        store.insert(key.as_bytes(), value.as_bytes())?;
    }
    let elapsed = start.elapsed();
    println!(
        "Insert time: {:?} ({:.0} ops/sec)\n",
        elapsed,
        1000.0 / elapsed.as_secs_f64()
    );

    // Read the data back
    println!("Reading 1000 key-value pairs...");
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let expected = format!("value_{:04}", i);
        let value = store.get(key.as_bytes())?;
        assert_eq!(value.as_slice(), expected.as_bytes());
    }
    let elapsed = start.elapsed();
    println!(
        "Read time: {:?} ({:.0} ops/sec)\n",
        elapsed,
        1000.0 / elapsed.as_secs_f64()
    );

    // Range query
    println!("Range query from key_0100 to key_0200:");
    let results = store.range_query(b"key_0100", b"key_0200", 10)?;
    for (key, value) in results.iter().take(5) {
        println!(
            "  {} => {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }
    println!("  ... ({} total results)\n", results.len());

    // Delete some data
    println!("Deleting keys key_0500 to key_0599...");
    for i in 500..600 {
        let key = format!("key_{:04}", i);
        store.delete(key.as_bytes())?;
    }

    // Get statistics
    println!("Store statistics:");
    println!("  Total records: {}", store.len());
    println!(
        "  Memory usage: {:.2} MB",
        store.memory_usage() as f64 / 1_048_576.0
    );

    Ok(())
}
