<div align="center">
  <img src="logo.svg" alt="FeOxDB Logo" width="400">

  Ultra-fast, embedded key-value database for Rust with sub-microsecond latency.
</div>

[📚 Documentation](https://feoxdb.com) | [📊 Benchmarks](https://feoxdb.com/benchmarks.html) | [💬 Issues](https://github.com/mehrantsi/FeOxDB/issues)

## Features

- **Sub-Microsecond Latency**: <300ns GET, <600ns INSERT operations
- **Lock-Free Concurrency**: Built on DashMap and Crossbeam SkipList
- **io_uring Support** (Linux): Kernel-bypass I/O for maximum throughput with minimal syscalls
- **Flexible Storage**: Memory-only or persistent modes with async I/O
- **JSON Patch Support**: RFC 6902 compliant partial updates for JSON values
- **Atomic Counters**: Thread-safe increment/decrement operations
- **Write Buffering**: Sharded buffers with batched writes to reduce contention
- **CLOCK Cache**: Second-chance eviction algorithm
- **Statistics**: Real-time performance monitoring
- **Free Space Management**: Dual RB-tree structure for O(log n) allocation
- **Zero Fragmentation**: Automatic coalescing prevents disk fragmentation

## ACID Properties and Durability

FeOxDB provides ACI properties with relaxed durability:

- **Atomicity**: ✅ Individual operations are atomic via Arc-wrapped records
- **Consistency**: ✅ Timestamp-based conflict resolution ensures consistency
- **Isolation**: ✅ Lock-free reads and sharded writes provide operation isolation
- **Durability**: ⚠️ Write-behind logging with bounded data loss window

### Durability Trade-offs

FeOxDB trades full durability for extreme performance:
- **Write-behind buffering**: Flushes every 100ms or when buffers fill (1024 entries or 16MB per shard)
- **Worst-case data loss**:
  - **Time window**: `100ms + 16MB / 4KB_random_write_QD1_throughput`
  - **Data at risk**: `16MB × num_shards (num_shards = num_cpus / 2)` (e.g., 64MB for 4 shards, 128MB for 8 shards)
  - Workers write in parallel, so time doesn't multiply with shards
  - Example (50MB/s 4KB random QD1): 420ms window, up to 64MB at risk (4 shards)
  - Example (200MB/s 4KB random QD1): 180ms window, up to 64MB at risk (4 shards)
- **Memory-only mode**: No durability, maximum performance
- **Explicit flush**: Call `store.flush()` to synchronously write all buffered data (blocks until fsync completes)

## Quick Start

### Installation

```toml
[dependencies]
feoxdb = "0.1.0"
```

### Basic Usage

```rust
use feoxdb::FeoxStore;

fn main() -> feoxdb::Result<()> {
    // Create an in-memory store
    let store = FeoxStore::new(None)?;
    
    // Insert a key-value pair
    store.insert(b"user:123", b"{\"name\":\"Mehran\"}")?;
    
    // Get a value
    let value = store.get(b"user:123")?;
    println!("Value: {}", String::from_utf8_lossy(&value));
    
    // Check existence
    if store.contains_key(b"user:123") {
        println!("Key exists!");
    }
    
    // Delete a key
    store.delete(b"user:123")?;
    
    Ok(())
}
```

### Persistent Storage

```rust
use feoxdb::FeoxStore;

fn main() -> feoxdb::Result<()> {
    // Create a persistent store
    let store = FeoxStore::new(Some("/path/to/data.feox".to_string()))?;
    
    // Operations are automatically persisted
    store.insert(b"config:app", b"production")?;
    
    // Flush to disk
    store.flush();
    
    // Data survives restarts
    drop(store);
    let store = FeoxStore::new(Some("/path/to/data.feox".to_string()))?;
    let value = store.get(b"config:app")?;
    assert_eq!(value, b"production");
    
    Ok(())
}
```

### Advanced Configuration

```rust
use feoxdb::FeoxStore;

fn main() -> feoxdb::Result<()> {
    let store = FeoxStore::builder()
        .device_path("/data/myapp.feox")
        .max_memory(2_000_000_000)  // 2GB limit
        .enable_caching(true)        // Enable CLOCK cache
        .hash_bits(20)               // 1M hash buckets
        .build()?;
    
    Ok(())
}
```

### Concurrent Access

```rust
use feoxdb::FeoxStore;
use std::sync::Arc;
use std::thread;

fn main() -> feoxdb::Result<()> {
    let store = Arc::new(FeoxStore::new(None)?);
    let mut handles = vec![];
    
    // Spawn 10 threads, each inserting data
    for i in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for j in 0..1000 {
                let key = format!("thread_{}:key_{}", i, j);
                store_clone.insert(key.as_bytes(), b"value", None).unwrap();
            }
        }));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!("Total keys: {}", store.len());  // 10,000
    Ok(())
}
```

### Range Queries

```rust
use feoxdb::FeoxStore;

fn main() -> feoxdb::Result<()> {
    let store = FeoxStore::new(None)?;
    
    // Insert sorted keys
    store.insert(b"user:001", b"Mehran")?;
    store.insert(b"user:002", b"Bob")?;
    store.insert(b"user:003", b"Charlie")?;
    store.insert(b"user:004", b"David")?;
    
    // Range query: get users 001-003 (inclusive on both ends)
    let results = store.range_query(b"user:001", b"user:003", 10)?;
    
    for (key, value) in results {
        println!("{}: {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }
    // Outputs: user:001, user:002, user:003
    
    Ok(())
}
```

### JSON Patch Operations (RFC 6902)

FeOxDB supports partial updates to JSON documents using the standard JSON Patch format:

```rust
use feoxdb::FeoxStore;

fn main() -> feoxdb::Result<()> {
    let store = FeoxStore::new(None)?;
    
    // Store a JSON document
    let user = r#"{
        "name": "Mehran",
        "age": 30,
        "skills": ["Rust", "Go"],
        "address": {
            "city": "San Francisco",
            "zip": "94105"
        }
    }"#;
    store.insert(b"user:123", user.as_bytes())?;
    
    // Apply patches to modify specific fields
    let patches = r#"[
        {"op": "replace", "path": "/age", "value": 31},
        {"op": "add", "path": "/skills/-", "value": "Python"},
        {"op": "add", "path": "/email", "value": "mehran@example.com"},
        {"op": "replace", "path": "/address/city", "value": "Seattle"}
    ]"#;
    
    store.json_patch(b"user:123", patches.as_bytes())?;
    
    // Document is now updated with patches applied
    let updated = store.get(b"user:123")?;
    println!("Updated: {}", String::from_utf8_lossy(&updated));
    
    Ok(())
}
```

Supported JSON Patch operations:
- `add`: Add a new field or array element
- `remove`: Remove a field or array element
- `replace`: Replace an existing value
- `move`: Move a value from one path to another
- `copy`: Copy a value from one path to another
- `test`: Test that a value at a path equals a specified value

### Atomic Counter Operations

```rust
use feoxdb::FeoxStore;

fn main() -> feoxdb::Result<()> {
    let store = FeoxStore::new(None)?;
    
    // Initialize counters (must be 8-byte i64 values)
    let zero: i64 = 0;
    store.insert(b"stats:visits", &zero.to_le_bytes())?;
    store.insert(b"stats:downloads", &zero.to_le_bytes())?;
    
    // Increment atomically (thread-safe)
    let visits = store.atomic_increment(b"stats:visits", 1)?;
    println!("Visits: {}", visits);  // 1
    
    // Increment by 10
    let downloads = store.atomic_increment(b"stats:downloads", 10)?;
    println!("Downloads: {}", downloads);  // 10
    
    // Decrement
    let visits = store.atomic_increment(b"stats:visits", -1)?;
    println!("Visits after decrement: {}", visits);  // 0
    
    Ok(())
}
```

## Performance

### Benchmarks

Run the included benchmarks:

```bash
# Performance test
cargo run --release --example performance_test 100000 100

# Deterministic test
cargo run --release --example deterministic_test 100000 100

# Criterion benchmarks
cargo bench
```

### Results

Typical performance on M3 Max:

| Operation | Latency | Throughput | Notes |
|-----------|---------|------------|-------|
| GET | ~200-260ns | 2.1M ops/sec | DashMap lookup + stats |
| INSERT | ~700ns | 850K ops/sec | Memory allocation + indexing |
| DELETE | ~290ns | 1.1M ops/sec | Removal from indexes |
| Mixed (80/20 R/W) | ~290ns | 3.1M ops/sec | Real-world workload |

### Throughput

Based on Criterion benchmarks:
- **GET**: 8.2M - 10.5M ops/sec (varies by batch size)
- **INSERT**: 730K - 1.1M ops/sec (varies by value size)
- **Mixed workload (80/20)**: 3.1M ops/sec

## Architecture

FeOxDB uses a lock-free, multi-tier architecture optimized for modern multi-core CPUs:

### Lock-Free Design

The entire hot path is lock-free, ensuring consistent sub-microsecond latency:

- **DashMap**: Sharded hash table with lock-free reads and fine-grained locking for writes
- **Crossbeam SkipList**: Fully lock-free ordered index for range queries
- **Atomic Operations**: All metadata updates use atomic primitives
- **RCU-style Access**: Read-Copy-Update pattern for zero-cost reads

### Async Write-Behind Logging

Writes are decoupled from disk I/O for maximum throughput:

1. **Sharded Write Buffers**
   - Multiple buffers with thread-consistent assignment
   - Reduces contention between threads
   - Cache-friendly access patterns

2. **Batched Flushing**
   - Writes accumulated and flushed in batches
   - Optimal disk utilization with large sequential writes
   - Configurable flush intervals

3. **io_uring Integration** (Linux)
   - Kernel-bypass I/O with submission/completion queues
   - Zero-copy operations where possible
   - Async I/O without thread pool overhead

4. **Write Coalescing**
   - Multiple updates to same key automatically merged
   - Reduces write amplification
   - Improves SSD lifespan

### Storage Tiers

1. **In-Memory Layer**
   - Primary storage in DashMap
   - O(1) lookups with ~100ns latency
   - Automatic memory management

2. **Write Buffer Layer**
   - Sharded buffers for concurrent writes
   - Lock-free MPSC queues
   - Backpressure handling

3. **Persistent Storage**
   - Sector-aligned writes (4KB blocks)
   - Write-ahead logging for durability
   - Crash recovery support

4. **Cache Layer**
   - CLOCK eviction algorithm
   - Keeps hot data in memory after disk write
   - Transparent cache management

## API Documentation

Full API documentation is available:

```bash
cargo doc --open
```

Key types:
- `FeoxStore` - Main database interface
- `StoreBuilder` - Configuration builder
- `FeoxError` - Error types
- `Statistics` - Performance metrics

## Examples

See the `examples/` directory for more:

- `performance_test.rs` - Benchmark tool
- `deterministic_test.rs` - Reproducible performance test

### Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## License

Copyright 2025 Mehran Toosi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

See [LICENSE](LICENSE) for the full license text.