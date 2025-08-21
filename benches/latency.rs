use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use feoxdb::FeoxStore;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn benchmark_get_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_latency");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    let store = Arc::new(FeoxStore::new(None).unwrap());

    // Pre-populate with data
    for i in 0..10000 {
        let key = format!("key_{:06}", i);
        let value = vec![0u8; 64]; // Small value
        store.insert(key.as_bytes(), &value).unwrap();
    }

    // Test different key patterns
    for pattern in ["sequential", "random", "hot_key"].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(pattern),
            pattern,
            |b, &pattern| {
                let store = store.clone();
                match pattern {
                    "sequential" => {
                        let mut i = 0;
                        b.iter(|| {
                            let key = format!("key_{:06}", i % 10000);
                            black_box(store.get(key.as_bytes()).ok());
                            i += 1;
                        });
                    }
                    "random" => {
                        use rand::Rng;
                        let mut rng = rand::rng();
                        b.iter(|| {
                            let idx = rng.random_range(0..10000);
                            let key = format!("key_{:06}", idx);
                            black_box(store.get(key.as_bytes()).ok());
                        });
                    }
                    "hot_key" => {
                        // 90% of requests go to 10% of keys
                        use rand::Rng;
                        let mut rng = rand::rng();
                        b.iter(|| {
                            let idx = if rng.random_bool(0.9) {
                                rng.random_range(0..1000)
                            } else {
                                rng.random_range(1000..10000)
                            };
                            let key = format!("key_{:06}", idx);
                            black_box(store.get(key.as_bytes()).ok());
                        });
                    }
                    _ => {}
                }
            },
        );
    }

    group.finish();
}

fn benchmark_insert_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_latency");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    // Only test memory mode as persistent benchmarks are misleading
    for value_size in [64, 1024, 4096].iter() {
        let test_name = format!("size_{}", value_size);
        group.bench_with_input(
            BenchmarkId::from_parameter(test_name),
            value_size,
            |b, &value_size| {
                let store = Arc::new(FeoxStore::new(None).unwrap());
                let value = vec![0u8; value_size];
                let mut i = 0u64;

                b.iter(|| {
                    let key = format!("key_{:012}", i);
                    black_box(store.insert(key.as_bytes(), &value).ok());
                    i += 1;
                });
            },
        );
    }

    group.finish();
}

fn benchmark_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_latency");
    group.measurement_time(Duration::from_secs(10));

    // Only test memory mode as persistent benchmarks are misleading
    let store = Arc::new(FeoxStore::new(None).unwrap());

    // Pre-populate
    for i in 0..10000 {
        let key = format!("key_{:06}", i);
        let value = vec![0u8; 256];
        store.insert(key.as_bytes(), &value).unwrap();
    }

    group.bench_function("80_read_15_write_5_delete", |b| {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut next_key = 10000u64;

        b.iter(|| {
            let op = rng.random_range(0..100);

            if op < 80 {
                // Read
                let idx = rng.random_range(0..10000);
                let key = format!("key_{:06}", idx);
                black_box(store.get(key.as_bytes()).ok());
            } else if op < 95 {
                // Write
                let key = format!("key_{:06}", next_key);
                let value = vec![0u8; 256];
                black_box(store.insert(key.as_bytes(), &value).ok());
                next_key += 1;
            } else {
                // Delete
                let idx = rng.random_range(0..10000);
                let key = format!("key_{:06}", idx);
                black_box(store.delete(key.as_bytes()).ok());
            }
        });
    });

    group.finish();
}

fn benchmark_delete_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_latency");
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    // Only test memory mode as persistent benchmarks are misleading
    group.bench_function("memory", |b| {
        let store = Arc::new(FeoxStore::new(None).unwrap());

        // Pre-populate with enough keys
        for i in 0..100000 {
            let key = format!("key_{:08}", i);
            let value = vec![0u8; 64];
            store.insert(key.as_bytes(), &value).unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let key = format!("key_{:08}", i);
            black_box(store.delete(key.as_bytes()).ok());
            i = (i + 1) % 100000;
        });
    });

    group.finish();
}

// Remove p99 latency benchmark as it was giving incorrect results

criterion_group!(
    benches,
    benchmark_get_latency,
    benchmark_insert_latency,
    benchmark_delete_latency,
    benchmark_mixed_workload
);
criterion_main!(benches);
