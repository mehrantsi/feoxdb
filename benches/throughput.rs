use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use feoxdb::FeoxStore;
use rand::Rng;
use std::hint::black_box;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for size in &[1000, 10000, 100000, 1000000] {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched_ref(
                || {
                    let store = FeoxStore::new(None).unwrap();
                    let keys: Vec<Vec<u8>> = (0..size)
                        .map(|i| format!("key_{:08}", i).into_bytes())
                        .collect();
                    let values: Vec<Vec<u8>> = (0..size).map(|_| vec![0u8; 100]).collect();
                    (store, keys, values)
                },
                |(store, keys, values)| {
                    for (key, value) in keys.iter().zip(values.iter()) {
                        store.insert(black_box(key), black_box(value)).unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    for size in &[1000, 10000, 100000, 1000000] {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let store = FeoxStore::new(None).unwrap();
            let keys: Vec<Vec<u8>> = (0..size)
                .map(|i| format!("key_{:08}", i).into_bytes())
                .collect();

            // Pre-populate the store
            for key in &keys {
                store.insert(key, b"value").unwrap();
            }

            b.iter(|| {
                for key in &keys {
                    black_box(store.get_bytes(black_box(key)).unwrap());
                }
            });
        });
    }
    group.finish();
}

fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_operations");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("80_20_read_write", |b| {
        b.iter_batched_ref(
            || {
                let store = FeoxStore::new(None).unwrap();
                // Pre-populate without timestamps
                for i in 0..1000 {
                    let key = format!("key_{:08}", i);
                    store.insert(key.as_bytes(), b"value").unwrap();
                }
                store
            },
            |store| {
                let mut rng = rand::rng();

                for _ in 0..1000 {
                    let key_id = rng.random_range(0..1000);
                    let key = format!("key_{:08}", key_id);

                    if rng.random_range(0..100) < 80 {
                        // 80% reads
                        let _ = black_box(store.get_bytes(key.as_bytes()));
                    } else {
                        // 20% writes - let the store handle timestamps automatically
                        let _ = store.insert(key.as_bytes(), b"new_value");
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_insert, bench_get, bench_mixed);
criterion_main!(benches);
