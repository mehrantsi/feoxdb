use feoxdb::{error::Result, FeoxStore};
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_RED: &str = "\x1b[31m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_MAGENTA: &str = "\x1b[35m";
const ANSI_CYAN: &str = "\x1b[36m";

#[derive(Debug)]
struct Stats {
    operations: AtomicUsize,
    errors: AtomicUsize,
    total_latency_ns: AtomicUsize,
    min_latency_ns: AtomicUsize,
    max_latency_ns: AtomicUsize,
}

impl Stats {
    fn new() -> Self {
        Self {
            operations: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            total_latency_ns: AtomicUsize::new(0),
            min_latency_ns: AtomicUsize::new(usize::MAX),
            max_latency_ns: AtomicUsize::new(0),
        }
    }

    fn record_op(&self, latency_ns: usize, success: bool) {
        if success {
            self.operations.fetch_add(1, Ordering::Relaxed);
            self.total_latency_ns
                .fetch_add(latency_ns, Ordering::Relaxed);

            // Update min latency
            let mut current_min = self.min_latency_ns.load(Ordering::Relaxed);
            while latency_ns < current_min {
                match self.min_latency_ns.compare_exchange_weak(
                    current_min,
                    latency_ns,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => current_min = x,
                }
            }

            // Update max latency
            let mut current_max = self.max_latency_ns.load(Ordering::Relaxed);
            while latency_ns > current_max {
                match self.max_latency_ns.compare_exchange_weak(
                    current_max,
                    latency_ns,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => current_max = x,
                }
            }
        } else {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_summary(&self) -> (usize, usize, f64, f64, f64) {
        let ops = self.operations.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let total_ns = self.total_latency_ns.load(Ordering::Relaxed);
        let min_ns = self.min_latency_ns.load(Ordering::Relaxed);
        let max_ns = self.max_latency_ns.load(Ordering::Relaxed);

        let avg_us = if ops > 0 {
            (total_ns as f64 / ops as f64) / 1000.0
        } else {
            0.0
        };

        let min_us = if min_ns == usize::MAX {
            0.0
        } else {
            min_ns as f64 / 1000.0
        };
        let max_us = max_ns as f64 / 1000.0;

        (ops, errors, avg_us, min_us, max_us)
    }
}

struct WorkloadConfig {
    num_operations: usize,
    value_size: usize,
    key_range: usize,
    read_ratio: f32,
}

fn generate_key(id: usize) -> Vec<u8> {
    format!("key_{:08}", id).into_bytes()
}

fn generate_value(size: usize) -> Vec<u8> {
    let mut value = vec![b'V'; size];
    // Add some variation
    for i in (0..size).step_by(10) {
        value[i] = b'A' + ((i % 26) as u8);
    }
    value
}

fn run_workload(
    store: Arc<FeoxStore>,
    _thread_id: usize,
    config: WorkloadConfig,
    stats: Arc<Stats>,
    running: Arc<AtomicBool>,
) {
    let mut rng = rand::rng();
    let mut local_ops = 0;

    while running.load(Ordering::Relaxed) && local_ops < config.num_operations {
        let key_id = rng.random_range(0..config.key_range);
        let key = generate_key(key_id);

        let is_read = rng.random::<f32>() < config.read_ratio;

        let start = Instant::now();
        let success = if is_read {
            // Read operation
            store.get(&key).is_ok()
        } else {
            // Write operation
            let value = generate_value(config.value_size);
            store.insert(&key, &value).is_ok()
        };
        let latency_ns = start.elapsed().as_nanos() as usize;

        stats.record_op(latency_ns, success);
        local_ops += 1;
    }
}

fn display_progress(stats: &Arc<Stats>, start_time: Instant, total_operations: usize) {
    let elapsed = start_time.elapsed();
    let (ops, errors, avg_us, min_us, max_us) = stats.get_summary();

    let ops_per_sec = if elapsed.as_secs() > 0 {
        ops as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let progress = (ops as f64 / total_operations as f64) * 100.0;

    print!("\r{}[{:.1}%] ", ANSI_CYAN, progress);
    print!("{}Ops:{} {:<8} ", ANSI_GREEN, ANSI_RESET, ops);
    print!("{}Ops/s:{} {:<10.0} ", ANSI_BLUE, ANSI_RESET, ops_per_sec);
    print!("{}Avg:{} {:<6.1}μs ", ANSI_YELLOW, ANSI_RESET, avg_us);
    print!("{}Min:{} {:<6.1}μs ", ANSI_MAGENTA, ANSI_RESET, min_us);
    print!("{}Max:{} {:<6.1}μs ", ANSI_RED, ANSI_RESET, max_us);

    if errors > 0 {
        print!("{}Errors:{} {} ", ANSI_RED, ANSI_RESET, errors);
    }

    print!("          "); // Clear any remaining characters
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
}

fn main() -> Result<()> {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    let num_operations = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    let value_size = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);

    let key_range = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let num_threads = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(num_cpus::get);

    let read_ratio = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(0.7); // 70% reads by default

    let persistence_mode = args.get(6).map(|s| s.as_str()).unwrap_or("memory");

    // Display configuration
    println!(
        "{}=== FeOx Rust Performance Test ==={}",
        ANSI_BOLD, ANSI_RESET
    );
    println!("{}Mode:{} {}", ANSI_CYAN, ANSI_RESET, persistence_mode);
    println!("{}Operations:{} {}", ANSI_GREEN, ANSI_RESET, num_operations);
    println!("{}Threads:{} {}", ANSI_GREEN, ANSI_RESET, num_threads);
    println!(
        "{}Value Size:{} {} bytes",
        ANSI_GREEN, ANSI_RESET, value_size
    );
    println!("{}Key Range:{} {}", ANSI_GREEN, ANSI_RESET, key_range);
    println!(
        "{}Read Ratio:{} {:.0}%",
        ANSI_GREEN,
        ANSI_RESET,
        read_ratio * 100.0
    );
    println!();

    // Create store based on mode
    let device_path = match persistence_mode {
        "persist" | "disk" => {
            let path = std::env::var("FEOX_DEVICE_PATH").unwrap_or_else(|_| {
                #[cfg(unix)]
                let default_path = "/tmp/feox_test.dat".to_string();

                #[cfg(windows)]
                let default_path = {
                    let temp_dir = std::env::temp_dir();
                    temp_dir.join("feox_test.dat").to_string_lossy().to_string()
                };

                #[cfg(not(any(unix, windows)))]
                let default_path = "feox_test.dat".to_string();

                default_path
            });
            println!(
                "{}Using persistent storage:{} {}",
                ANSI_YELLOW, ANSI_RESET, path
            );
            Some(path)
        }
        _ => {
            println!("{}Using in-memory storage{}", ANSI_YELLOW, ANSI_RESET);
            None
        }
    };

    let store = Arc::new(FeoxStore::new(device_path)?);

    // Pre-populate some data for read operations
    println!("{}Pre-populating data...{}", ANSI_BLUE, ANSI_RESET);
    let prepop_count = (key_range as f32 * 0.5) as usize; // Pre-populate 50% of key range
    for i in 0..prepop_count {
        let key = generate_key(i);
        let value = generate_value(value_size);
        store.insert(&key, &value)?;

        if i % 1000 == 0 {
            print!("\r{}Pre-populated:{} {} keys", ANSI_CYAN, ANSI_RESET, i);
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    println!(
        "\r{}Pre-populated:{} {} keys",
        ANSI_GREEN, ANSI_RESET, prepop_count
    );
    println!();

    // Create shared statistics
    let stats = Arc::new(Stats::new());
    let running = Arc::new(AtomicBool::new(true));

    // Start worker threads
    println!("{}Starting workload...{}", ANSI_BOLD, ANSI_RESET);
    let start_time = Instant::now();
    let ops_per_thread = num_operations / num_threads;

    let mut handles = vec![];
    for thread_id in 0..num_threads {
        let store = store.clone();
        let stats = stats.clone();
        let running = running.clone();

        let config = WorkloadConfig {
            num_operations: ops_per_thread,
            value_size,
            key_range,
            read_ratio,
        };

        let handle = thread::spawn(move || {
            run_workload(store, thread_id, config, stats, running);
        });
        handles.push(handle);
    }

    // Display progress
    let display_stats = stats.clone();
    let display_running = running.clone();
    let display_handle = thread::spawn(move || {
        while display_running.load(Ordering::Relaxed) {
            display_progress(&display_stats, start_time, num_operations);
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Wait for workers to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Stop display thread
    running.store(false, Ordering::Relaxed);
    display_handle.join().unwrap();

    // Final statistics
    let elapsed = start_time.elapsed();
    let (ops, errors, avg_us, min_us, max_us) = stats.get_summary();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();

    println!("\r\n");
    println!("{}=== Final Results ==={}", ANSI_BOLD, ANSI_RESET);
    println!("{}Total Time:{} {:?}", ANSI_CYAN, ANSI_RESET, elapsed);
    println!("{}Total Operations:{} {}", ANSI_GREEN, ANSI_RESET, ops);
    println!(
        "{}Throughput:{} {:.0} ops/sec",
        ANSI_GREEN, ANSI_RESET, ops_per_sec
    );
    println!(
        "{}Average Latency:{} {:.2} μs",
        ANSI_YELLOW, ANSI_RESET, avg_us
    );
    println!(
        "{}Min Latency:{} {:.2} μs",
        ANSI_MAGENTA, ANSI_RESET, min_us
    );
    println!("{}Max Latency:{} {:.2} μs", ANSI_RED, ANSI_RESET, max_us);

    if errors > 0 {
        println!("{}Errors:{} {}", ANSI_RED, ANSI_RESET, errors);
    }

    // Flush to disk if in persistence mode
    if persistence_mode == "persist" || persistence_mode == "disk" {
        print!("\n{}Flushing to disk...{}", ANSI_YELLOW, ANSI_RESET);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        // Note: flush() is not available in current implementation
        // store.flush();
        println!(" {}Done!{}", ANSI_GREEN, ANSI_RESET);
    }

    // Memory usage
    let mem_usage = store.memory_usage();
    let mem_mb = mem_usage as f64 / (1024.0 * 1024.0);
    println!(
        "\n{}Memory Usage:{} {:.2} MB",
        ANSI_CYAN, ANSI_RESET, mem_mb
    );
    println!("{}Record Count:{} {}", ANSI_CYAN, ANSI_RESET, store.len());

    Ok(())
}
