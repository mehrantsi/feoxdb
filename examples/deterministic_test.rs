use feoxdb::error::Result;
use feoxdb::FeoxStore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ANSI color codes
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_MAGENTA: &str = "\x1b[35m";
const ANSI_CYAN: &str = "\x1b[36m";
const CLEAR_LINE: &str = "\x1b[2K";

#[derive(Default)]
struct OperationStats {
    operations: AtomicUsize,
    errors: AtomicUsize,
    total_latency_ns: AtomicUsize,
    min_latency_ns: AtomicUsize,
    max_latency_ns: AtomicUsize,
    // For percentile calculation, we'll sample some operations
    samples: parking_lot::Mutex<Vec<u64>>,
}

impl OperationStats {
    fn new() -> Self {
        Self {
            operations: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            total_latency_ns: AtomicUsize::new(0),
            min_latency_ns: AtomicUsize::new(usize::MAX),
            max_latency_ns: AtomicUsize::new(0),
            samples: parking_lot::Mutex::new(Vec::with_capacity(10000)),
        }
    }

    fn record(&self, latency_ns: u64, success: bool) {
        if success {
            self.operations.fetch_add(1, Ordering::Relaxed);
            self.total_latency_ns
                .fetch_add(latency_ns as usize, Ordering::Relaxed);

            // Update min latency
            let mut current_min = self.min_latency_ns.load(Ordering::Relaxed);
            while (latency_ns as usize) < current_min {
                match self.min_latency_ns.compare_exchange_weak(
                    current_min,
                    latency_ns as usize,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => current_min = x,
                }
            }

            // Update max latency
            let mut current_max = self.max_latency_ns.load(Ordering::Relaxed);
            while (latency_ns as usize) > current_max {
                match self.max_latency_ns.compare_exchange_weak(
                    current_max,
                    latency_ns as usize,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => current_max = x,
                }
            }

            // Sample for percentiles (reservoir sampling)
            let mut samples = self.samples.lock();
            if samples.len() < 10000 {
                samples.push(latency_ns);
            } else {
                let ops = self.operations.load(Ordering::Relaxed);
                if rand::random::<u64>() as usize % ops < 10000 {
                    samples[rand::random::<u64>() as usize % 10000] = latency_ns;
                }
            }
        } else {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_percentile(&self, percentile: f64) -> f64 {
        let mut samples = self.samples.lock().clone();
        if samples.is_empty() {
            return 0.0;
        }
        samples.sort_unstable();
        let index = ((samples.len() as f64 * percentile / 100.0) as usize).min(samples.len() - 1);
        samples[index] as f64 / 1000.0 // Convert to microseconds
    }

    fn get_stats(&self) -> (usize, usize, f64, f64, f64, f64) {
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

        // Calculate standard deviation
        let variance = if ops > 1 {
            let mean_ns = total_ns as f64 / ops as f64;
            let samples = self.samples.lock();
            let sum_sq: f64 = samples
                .iter()
                .map(|&x| {
                    let diff = x as f64 - mean_ns;
                    diff * diff
                })
                .sum();
            sum_sq / samples.len() as f64
        } else {
            0.0
        };
        let std_dev_us = (variance.sqrt()) / 1000.0;

        (ops, errors, avg_us, min_us, max_us, std_dev_us)
    }
}

fn generate_key(id: usize) -> Vec<u8> {
    format!("key_{:08}", id).into_bytes()
}

fn generate_value(size: usize) -> Vec<u8> {
    let mut value = vec![b'V'; size];
    // Add some variation to make it more realistic
    for i in (0..size).step_by(10) {
        value[i] = b'A' + ((i % 26) as u8);
    }
    value
}

fn display_progress(current: usize, total: usize, phase: &str, ops_per_sec: f64) {
    let progress = (current as f64 / total as f64) * 100.0;
    let bar_width = 50;
    let filled = (progress * bar_width as f64 / 100.0) as usize;

    print!("\r{}Progress: [", CLEAR_LINE);
    for i in 0..bar_width {
        if i < filled {
            print!("█");
        } else {
            print!("░");
        }
    }
    print!("] {:.1}% | {} | ", progress, phase);
    print!("{}{:.0} ops/sec{}", ANSI_YELLOW, ops_per_sec, ANSI_RESET);
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
}

fn display_operation_stats(name: &str, stats: &OperationStats, color: &str, phase_time: f64) {
    let (ops, errors, avg_us, min_us, max_us, std_dev_us) = stats.get_stats();

    if ops == 0 && errors == 0 {
        return;
    }

    println!("\n{}{}{} Operations:{}", ANSI_BOLD, color, name, ANSI_RESET);
    println!("  Count: {}", ops);
    println!("  Errors: {}", errors);
    if phase_time > 0.0 {
        println!(
            "  Throughput: {}{:.0} ops/sec{}",
            color,
            ops as f64 / phase_time,
            ANSI_RESET
        );
    }

    if ops > 0 {
        println!("  Latency (μs):");
        println!("    Min:    {:.2}", min_us);
        println!("    Max:    {:.2}", max_us);
        println!("    Avg:    {:.2}", avg_us);
        println!("    StdDev: {:.2}", std_dev_us);
        println!("    P50:    {:.2}", stats.get_percentile(50.0));
        println!("    P95:    {:.2}", stats.get_percentile(95.0));
        println!("    P99:    {:.2}", stats.get_percentile(99.0));
    }
}

fn main() -> Result<()> {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!(
            "Usage: {} <num_keys> [value_size] [--persist] [--file <path>]",
            args[0]
        );
        println!("  num_keys:    Number of keys to test (will insert, get, then delete all)");
        println!("  value_size:  Value size in bytes (default: 300)");
        println!("  --persist:   Use persistent storage with default temp file");
        println!("  --file <path>: Use custom persistence file (must exist)");
        println!();
        println!("Examples:");
        println!("  {} 100000", args[0]);
        println!("  {} 100000 500 --persist", args[0]);
        println!("  {} 100000 500 --file /path/to/custom.dat", args[0]);
        println!();
        println!("Creating a custom persistence file:");
        println!("  # Create a 100MB file:");
        println!("  dd if=/dev/zero of=test.dat bs=1M count=100");
        println!();
        println!("  # Create a 1GB file:");
        println!("  dd if=/dev/zero of=test.dat bs=1G count=1");
        return Ok(());
    }

    let num_keys = args[1].parse::<usize>().unwrap_or(100_000);

    // Parse value_size (handle it being --persist or --file)
    let mut value_size = 300;
    let mut custom_file: Option<String> = None;
    let mut use_persistence = false;

    let mut i = 2;
    while i < args.len() {
        if args[i] == "--persist" {
            use_persistence = true;
        } else if args[i] == "--file" {
            if i + 1 < args.len() {
                custom_file = Some(args[i + 1].clone());
                use_persistence = true;
                i += 1; // Skip the file path argument
            } else {
                eprintln!("Error: --file requires a path argument");
                return Ok(());
            }
        } else if let Ok(size) = args[i].parse::<usize>() {
            value_size = size;
        }
        i += 1;
    }

    // Validate custom file if specified
    if let Some(ref file_path) = custom_file {
        use std::path::Path;
        if !Path::new(file_path).exists() {
            eprintln!(
                "Error: Custom persistence file '{}' does not exist",
                file_path
            );
            eprintln!();
            eprintln!("Please create the file first. Example:");
            eprintln!("  dd if=/dev/zero of={} bs=1M count=100", file_path);
            return Ok(());
        }

        // Display file size info
        use std::fs;
        if let Ok(metadata) = fs::metadata(file_path) {
            let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
            println!(
                "Using custom persistence file: {} ({:.2} MB)",
                file_path, size_mb
            );
        }
    }

    // Display configuration
    println!(
        "{}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{}",
        ANSI_CYAN, ANSI_RESET
    );
    println!(
        "{}Starting Deterministic Performance Test{}",
        ANSI_BOLD, ANSI_RESET
    );
    println!("Key count: {}", num_keys);
    println!("Value size: {} bytes", value_size);
    println!(
        "Mode: {}",
        if use_persistence {
            if custom_file.is_some() {
                "Persistent (custom file)"
            } else {
                "Persistent (default temp)"
            }
        } else {
            "Memory"
        }
    );
    println!(
        "{}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{}\n",
        ANSI_CYAN, ANSI_RESET
    );

    // Initialize store
    let device_path = if let Some(custom) = custom_file {
        Some(custom)
    } else if use_persistence {
        #[cfg(unix)]
        let path = "/tmp/feox_deterministic_test.dat".to_string();

        #[cfg(windows)]
        let path = {
            let temp_dir = std::env::temp_dir();
            temp_dir
                .join("feox_deterministic_test.dat")
                .to_string_lossy()
                .to_string()
        };

        #[cfg(not(any(unix, windows)))]
        let path = "feox_deterministic_test.dat".to_string();

        Some(path)
    } else {
        None
    };

    let store = Arc::new(FeoxStore::new(device_path)?);

    // Statistics for each operation type
    let insert_stats = Arc::new(OperationStats::new());
    let get_stats = Arc::new(OperationStats::new());
    let delete_stats = Arc::new(OperationStats::new());

    let total_start = Instant::now();

    // Phase 1: INSERT all keys
    println!("{}Phase 1: INSERT{}", ANSI_GREEN, ANSI_RESET);
    let phase_start = Instant::now();
    for i in 0..num_keys {
        let key = generate_key(i);
        let value = generate_value(value_size);

        let op_start = Instant::now();
        let success = store.insert(&key, &value).is_ok();
        let latency_ns = op_start.elapsed().as_nanos() as u64;

        insert_stats.record(latency_ns, success);

        // Update progress every 1000 operations
        if i % 1000 == 0 || i == num_keys - 1 {
            let elapsed = phase_start.elapsed().as_secs_f64();
            let ops_per_sec = if elapsed > 0.0 {
                (i + 1) as f64 / elapsed
            } else {
                0.0
            };
            display_progress(i + 1, num_keys, "INSERT", ops_per_sec);
        }
    }
    let insert_time = phase_start.elapsed().as_secs_f64();
    println!();

    // Phase 2: GET all keys
    println!("\n{}Phase 2: GET{}", ANSI_BLUE, ANSI_RESET);

    let phase_start = Instant::now();
    for i in 0..num_keys {
        let key = generate_key(i);

        let op_start = Instant::now();
        let success = store.get_bytes(&key).is_ok();
        let latency_ns = op_start.elapsed().as_nanos() as u64;

        get_stats.record(latency_ns, success);

        // Update progress every 1000 operations
        if i % 1000 == 0 || i == num_keys - 1 {
            let elapsed = phase_start.elapsed().as_secs_f64();
            let ops_per_sec = if elapsed > 0.0 {
                (i + 1) as f64 / elapsed
            } else {
                0.0
            };
            display_progress(i + 1, num_keys, "GET", ops_per_sec);
        }
    }
    let get_time = phase_start.elapsed().as_secs_f64();
    println!();

    // Phase 3: DELETE all keys
    println!("\n{}Phase 3: DELETE{}", ANSI_MAGENTA, ANSI_RESET);
    let phase_start = Instant::now();
    for i in 0..num_keys {
        let key = generate_key(i);

        let op_start = Instant::now();
        let success = store.delete(&key).is_ok();
        let latency_ns = op_start.elapsed().as_nanos() as u64;

        delete_stats.record(latency_ns, success);

        // Update progress every 1000 operations
        if i % 1000 == 0 || i == num_keys - 1 {
            let elapsed = phase_start.elapsed().as_secs_f64();
            let ops_per_sec = if elapsed > 0.0 {
                (i + 1) as f64 / elapsed
            } else {
                0.0
            };
            display_progress(i + 1, num_keys, "DELETE", ops_per_sec);
        }
    }
    let delete_time = phase_start.elapsed().as_secs_f64();
    println!();

    let total_time = total_start.elapsed().as_secs_f64();
    let total_operations = num_keys * 3; // INSERT + GET + DELETE

    // Display results
    println!(
        "\n{}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{}",
        ANSI_CYAN, ANSI_RESET
    );
    println!("{}Performance Results{}", ANSI_BOLD, ANSI_RESET);
    println!(
        "{}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{}",
        ANSI_CYAN, ANSI_RESET
    );

    println!("\n{}Overall Performance:{}", ANSI_BOLD, ANSI_RESET);
    println!("  Total Operations: {}", total_operations);
    println!("  Total Time: {:.3} seconds", total_time);
    println!(
        "  Overall Throughput: {}{:.0} ops/sec{}",
        ANSI_GREEN,
        total_operations as f64 / total_time,
        ANSI_RESET
    );

    // Display per-operation statistics
    display_operation_stats("INSERT", &insert_stats, ANSI_GREEN, insert_time);
    display_operation_stats("GET", &get_stats, ANSI_BLUE, get_time);
    display_operation_stats("DELETE", &delete_stats, ANSI_MAGENTA, delete_time);

    println!(
        "\n{}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{}",
        ANSI_CYAN, ANSI_RESET
    );

    // If using persistence, ensure data is flushed
    if use_persistence {
        print!("\n{}Flushing to disk...{} ", ANSI_YELLOW, ANSI_RESET);
        store.flush();
        println!("{}Done!{}", ANSI_GREEN, ANSI_RESET);
    }

    // Display final statistics
    println!("\n{}Record Count:{} {}", ANSI_CYAN, ANSI_RESET, store.len());

    Ok(())
}
