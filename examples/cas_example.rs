use feoxdb::FeoxStore;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

fn main() -> feoxdb::Result<()> {
    println!("=== Compare-and-Swap Examples ===\n");

    // Example 1: Configuration versioning
    configuration_example()?;

    // Example 2: State machine transitions
    state_machine_example()?;

    // Example 3: Optimistic locking for updates
    optimistic_update_example()?;

    Ok(())
}

fn configuration_example() -> feoxdb::Result<()> {
    println!("1. Configuration Versioning");
    println!("   Using CAS to safely update configuration:");

    let store = FeoxStore::new(None)?;

    // Initial config
    let config_v1 = r#"{"timeout": 30, "retries": 3}"#;
    store.insert(b"app_config", config_v1.as_bytes())?;
    println!("   Initial config: {}", config_v1);

    // Update config only if it hasn't changed
    let config_v2 = r#"{"timeout": 60, "retries": 5}"#;
    if store.compare_and_swap(b"app_config", config_v1.as_bytes(), config_v2.as_bytes())? {
        println!("   ✓ Config updated to: {}", config_v2);
    } else {
        println!("   ✗ Config was modified by another process");
    }

    // This will fail because config is now v2
    if store.compare_and_swap(b"app_config", config_v1.as_bytes(), config_v2.as_bytes())? {
        println!("   ✓ Config updated again");
    } else {
        println!("   ✗ Update failed - config already changed");
    }

    println!();
    Ok(())
}

fn state_machine_example() -> feoxdb::Result<()> {
    println!("2. State Machine Transitions");
    println!("   Using CAS to ensure valid state transitions:");

    let store = Arc::new(FeoxStore::new(None)?);

    // Order processing state machine: PENDING -> PROCESSING -> COMPLETED
    store.insert(b"order:1234:status", b"PENDING")?;

    let num_workers = 3;
    let barrier = Arc::new(Barrier::new(num_workers));
    let mut handles = vec![];

    let start = Instant::now();

    for worker_id in 0..num_workers {
        let store_clone = Arc::clone(&store);
        let barrier_clone = Arc::clone(&barrier);

        handles.push(thread::spawn(move || -> feoxdb::Result<String> {
            barrier_clone.wait();

            // Try to claim the order for processing
            if store_clone.compare_and_swap(b"order:1234:status", b"PENDING", b"PROCESSING")? {
                println!("   Worker {} claimed the order", worker_id);

                // Simulate processing
                std::thread::sleep(std::time::Duration::from_millis(50));

                // Mark as completed
                store_clone.compare_and_swap(b"order:1234:status", b"PROCESSING", b"COMPLETED")?;

                return Ok(format!("Worker {} completed the order", worker_id));
            }

            Ok(format!("Worker {} couldn't claim the order", worker_id))
        }));
    }

    let results: Vec<String> = handles
        .into_iter()
        .map(|h| h.join().unwrap().unwrap())
        .collect();

    let elapsed = start.elapsed();

    for result in results {
        println!("   {}", result);
    }

    let final_status = store.get(b"order:1234:status")?;
    println!(
        "   Final status: {}",
        String::from_utf8_lossy(&final_status)
    );
    println!("   Time: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
    println!();

    assert_eq!(final_status, b"COMPLETED");

    Ok(())
}

fn optimistic_update_example() -> feoxdb::Result<()> {
    println!("3. Optimistic Locking Pattern");
    println!("   Updating user balance with retry logic:");

    let store = FeoxStore::new(None)?;

    // Initial balance
    store.insert(b"user:1234:balance", b"1000")?;

    // Function to safely deduct amount
    let deduct_balance = |amount: i32| -> feoxdb::Result<bool> {
        const MAX_RETRIES: u32 = 3;

        for attempt in 1..=MAX_RETRIES {
            // Read current balance
            let current = store.get(b"user:1234:balance")?;
            let balance: i32 = String::from_utf8_lossy(&current).parse().unwrap();

            // Check if sufficient funds
            if balance < amount {
                println!(
                    "   ✗ Insufficient funds (balance: {}, required: {})",
                    balance, amount
                );
                return Ok(false);
            }

            // Try to update
            let new_balance = (balance - amount).to_string();
            if store.compare_and_swap(b"user:1234:balance", &current, new_balance.as_bytes())? {
                println!(
                    "   ✓ Deducted {} (attempt {}), new balance: {}",
                    amount, attempt, new_balance
                );
                return Ok(true);
            }

            if attempt < MAX_RETRIES {
                println!("   ↻ Retry {} - balance changed during read", attempt);
            }
        }

        println!("   ✗ Failed after {} retries", MAX_RETRIES);
        Ok(false)
    };

    // Perform transactions
    deduct_balance(200)?;
    deduct_balance(300)?;
    deduct_balance(600)?; // This should fail - insufficient funds

    println!();
    Ok(())
}
