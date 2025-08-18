use crate::utils::hash::{hash_key, murmur3_32, MurmurHasher};
use std::hash::Hasher;

#[test]
fn test_murmur3_consistency() {
    let test_cases = vec![
        (&b""[..], 0u32, 0u32),
        (&b"a"[..], 0, 0x3c2569b2),
        (&b"ab"[..], 0, 0x9bbfd75f),
        (&b"abc"[..], 0, 0xb3dd93fa),
        (&b"abcd"[..], 0, 0x43ed676a),
        (&b"hello"[..], 0, 0x248bfa47),
        (&b"hello world"[..], 0, 0x5e928f0f),
    ];

    for (input, seed, expected) in test_cases {
        let result = murmur3_32(input, seed);
        assert_eq!(result, expected, "Failed for input: {:?}", input);
    }
}

#[test]
fn test_hash_key_function() {
    let key1 = b"test_key";
    let key2 = b"test_key";
    let key3 = b"different_key";

    // Same keys should produce same hash
    assert_eq!(hash_key(key1), hash_key(key2));

    // Different keys should (almost always) produce different hashes
    assert_ne!(hash_key(key1), hash_key(key3));
}

#[test]
fn test_murmur_hasher() {
    let mut hasher1 = MurmurHasher::new();
    hasher1.write(b"test");
    let hash1 = hasher1.finish();

    let mut hasher2 = MurmurHasher::new();
    hasher2.write(b"test");
    let hash2 = hasher2.finish();

    assert_eq!(hash1, hash2);

    let mut hasher3 = MurmurHasher::new();
    hasher3.write(b"different");
    let hash3 = hasher3.finish();

    assert_ne!(hash1, hash3);
}

#[test]
fn test_murmur_hasher_with_seed() {
    let mut hasher1 = MurmurHasher::with_seed(12345);
    hasher1.write(b"test");
    let hash1 = hasher1.finish();

    let mut hasher2 = MurmurHasher::with_seed(12345);
    hasher2.write(b"test");
    let hash2 = hasher2.finish();

    assert_eq!(hash1, hash2);

    let mut hasher3 = MurmurHasher::with_seed(54321);
    hasher3.write(b"test");
    let hash3 = hasher3.finish();

    assert_ne!(hash1, hash3);
}

#[test]
fn test_murmur3_different_seeds() {
    let data = b"test data";

    let hash1 = murmur3_32(data, 0);
    let hash2 = murmur3_32(data, 1);
    let hash3 = murmur3_32(data, 12345);

    // Different seeds should produce different hashes
    assert_ne!(hash1, hash2);
    assert_ne!(hash1, hash3);
    assert_ne!(hash2, hash3);
}

#[test]
fn test_murmur3_various_lengths() {
    // Test with various input lengths to cover different code paths
    let test_cases = vec![
        vec![],              // Empty
        vec![1],             // 1 byte
        vec![1, 2, 3],       // 3 bytes (< 4)
        vec![1, 2, 3, 4],    // Exactly 4 bytes
        vec![1, 2, 3, 4, 5], // 5 bytes (> 4)
        vec![1; 100],        // Large input
    ];

    for data in test_cases {
        let hash = murmur3_32(&data, 0);
        // Just verify it doesn't panic and produces a value
        assert!(hash != 0 || data.is_empty());
    }
}

#[test]
fn test_hash_distribution() {
    use std::collections::HashSet;

    // Generate hashes for sequential keys
    let mut hashes = HashSet::new();
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let hash = hash_key(key.as_bytes());
        hashes.insert(hash);
    }

    // Should have very few collisions (ideally 1000 unique hashes)
    assert!(
        hashes.len() > 990,
        "Too many hash collisions: {}/1000",
        hashes.len()
    );
}

#[test]
fn test_murmur_hasher_default() {
    let mut hasher1 = MurmurHasher::default();
    let mut hasher2 = MurmurHasher::new();

    // Default should produce same hash for same input
    hasher1.write(b"test");
    hasher2.write(b"test");
    assert_eq!(hasher1.finish(), hasher2.finish());
}

#[test]
fn test_hash_consistency_across_calls() {
    let key = b"consistent_key";

    let mut hashes = Vec::new();
    for _ in 0..10 {
        hashes.push(hash_key(key));
    }

    // All hashes should be identical
    for hash in &hashes[1..] {
        assert_eq!(hashes[0], *hash);
    }
}

#[test]
fn test_murmur3_known_values() {
    // Test against known good values for murmur3
    assert_eq!(murmur3_32(b"", 0), 0);
    assert_eq!(murmur3_32(b"", 1), 0x514e28b7);
    assert_eq!(murmur3_32(b"", 0xffffffff), 0x81f16f39);

    assert_eq!(murmur3_32(b"\0\0\0\0", 0), 0x2362f9de);
    assert_eq!(murmur3_32(b"aaaa", 0x9747b28c), 0x5a97808a);
    assert_eq!(murmur3_32(b"Hello, world!", 0x9747b28c), 0x24884cba);
}

#[test]
fn test_hasher_trait_implementation() {
    // Verify our hasher implements the trait correctly
    let mut our_hasher = MurmurHasher::new();
    our_hasher.write(b"test");
    our_hasher.write(b"data");
    let our_hash = our_hasher.finish();

    // Should produce consistent results
    let mut our_hasher2 = MurmurHasher::new();
    our_hasher2.write(b"test");
    our_hasher2.write(b"data");
    let our_hash2 = our_hasher2.finish();

    assert_eq!(our_hash, our_hash2);
}
