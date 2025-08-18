use std::hash::Hasher;

const C1: u32 = 0xcc9e2d51;
const C2: u32 = 0x1b873593;
const R1: u32 = 15;
const R2: u32 = 13;
const M: u32 = 5;
const N: u32 = 0xe6546b64;

#[inline(always)]
pub fn murmur3_32(key: &[u8], seed: u32) -> u32 {
    let mut h = seed;
    let mut chunks = key.chunks_exact(4);

    for chunk in chunks.by_ref() {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(R1);
        k = k.wrapping_mul(C2);

        h ^= k;
        h = h.rotate_left(R2);
        h = h.wrapping_mul(M).wrapping_add(N);
    }

    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut k = 0u32;
        for (i, &byte) in remainder.iter().enumerate() {
            k |= (byte as u32) << (i * 8);
        }

        k = k.wrapping_mul(C1);
        k = k.rotate_left(R1);
        k = k.wrapping_mul(C2);
        h ^= k;
    }

    h ^= key.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;

    h
}

/// Compute hash of a key using the best available method.
///
/// On x86_64 CPUs with AES-NI support, uses hardware-accelerated hashing.
/// Otherwise falls back to MurmurHash3.
pub fn hash_key(key: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if let Some(hash) = simd::hash_key_aes_safe(key) {
            return hash;
        }
    }
    murmur3_32(key, 0)
}

#[cfg(target_arch = "x86_64")]
pub mod simd {
    use std::arch::x86_64::*;

    /// Compute hash using AES-NI instructions for better performance.
    ///
    /// Returns None if AES-NI is not supported on this CPU.
    pub fn hash_key_aes_safe(key: &[u8]) -> Option<u32> {
        if is_x86_feature_detected!("aes") {
            unsafe { Some(hash_key_aes_unsafe(key)) }
        } else {
            None
        }
    }

    /// Compute hash using AES-NI instructions for better performance.
    ///
    /// # Safety
    ///
    /// This function requires the CPU to support AES-NI instructions.
    /// The caller must ensure that the CPU supports AES-NI before calling this function.
    /// This is typically checked using `is_x86_feature_detected!("aes")`.
    #[target_feature(enable = "aes")]
    #[inline]
    unsafe fn hash_key_aes_unsafe(key: &[u8]) -> u32 {
        // Use a non-zero initial seed for better distribution
        let seed = _mm_set_epi32(
            0x9747b28c_u32 as i32,
            0xc2b2ae35_u32 as i32,
            0x85ebca6b_u32 as i32,
            0xe6546b64_u32 as i32,
        );
        let mut hash = seed;

        // Process full 16-byte chunks
        let mut chunks = key.chunks_exact(16);
        for chunk in chunks.by_ref() {
            let data = _mm_loadu_si128(chunk.as_ptr() as *const __m128i);
            // Mix the data with XOR before AES round
            hash = _mm_xor_si128(hash, data);
            // Apply AES round
            hash = _mm_aesenc_si128(hash, seed);
        }

        // Handle remainder
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut buffer = [0u8; 16];
            buffer[..remainder.len()].copy_from_slice(remainder);
            // Mix in the length of remainder for better distribution
            buffer[15] = remainder.len() as u8;

            let data = _mm_loadu_si128(buffer.as_ptr() as *const __m128i);
            hash = _mm_xor_si128(hash, data);
            hash = _mm_aesenc_si128(hash, seed);
        }

        // Final mixing: incorporate key length
        let len_vec = _mm_set1_epi32(key.len() as i32);
        hash = _mm_xor_si128(hash, len_vec);
        hash = _mm_aesenc_si128(hash, seed);

        // Extract and mix all 4 32-bit parts for better distribution
        let a = _mm_extract_epi32(hash, 0) as u32;
        let b = _mm_extract_epi32(hash, 1) as u32;
        let c = _mm_extract_epi32(hash, 2) as u32;
        let d = _mm_extract_epi32(hash, 3) as u32;

        // Final mixing similar to MurmurHash3 finalizer
        let mut h = a ^ b ^ c ^ d;
        h ^= h >> 16;
        h = h.wrapping_mul(0x85ebca6b);
        h ^= h >> 13;
        h = h.wrapping_mul(0xc2b2ae35);
        h ^= h >> 16;

        h
    }
}

pub struct MurmurHasher {
    state: u32,
}

impl Default for MurmurHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl MurmurHasher {
    pub fn new() -> Self {
        Self { state: 0 }
    }

    pub fn with_seed(seed: u32) -> Self {
        Self { state: seed }
    }
}

impl Hasher for MurmurHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.state = murmur3_32(bytes, self.state);
    }

    fn finish(&self) -> u64 {
        self.state as u64
    }
}
