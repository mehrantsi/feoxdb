//! Deterministic interleaving hooks.
//!
//! Mirrors the shape of `crash_at` in `storage::write_buffer`: a named point that is
//! compiled to nothing outside `cfg(test)`. Instead of `process::exit`, `pause_at`
//! blocks the arriving thread on a test-controlled rendezvous, so a test can hold one
//! thread at a chosen instruction boundary while it drives other threads past it.

pub(crate) const AFTER_SECTOR_LOAD: &str = "after_sector_load";
pub(crate) const RECORD_WRITE: &str = "record_write";
pub(crate) const AFTER_UPSERT_READ: &str = "after_upsert_read";
pub(crate) const AFTER_JSON_PATCH_READ: &str = "after_json_patch_read";
#[cfg(test)]
pub(crate) const AFTER_TTL_DEFERRED_SOURCE: &str = "after_ttl_deferred_source";
#[cfg(test)]
pub(crate) const AFTER_CACHE_BUCKET_CLEAR: &str = "after_cache_bucket_clear";
#[cfg(test)]
pub(crate) const TTL_AFTER_EXPIRED_SAMPLE: &str = "ttl_after_expired_sample";

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn pause_at(_: &'static str) {}

#[cfg(test)]
#[inline]
pub(crate) fn pause_at(point: &'static str) {
    if gate::any_armed() {
        gate::arrive(point, std::thread::current().id());
    }
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn new_fault_scope() -> usize {
    0
}

#[cfg(test)]
pub(crate) fn new_fault_scope() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_SCOPE: AtomicUsize = AtomicUsize::new(1);
    NEXT_SCOPE.fetch_add(1, Ordering::Relaxed)
}

/// True when a test has armed `point` for this store. The caller that must fail
/// is usually a flusher whose `ThreadId` the test cannot name.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn fail_at(_: &'static str, _: usize) -> bool {
    false
}

#[cfg(test)]
#[inline]
pub(crate) fn fail_at(point: &'static str, scope: usize) -> bool {
    fault::consume(point, scope)
}

#[cfg(test)]
pub(crate) mod fault {
    use once_cell::sync::Lazy;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fast path. No fault armed means `fail_at` is a single relaxed load.
    static ARMED: AtomicUsize = AtomicUsize::new(0);

    struct State {
        point: &'static str,
        scope: usize,
        budget: usize,
        consumed: usize,
    }

    static STATE: Lazy<Mutex<Option<State>>> = Lazy::new(|| Mutex::new(None));

    #[inline]
    pub(crate) fn consume(point: &'static str, scope: usize) -> bool {
        if ARMED.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let mut state = STATE.lock();
        match state.as_mut() {
            Some(armed) if armed.point == point && armed.scope == scope && armed.budget > 0 => {
                armed.budget -= 1;
                armed.consumed += 1;
                if armed.budget == 0 {
                    ARMED.store(0, Ordering::Release);
                }
                true
            }
            _ => false,
        }
    }

    /// Make the next `times` arrivals at `point` fail. Call inside a
    /// `gate::session()` so two fault-using tests cannot arm concurrently.
    pub fn fail_next(point: &'static str, scope: usize, times: usize) -> ArmedFault {
        *STATE.lock() = Some(State {
            point,
            scope,
            budget: times,
            consumed: 0,
        });
        ARMED.store(1, Ordering::Release);
        ArmedFault
    }

    /// Disarms on drop, so a panicking test cannot leave the fault armed for the
    /// rest of the test binary.
    pub struct ArmedFault;

    impl ArmedFault {
        pub fn consumed(&self) -> usize {
            STATE.lock().as_ref().map_or(0, |armed| armed.consumed)
        }
    }

    impl Drop for ArmedFault {
        fn drop(&mut self) {
            ARMED.store(0, Ordering::Release);
            *STATE.lock() = None;
        }
    }
}

#[cfg(test)]
pub(crate) mod gate {
    use once_cell::sync::Lazy;
    use parking_lot::{Condvar, Mutex, MutexGuard};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::ThreadId;
    use std::time::{Duration, Instant};

    /// Safety valve: a parked thread gives up after this long so a forgotten
    /// `release()` degrades into a test failure instead of a wedged process.
    const MAX_PARK: Duration = Duration::from_secs(20);

    /// Fast path. Zero gates armed means `pause_at` is a single relaxed load.
    static ARMED: AtomicUsize = AtomicUsize::new(0);

    type Registry = HashMap<(ThreadId, &'static str), Arc<Gate>>;

    static REGISTRY: Lazy<Mutex<Registry>> = Lazy::new(|| Mutex::new(Registry::new()));

    /// Serializes hook-using tests against each other within the test binary.
    static EXCLUSIVE: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    #[inline]
    pub(crate) fn any_armed() -> bool {
        ARMED.load(Ordering::Relaxed) != 0
    }

    struct State {
        capacity: usize,
        arrived: usize,
        parked: usize,
        open: bool,
        timed_out: usize,
    }

    struct Gate {
        state: Mutex<State>,
        cv: Condvar,
    }

    impl Gate {
        fn new(capacity: usize) -> Self {
            Self {
                state: Mutex::new(State {
                    capacity,
                    arrived: 0,
                    parked: 0,
                    open: false,
                    timed_out: 0,
                }),
                cv: Condvar::new(),
            }
        }
    }

    pub(crate) fn arrive(point: &'static str, owner: ThreadId) {
        let gate = {
            let reg = REGISTRY.lock();
            match reg.get(&(owner, point)) {
                Some(g) => Arc::clone(g),
                None => return,
            }
        };

        let mut st = gate.state.lock();
        st.arrived += 1;
        if st.capacity == 0 || st.open {
            // Counting-only arming, or the gate is already open: record and pass through.
            gate.cv.notify_all();
            return;
        }
        st.capacity -= 1;
        st.parked += 1;
        gate.cv.notify_all();

        let deadline = Instant::now() + MAX_PARK;
        while !st.open {
            if gate.cv.wait_until(&mut st, deadline).timed_out() {
                st.timed_out += 1;
                break;
            }
        }
        st.parked -= 1;
        gate.cv.notify_all();
    }

    /// Exclusive hook session. Only one exists at a time in the test binary, so two
    /// hook tests can never arm the same point concurrently.
    pub struct Session {
        _excl: MutexGuard<'static, ()>,
    }

    pub fn session() -> Session {
        Session {
            _excl: EXCLUSIVE.lock(),
        }
    }

    impl Session {
        /// Arm `point` to trap the next `capacity` arrivals from `owner`.
        pub fn arm_for_thread(
            &self,
            point: &'static str,
            owner: ThreadId,
            capacity: usize,
        ) -> Armed {
            let gate = Arc::new(Gate::new(capacity));
            REGISTRY.lock().insert((owner, point), Arc::clone(&gate));
            ARMED.fetch_add(1, Ordering::SeqCst);
            Armed { point, owner, gate }
        }
    }

    /// A live arming of one named point. Dropping it releases every parked thread
    /// and unregisters the point, so a panicking test cannot wedge the store.
    #[allow(dead_code)]
    pub struct Armed {
        point: &'static str,
        owner: ThreadId,
        gate: Arc<Gate>,
    }

    #[allow(dead_code)]
    impl Armed {
        /// Block until at least `n` threads have been trapped. False on timeout.
        pub fn wait_for_arrivals(&self, n: usize, timeout: Duration) -> bool {
            let mut st = self.gate.state.lock();
            let deadline = Instant::now() + timeout;
            while st.arrived < n {
                if self.gate.cv.wait_until(&mut st, deadline).timed_out() {
                    return st.arrived >= n;
                }
            }
            true
        }

        /// True if the point stayed cold for `d`.
        pub fn stayed_cold_for(&self, d: Duration) -> bool {
            !self.wait_for_arrivals(1, d)
        }

        pub fn arrivals(&self) -> usize {
            self.gate.state.lock().arrived
        }

        pub fn parked(&self) -> usize {
            self.gate.state.lock().parked
        }

        pub fn timeouts(&self) -> usize {
            self.gate.state.lock().timed_out
        }

        /// Let every parked thread through and stop trapping new arrivals.
        pub fn release(&self) {
            let mut st = self.gate.state.lock();
            st.open = true;
            st.capacity = 0;
            self.gate.cv.notify_all();
        }

        /// Release, then block until no thread is parked here any more.
        pub fn release_and_drain(&self, timeout: Duration) -> bool {
            self.release();
            let mut st = self.gate.state.lock();
            let deadline = Instant::now() + timeout;
            while st.parked > 0 {
                if self.gate.cv.wait_until(&mut st, deadline).timed_out() {
                    return st.parked == 0;
                }
            }
            true
        }
    }

    impl Drop for Armed {
        fn drop(&mut self) {
            self.release();
            REGISTRY.lock().remove(&(self.owner, self.point));
            ARMED.fetch_sub(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut st = self.gate.state.lock();
            while st.parked > 0 {
                if self.gate.cv.wait_until(&mut st, deadline).timed_out() {
                    break;
                }
            }
        }
    }
}
