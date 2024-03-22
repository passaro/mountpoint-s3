use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering;
use crate::sync::Arc;

/// Counter with an upper bound.
/// Dispense up to a maximum number of [CounterToken]s, which decrease the counter when dropped.
#[derive(Debug)]
pub struct BoundedCounter {
    max: usize,
    current: Arc<AtomicUsize>,
}

impl BoundedCounter {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            current: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn increment(&self) -> Result<CounterToken, usize> {
        self.current.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |i| {
            if i < self.max {
                Some(i + 1)
            } else {
                None
            }
        })?;
        Ok(CounterToken {
            count: self.current.clone(),
        })
    }
}

/// A token dispensed by a [BoundedCounter]. Decrease the counter when dropped.
#[derive(Debug)]
pub struct CounterToken {
    count: Arc<AtomicUsize>,
}

impl Drop for CounterToken {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(0)]
    #[test_case(1)]
    #[test_case(20)]
    fn test_increment(max: usize) {
        let counter = BoundedCounter::new(max);
        let mut tokens = Vec::new();
        for _ in 0..max {
            let token = counter.increment().expect("should succeed before reaching the limit");
            tokens.push(token);
        }
        let current = counter.increment().expect_err("should fail after reaching the limit");
        assert_eq!(current, max);
    }

    #[test]
    fn test_drop() {
        let counter = BoundedCounter::new(1);

        let token = counter.increment().expect("should succeed once");
        let current = counter.increment().expect_err("should fail while token is alive");
        assert_eq!(current, 1);
        drop(token);

        _ = counter.increment().expect("should succeed after dropping token");
    }
}
