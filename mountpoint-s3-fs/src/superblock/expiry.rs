use std::time::{Duration, Instant};

use thiserror::Error;

use crate::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy)]
pub struct Expiry(ShortDuration);

impl Expiry {
    pub const EXPIRED: Self = Self(ShortDuration::ZERO);
    pub const NEVER: Self = Self(ShortDuration::MAX);
}

#[derive(Debug)]
pub struct AtomicExpiry(AtomicU64);

impl AtomicExpiry {
    pub fn new(value: Expiry) -> Self {
        Self(AtomicU64::new(value.0.nanos))
    }

    pub fn load(&self, order: Ordering) -> Expiry {
        let value = self.0.load(order);
        Expiry(ShortDuration { nanos: value })
    }

    pub fn store(&self, value: Expiry, order: Ordering) {
        let val = value.0.nanos;
        self.0.store(val, order);
    }
}

#[derive(Debug, Clone)]
pub struct Timeline(Instant);

impl Timeline {
    pub fn new_from_instant(instant: Instant) -> Self {
        Self(instant)
    }

    pub fn new_from_now() -> Self {
        Self::new_from_instant(Instant::now())
    }

    pub fn expiry_instant(&self, expiry: Expiry) -> Instant {
        self.0 + expiry.0.to_duration()
    }

    pub fn is_valid(&self, expiry: Expiry) -> bool {
        expiry.0.to_duration() > self.0.elapsed()
    }

    /// Create a new [Expiry] with the given TTL starting from now.
    pub fn expiry_from_now(&self, duration: ShortDuration) -> Expiry {
        let now = self.0.elapsed();
        let duration = now.saturating_add(duration.into());
        let short = ShortDuration::saturating_from_duration(duration);
        Expiry(short)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShortDuration {
    nanos: u64,
}

#[derive(Debug, Error)]
pub enum ShortDurationError {
    #[error("Duration is too long: {0:?}")]
    TooLong(Duration),
}

const NANOS_PER_MILLI: u64 = 1_000_000;
const MILLIS_PER_SEC: u64 = 1_000;
const NANOS_PER_SEC: u64 = NANOS_PER_MILLI * MILLIS_PER_SEC;
const MAX_SECONDS: u64 = 200 * 365 * 24 * 60 * 60;
const MAX_NANOS: u64 = MAX_SECONDS * NANOS_PER_SEC;
const MAX_DURATION: Duration = Duration::from_secs(MAX_SECONDS);

impl ShortDuration {
    pub const ZERO: Self = Self { nanos: 0 };
    pub const MAX: Self = Self { nanos: MAX_NANOS };

    pub fn saturating_from_duration(duration: Duration) -> Self {
        duration.try_into().unwrap_or(Self::MAX)
    }

    pub fn to_duration(self) -> Duration {
        Duration::from_nanos(self.nanos)
    }

    pub fn as_secs(&self) -> u64 {
        self.nanos / NANOS_PER_SEC
    }

    pub const fn from_millis(millis: u64) -> ShortDuration {
        let nanos = millis * NANOS_PER_MILLI;
        assert!(nanos < MAX_NANOS);
        Self { nanos }
    }

    pub const fn from_secs(secs: u64) -> ShortDuration {
        let nanos = secs * NANOS_PER_SEC;
        assert!(nanos < MAX_NANOS);
        Self { nanos }
    }
}

impl TryFrom<Duration> for ShortDuration {
    type Error = ShortDurationError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        if value > MAX_DURATION {
            return Err(ShortDurationError::TooLong(value));
        }
        let nanos = value.as_secs() * NANOS_PER_SEC + value.subsec_nanos() as u64;
        Ok(ShortDuration { nanos })
    }
}

impl From<ShortDuration> for Duration {
    fn from(value: ShortDuration) -> Self {
        value.to_duration()
    }
}
