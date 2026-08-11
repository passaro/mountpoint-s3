//! Mapping [`Urgency`] onto the transfer manager's `u8` priority scale.
//!
//! Two levels, for the two things worth distinguishing: a fetch something is blocked on, and a
//! fetch nobody has asked for yet. Both are derived inside the data plane, so this table is the
//! whole of the priority policy.
//!
//! The `u8` stays internal because the scale is specific to this transfer layer — 1..=255,
//! default 128, weighted so the extremes differ in share by roughly 256x. A caller passing raw
//! priorities would be encoding those semantics into code that should not depend on them.
//!
//! The benchmark treats this table as configuration, so a measurement can compare policies.

use crate::data::Urgency;

/// Priority assigned per urgency.
///
/// The gap between the two is what lets a blocked read outrank speculation, including
/// speculation belonging to a different reader sharing the same transfer client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriorityTable {
    /// A `read_at` is blocked on these bytes.
    pub demand: u8,
    /// Read-ahead beyond what anyone has asked for.
    pub speculative: u8,
}

impl Default for PriorityTable {
    /// A moderate spread around the RTM's default of 128.
    ///
    /// Deliberately not the extremes: 1 versus 255 is close enough to strict priority to hide
    /// whether weighted sharing is doing anything. This is the setting to run in; the extremes
    /// are available for comparison.
    fn default() -> Self {
        Self {
            demand: 192,
            speculative: 64,
        }
    }
}

impl PriorityTable {
    /// Both levels at the default, i.e. priority disabled. A control: an effect that survives
    /// this table is not caused by priority.
    pub fn flat() -> Self {
        Self {
            demand: 128,
            speculative: 128,
        }
    }

    /// The widest spread the scale allows, for bounding the achievable effect.
    pub fn extreme() -> Self {
        Self {
            demand: 255,
            speculative: 1,
        }
    }

    pub fn priority_for(&self, urgency: Urgency) -> u8 {
        match urgency {
            Urgency::Demand => self.demand,
            Urgency::Speculative => self.speculative,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demand_outranks_speculative() {
        let t = PriorityTable::default();
        assert!(t.priority_for(Urgency::Demand) > t.priority_for(Urgency::Speculative));
    }

    #[test]
    fn flat_table_is_uniform() {
        let t = PriorityTable::flat();
        assert_eq!(t.priority_for(Urgency::Demand), 128);
        assert_eq!(t.priority_for(Urgency::Speculative), 128);
    }

    #[test]
    fn all_priorities_are_valid_rtm_values() {
        // The RTM's range is 1..=255; 0 is not a priority.
        for t in [
            PriorityTable::default(),
            PriorityTable::flat(),
            PriorityTable::extreme(),
        ] {
            assert!(t.priority_for(Urgency::Demand) >= 1);
            assert!(t.priority_for(Urgency::Speculative) >= 1);
        }
    }
}
