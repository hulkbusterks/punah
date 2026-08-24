#![forbid(unsafe_code)]

use std::cmp::Ordering;

use punah_metadata::SegmentRef;
use punah_types::SegmentStats;

/// One whole reclaim ratio expressed in basis points.
pub const FULL_RECLAIM_RATIO_BPS: u16 = 10_000;

/// A segment visible to the compaction policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentCandidate {
    pub segment: SegmentRef,
    pub stats: SegmentStats,
    /// Sealing makes the segment's byte range immutable.
    pub sealed: bool,
    /// The active append target must never be compacted.
    pub active: bool,
}

/// Resource limits applied while choosing a compaction batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanningLimits {
    /// Currently unused capacity in the storage arena.
    pub available_bytes: u64,
    /// Capacity that compaction is forbidden to consume.
    pub emergency_reserve_bytes: u64,
    /// Maximum sum of source segment sizes in one batch.
    pub max_input_bytes: u64,
    /// Extra capacity retained above the estimated live output.
    pub output_safety_margin_bytes: u64,
    /// Minimum dead/expired share required for each selected segment.
    /// `0` permits any positive reclaim; `10_000` selects only all-dead input.
    pub minimum_reclaim_ratio_bps: u16,
    /// Minimum number of selected inputs when live records require a
    /// replacement. A single all-dead input may always be retired directly.
    pub minimum_input_segments: usize,
}

/// A deterministic, I/O-free compaction decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPlan {
    pub input_segments: Vec<SegmentRef>,
    pub input_bytes: u64,
    pub reclaimable_bytes: u64,
    /// Upper bound derived from the input segments' reported live bytes.
    pub required_output_bytes: u64,
    /// Output plus the configured safety margin.
    pub required_headroom_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanningError {
    NoEligibleSegments,
    InvalidMinimumReclaimRatio(u16),
    InvalidMinimumInputSegments,
    NoCompactionHeadroom {
        required_bytes: u64,
        usable_bytes: u64,
    },
    ArithmeticOverflow,
}

/// Selects a bounded compaction batch without inspecting or modifying storage.
///
/// Candidates are ordered by reclaimable ratio, then reclaimable bytes, then
/// stable segment identity. Invalid statistics are treated conservatively:
/// output is bounded by `max(live_bytes, total_bytes - reclaimable_bytes)`.
///
/// # Errors
/// Returns an explicit policy error when the minimum ratio is invalid, no
/// segment is eligible, capacity above the emergency reserve is insufficient,
/// or checked byte accounting overflows.
pub fn plan_compaction(
    candidates: &[SegmentCandidate],
    limits: PlanningLimits,
) -> Result<CompactionPlan, PlanningError> {
    if limits.minimum_reclaim_ratio_bps > FULL_RECLAIM_RATIO_BPS {
        return Err(PlanningError::InvalidMinimumReclaimRatio(
            limits.minimum_reclaim_ratio_bps,
        ));
    }
    if limits.minimum_input_segments == 0 {
        return Err(PlanningError::InvalidMinimumInputSegments);
    }
    let usable_bytes = limits
        .available_bytes
        .checked_sub(limits.emergency_reserve_bytes)
        .ok_or(PlanningError::NoCompactionHeadroom {
            required_bytes: limits.output_safety_margin_bytes,
            usable_bytes: 0,
        })?;

    let mut eligible = Vec::new();
    for candidate in candidates.iter().copied() {
        if !candidate.sealed || candidate.active {
            continue;
        }
        let reclaimable = candidate
            .stats
            .dead_bytes
            .checked_add(candidate.stats.expired_bytes)
            .ok_or(PlanningError::ArithmeticOverflow)?;
        if candidate.stats.total_bytes > 0
            && reclaimable > 0
            && candidate.stats.total_bytes <= limits.max_input_bytes
            && meets_minimum_reclaim_ratio(
                reclaimable,
                candidate.stats.total_bytes,
                limits.minimum_reclaim_ratio_bps,
            )
        {
            eligible.push(candidate);
        }
    }
    if eligible.is_empty() {
        return Err(PlanningError::NoEligibleSegments);
    }
    eligible.sort_by(compare_candidates);

    let mut plan = CompactionPlan {
        input_segments: Vec::new(),
        input_bytes: 0,
        reclaimable_bytes: 0,
        required_output_bytes: 0,
        required_headroom_bytes: 0,
    };
    for candidate in eligible {
        let Some(next_input) = plan.input_bytes.checked_add(candidate.stats.total_bytes) else {
            return Err(PlanningError::ArithmeticOverflow);
        };
        if next_input > limits.max_input_bytes {
            continue;
        }
        let reclaimable = candidate.stats.reclaimable_bytes();
        let inferred_live = candidate.stats.total_bytes.saturating_sub(reclaimable);
        let conservative_live = candidate.stats.live_bytes.max(inferred_live);
        plan.input_bytes = next_input;
        plan.reclaimable_bytes = plan
            .reclaimable_bytes
            .checked_add(reclaimable)
            .ok_or(PlanningError::ArithmeticOverflow)?;
        plan.required_output_bytes = plan
            .required_output_bytes
            .checked_add(conservative_live)
            .ok_or(PlanningError::ArithmeticOverflow)?;
        plan.input_segments.push(candidate.segment);
    }
    if plan.input_segments.is_empty() {
        return Err(PlanningError::NoEligibleSegments);
    }
    if plan.required_output_bytes > 0 && plan.input_segments.len() < limits.minimum_input_segments {
        return Err(PlanningError::NoEligibleSegments);
    }
    plan.required_headroom_bytes = plan
        .required_output_bytes
        .checked_add(limits.output_safety_margin_bytes)
        .ok_or(PlanningError::ArithmeticOverflow)?;
    if plan.required_headroom_bytes > usable_bytes {
        return Err(PlanningError::NoCompactionHeadroom {
            required_bytes: plan.required_headroom_bytes,
            usable_bytes,
        });
    }
    Ok(plan)
}

fn meets_minimum_reclaim_ratio(reclaimable: u64, total: u64, minimum_bps: u16) -> bool {
    u128::from(reclaimable) * u128::from(FULL_RECLAIM_RATIO_BPS)
        >= u128::from(total) * u128::from(minimum_bps)
}

fn compare_candidates(left: &SegmentCandidate, right: &SegmentCandidate) -> Ordering {
    let left_reclaimable = left.stats.reclaimable_bytes();
    let right_reclaimable = right.stats.reclaimable_bytes();
    // Cross multiplication compares ratios exactly and cannot overflow u128.
    let ratio_order = (u128::from(right_reclaimable) * u128::from(left.stats.total_bytes))
        .cmp(&(u128::from(left_reclaimable) * u128::from(right.stats.total_bytes)));
    ratio_order
        .then_with(|| right_reclaimable.cmp(&left_reclaimable))
        .then_with(|| left.segment.id.cmp(&right.segment.id))
        .then_with(|| left.segment.generation.cmp(&right.segment.generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: u32, total: u64, live: u64, dead: u64) -> SegmentCandidate {
        SegmentCandidate {
            segment: SegmentRef { id, generation: 1 },
            stats: SegmentStats {
                total_bytes: total,
                live_bytes: live,
                dead_bytes: dead,
                expired_bytes: 0,
            },
            sealed: true,
            active: false,
        }
    }

    fn limits() -> PlanningLimits {
        PlanningLimits {
            available_bytes: 1_000,
            emergency_reserve_bytes: 100,
            max_input_bytes: 1_000,
            output_safety_margin_bytes: 10,
            minimum_reclaim_ratio_bps: 0,
            minimum_input_segments: 1,
        }
    }

    #[test]
    fn rejects_empty_and_non_reclaimable_inputs() {
        assert_eq!(
            plan_compaction(&[], limits()),
            Err(PlanningError::NoEligibleSegments)
        );
        assert_eq!(
            plan_compaction(&[candidate(1, 10, 10, 0)], limits()),
            Err(PlanningError::NoEligibleSegments)
        );
    }

    #[test]
    fn minimum_reclaim_ratio_is_per_segment_and_checks_exact_boundary() {
        let mut value = limits();
        value.minimum_reclaim_ratio_bps = 5_000;
        let plan = plan_compaction(
            &[
                candidate(1, 100, 51, 49),
                candidate(2, 100, 50, 50),
                candidate(3, 100, 25, 75),
            ],
            value,
        )
        .unwrap();
        assert_eq!(
            plan.input_segments.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![3, 2]
        );
    }

    #[test]
    fn rejects_an_invalid_minimum_reclaim_ratio() {
        let mut value = limits();
        value.minimum_reclaim_ratio_bps = FULL_RECLAIM_RATIO_BPS + 1;
        assert_eq!(
            plan_compaction(&[candidate(1, 100, 0, 100)], value),
            Err(PlanningError::InvalidMinimumReclaimRatio(10_001))
        );
    }

    #[test]
    fn replacement_requires_minimum_inputs_but_all_dead_can_retire_alone() {
        let mut value = limits();
        value.minimum_input_segments = 2;
        assert_eq!(
            plan_compaction(&[candidate(1, 100, 50, 50)], value),
            Err(PlanningError::NoEligibleSegments)
        );
        assert_eq!(
            plan_compaction(&[candidate(1, 100, 0, 100)], value)
                .unwrap()
                .input_segments
                .len(),
            1
        );
        assert_eq!(
            plan_compaction(
                &[candidate(1, 100, 50, 50), candidate(2, 100, 50, 50)],
                value,
            )
            .unwrap()
            .input_segments
            .len(),
            2
        );
    }

    #[test]
    fn rejects_zero_minimum_input_segments() {
        let mut value = limits();
        value.minimum_input_segments = 0;
        assert_eq!(
            plan_compaction(&[candidate(1, 100, 0, 100)], value),
            Err(PlanningError::InvalidMinimumInputSegments)
        );
    }

    #[test]
    fn never_selects_active_or_unsealed_segments() {
        let mut active = candidate(1, 100, 1, 99);
        active.active = true;
        let mut unsealed = candidate(2, 100, 1, 99);
        unsealed.sealed = false;
        let selected = candidate(3, 100, 50, 50);
        let plan = plan_compaction(&[active, unsealed, selected], limits()).unwrap();
        assert_eq!(plan.input_segments, vec![selected.segment]);
    }

    #[test]
    fn orders_by_ratio_then_bytes_then_identity() {
        let plan = plan_compaction(
            &[
                candidate(4, 100, 50, 50),
                candidate(3, 200, 100, 100),
                candidate(2, 100, 10, 90),
            ],
            limits(),
        )
        .unwrap();
        assert_eq!(
            plan.input_segments.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn respects_maximum_input_and_skips_items_that_do_not_fit() {
        let mut value = limits();
        value.max_input_bytes = 150;
        let plan = plan_compaction(
            &[
                candidate(1, 100, 10, 90),
                candidate(2, 60, 20, 40),
                candidate(3, 50, 25, 25),
            ],
            value,
        )
        .unwrap();
        assert_eq!(
            plan.input_segments.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(plan.input_bytes, 150);
    }

    #[test]
    fn reserve_is_never_counted_as_usable_headroom() {
        let mut value = limits();
        value.available_bytes = 100;
        value.emergency_reserve_bytes = 50;
        value.output_safety_margin_bytes = 1;
        assert_eq!(
            plan_compaction(&[candidate(1, 100, 50, 50)], value),
            Err(PlanningError::NoCompactionHeadroom {
                required_bytes: 51,
                usable_bytes: 50
            })
        );
    }

    #[test]
    fn exact_headroom_boundary_succeeds() {
        let mut value = limits();
        value.available_bytes = 61;
        value.emergency_reserve_bytes = 1;
        let plan = plan_compaction(&[candidate(1, 100, 50, 50)], value).unwrap();
        assert_eq!(plan.required_output_bytes, 50);
        assert_eq!(plan.required_headroom_bytes, 60);
    }

    #[test]
    fn inconsistent_stats_use_conservative_live_estimate() {
        let plan = plan_compaction(&[candidate(1, 100, 90, 50)], limits()).unwrap();
        assert_eq!(plan.required_output_bytes, 90);
    }

    #[test]
    fn reports_arithmetic_overflow() {
        let mut value = limits();
        value.available_bytes = u64::MAX;
        value.max_input_bytes = u64::MAX;
        value.output_safety_margin_bytes = u64::MAX;
        assert_eq!(
            plan_compaction(&[candidate(1, 1, 1, 1)], value),
            Err(PlanningError::ArithmeticOverflow)
        );

        let mut broken = candidate(2, u64::MAX, 0, u64::MAX);
        broken.stats.expired_bytes = 1;
        assert_eq!(
            plan_compaction(&[broken], limits()),
            Err(PlanningError::ArithmeticOverflow)
        );
    }

    #[test]
    fn selection_is_permutation_invariant() {
        let original = [
            candidate(9, 100, 20, 80),
            candidate(2, 80, 40, 40),
            candidate(4, 60, 30, 30),
        ];
        let permutations = [
            [original[0], original[1], original[2]],
            [original[2], original[0], original[1]],
            [original[1], original[2], original[0]],
        ];
        let expected = plan_compaction(&permutations[0], limits()).unwrap();
        for permutation in &permutations[1..] {
            assert_eq!(plan_compaction(permutation, limits()).unwrap(), expected);
        }
    }
}
