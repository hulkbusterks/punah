#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use punah_compaction::{PlanningError, PlanningLimits};
use punah_store::Store;

const DEFAULT_MINIMUM_RECLAIM_RATIO_BPS: u16 = 7_500;
const DEFAULT_MINIMUM_INPUT_SEGMENTS: usize = 2;

/// Opt-in policy for shard-local background compaction.
///
/// A cycle is considered only after `check_every_mutations` accepted mutations
/// and only while the owning shard's ingress is observed empty. Input and
/// temporary-output bytes are hard planning bounds. `pause_budget` is an
/// observable target: the current synchronous compactor finishes an accepted
/// plan rather than interrupting publication halfway through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackgroundCompaction {
    Disabled,
    Enabled {
        check_every_mutations: u64,
        max_input_bytes: u64,
        temporary_output_budget_bytes: u64,
        emergency_reserve_bytes: u64,
        output_safety_margin_bytes: u64,
        minimum_reclaim_ratio_bps: u16,
        minimum_input_segments: usize,
        pause_budget: Duration,
    },
}

impl BackgroundCompaction {
    /// Builds an enabled policy with no reserve or output safety margin and a
    /// 75% minimum reclaim ratio.
    #[must_use]
    pub const fn bounded(
        check_every_mutations: u64,
        max_input_bytes: u64,
        temporary_output_budget_bytes: u64,
        pause_budget: Duration,
    ) -> Self {
        Self::bounded_with_policy(
            check_every_mutations,
            max_input_bytes,
            temporary_output_budget_bytes,
            DEFAULT_MINIMUM_RECLAIM_RATIO_BPS,
            DEFAULT_MINIMUM_INPUT_SEGMENTS,
            pause_budget,
        )
    }

    /// Builds an enabled policy with an explicit minimum reclaim yield.
    #[must_use]
    pub const fn bounded_with_minimum_reclaim_ratio(
        check_every_mutations: u64,
        max_input_bytes: u64,
        temporary_output_budget_bytes: u64,
        minimum_reclaim_ratio_bps: u16,
        pause_budget: Duration,
    ) -> Self {
        Self::bounded_with_policy(
            check_every_mutations,
            max_input_bytes,
            temporary_output_budget_bytes,
            minimum_reclaim_ratio_bps,
            DEFAULT_MINIMUM_INPUT_SEGMENTS,
            pause_budget,
        )
    }

    /// Builds an enabled policy with explicit reclaim and aggregation limits.
    #[must_use]
    pub const fn bounded_with_policy(
        check_every_mutations: u64,
        max_input_bytes: u64,
        temporary_output_budget_bytes: u64,
        minimum_reclaim_ratio_bps: u16,
        minimum_input_segments: usize,
        pause_budget: Duration,
    ) -> Self {
        Self::Enabled {
            check_every_mutations,
            max_input_bytes,
            temporary_output_budget_bytes,
            emergency_reserve_bytes: 0,
            output_safety_margin_bytes: 0,
            minimum_reclaim_ratio_bps,
            minimum_input_segments,
            pause_budget,
        }
    }

    pub(crate) const fn is_valid(self) -> bool {
        match self {
            Self::Disabled => true,
            Self::Enabled {
                check_every_mutations,
                max_input_bytes,
                temporary_output_budget_bytes,
                emergency_reserve_bytes,
                output_safety_margin_bytes,
                minimum_reclaim_ratio_bps,
                minimum_input_segments,
                pause_budget,
            } => {
                check_every_mutations > 0
                    && max_input_bytes > 0
                    && temporary_output_budget_bytes > 0
                    && emergency_reserve_bytes <= temporary_output_budget_bytes
                    && output_safety_margin_bytes <= temporary_output_budget_bytes
                    && minimum_reclaim_ratio_bps <= punah_compaction::FULL_RECLAIM_RATIO_BPS
                    && minimum_input_segments > 0
                    && !pause_budget.is_zero()
            }
        }
    }
}

/// Monotonic observability snapshot for one shard's maintenance work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceSnapshot {
    pub shard: usize,
    pub checks: u64,
    pub compactions: u64,
    pub input_bytes: u64,
    pub copied_bytes: u64,
    pub max_input_bytes: u64,
    pub max_copied_bytes: u64,
    pub reclaimable_bytes: u64,
    pub planning_failures: u64,
    pub compaction_failures: u64,
    pub pause_budget_exceeded: u64,
    pub last_pause_ns: u64,
    pub max_pause_ns: u64,
}

#[derive(Default)]
pub(crate) struct MaintenanceCounters {
    checks: AtomicU64,
    compactions: AtomicU64,
    input_bytes: AtomicU64,
    copied_bytes: AtomicU64,
    max_input_bytes: AtomicU64,
    max_copied_bytes: AtomicU64,
    reclaimable_bytes: AtomicU64,
    planning_failures: AtomicU64,
    compaction_failures: AtomicU64,
    pause_budget_exceeded: AtomicU64,
    last_pause_ns: AtomicU64,
    max_pause_ns: AtomicU64,
}

impl MaintenanceCounters {
    pub(crate) fn snapshot(&self, shard: usize) -> MaintenanceSnapshot {
        // A completed-compaction increment is the publication fence for that
        // cycle's byte and pause counters.
        let compactions = self.compactions.load(Ordering::Acquire);
        MaintenanceSnapshot {
            shard,
            checks: self.checks.load(Ordering::Relaxed),
            compactions,
            input_bytes: self.input_bytes.load(Ordering::Relaxed),
            copied_bytes: self.copied_bytes.load(Ordering::Relaxed),
            max_input_bytes: self.max_input_bytes.load(Ordering::Relaxed),
            max_copied_bytes: self.max_copied_bytes.load(Ordering::Relaxed),
            reclaimable_bytes: self.reclaimable_bytes.load(Ordering::Relaxed),
            planning_failures: self.planning_failures.load(Ordering::Relaxed),
            compaction_failures: self.compaction_failures.load(Ordering::Relaxed),
            pause_budget_exceeded: self.pause_budget_exceeded.load(Ordering::Relaxed),
            last_pause_ns: self.last_pause_ns.load(Ordering::Relaxed),
            max_pause_ns: self.max_pause_ns.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct MaintenanceScheduler {
    policy: BackgroundCompaction,
    mutations_since_check: u64,
    counters: Arc<MaintenanceCounters>,
}

impl MaintenanceScheduler {
    pub(crate) fn new(policy: BackgroundCompaction, counters: Arc<MaintenanceCounters>) -> Self {
        Self {
            policy,
            mutations_since_check: 0,
            counters,
        }
    }

    pub(crate) fn record_mutations(&mut self, count: u64) {
        self.mutations_since_check = self.mutations_since_check.saturating_add(count);
    }

    pub(crate) fn is_due(&self) -> bool {
        match self.policy {
            BackgroundCompaction::Disabled => false,
            BackgroundCompaction::Enabled {
                check_every_mutations,
                ..
            } => self.mutations_since_check >= check_every_mutations,
        }
    }

    pub(crate) fn run_once(&mut self, store: &mut Store) {
        let BackgroundCompaction::Enabled {
            max_input_bytes,
            temporary_output_budget_bytes,
            emergency_reserve_bytes,
            output_safety_margin_bytes,
            minimum_reclaim_ratio_bps,
            minimum_input_segments,
            pause_budget,
            ..
        } = self.policy
        else {
            return;
        };
        self.mutations_since_check = 0;
        self.counters.checks.fetch_add(1, Ordering::Relaxed);
        let plan = match store.plan_compaction(PlanningLimits {
            available_bytes: temporary_output_budget_bytes,
            emergency_reserve_bytes,
            max_input_bytes,
            output_safety_margin_bytes,
            minimum_reclaim_ratio_bps,
            minimum_input_segments,
        }) {
            Ok(plan) => plan,
            Err(PlanningError::NoEligibleSegments) => return,
            Err(
                PlanningError::NoCompactionHeadroom { .. }
                | PlanningError::ArithmeticOverflow
                | PlanningError::InvalidMinimumReclaimRatio(_)
                | PlanningError::InvalidMinimumInputSegments,
            ) => {
                self.counters
                    .planning_failures
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        let started = Instant::now();
        let result = store.compact(&plan);
        let elapsed = started.elapsed();
        let elapsed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.counters
            .last_pause_ns
            .store(elapsed_ns, Ordering::Relaxed);
        self.counters
            .max_pause_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
        if elapsed > pause_budget {
            self.counters
                .pause_budget_exceeded
                .fetch_add(1, Ordering::Relaxed);
        }
        match result {
            Ok(report) => {
                self.counters
                    .input_bytes
                    .fetch_add(plan.input_bytes, Ordering::Relaxed);
                self.counters
                    .copied_bytes
                    .fetch_add(report.copied_bytes, Ordering::Relaxed);
                self.counters
                    .max_input_bytes
                    .fetch_max(plan.input_bytes, Ordering::Relaxed);
                self.counters
                    .max_copied_bytes
                    .fetch_max(report.copied_bytes, Ordering::Relaxed);
                self.counters
                    .reclaimable_bytes
                    .fetch_add(plan.reclaimable_bytes, Ordering::Relaxed);
                self.counters.compactions.fetch_add(1, Ordering::Release);
            }
            Err(_) => {
                self.counters
                    .compaction_failures
                    .fetch_add(1, Ordering::Release);
            }
        }
    }
}
