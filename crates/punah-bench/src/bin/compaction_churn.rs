#![forbid(unsafe_code)]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use punah_runtime::{BackgroundCompaction, MaintenanceSnapshot, Runtime, RuntimeConfig};
use punah_store::StoreIoTelemetry;
use punah_types::{Durability, Namespace};

const CSV_HEADER: &str = "strategy,operations,value_bytes,key_space,persistent_every,segment_bytes,minimum_reclaim_ratio_bps,minimum_input_segments,pause_budget_ns,trial,elapsed_ns,ops_per_second,set_p50_ns,set_p95_ns,set_p99_ns,set_p999_ns,foreground_encoded_bytes,logical_write_amplification,checks,compactions,maintenance_steps,copy_slices,output_preparations,input_bytes,copied_bytes,reclaimable_bytes,actual_reclaim_ratio_bps,max_input_bytes,max_copied_bytes,planning_failures,compaction_failures,pause_budget_exceeded,max_pause_ns,retained_segment_files,retained_segment_bytes,retained_allocated_bytes,recovery_ns";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn main() -> Result<(), String> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let config = Config::parse(&arguments)?;
    let mut rows = Vec::new();
    for trial in 1..=config.trials {
        let strategies = if trial % 2 == 0 {
            [Strategy::Background, Strategy::Disabled]
        } else {
            [Strategy::Disabled, Strategy::Background]
        };
        for strategy in strategies {
            rows.push(run_case(&config, strategy, trial)?);
        }
    }
    rows.sort_by_key(|row| (row.trial, row.strategy));
    println!("{CSV_HEADER}");
    for row in rows {
        println!("{}", row.csv_line());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    operations: usize,
    value_bytes: usize,
    key_space: usize,
    segment_bytes: u64,
    trials: usize,
    check_every_mutations: u64,
    max_input_bytes: u64,
    minimum_reclaim_ratio_bps: u16,
    pause_budget_ms: u64,
    persistent_every: usize,
    minimum_input_segments: usize,
}

impl Config {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        if !(4..=11).contains(&arguments.len()) {
            return Err(usage());
        }
        let operations = parse_usize(&arguments[0], "OPERATIONS")?;
        u32::try_from(operations).map_err(|_| "OPERATIONS cannot exceed u32::MAX".to_owned())?;
        let value_bytes = parse_usize(&arguments[1], "VALUE_BYTES")?;
        let key_space = parse_usize(&arguments[2], "KEY_SPACE")?;
        let segment_bytes = parse_u64(&arguments[3], "SEGMENT_BYTES")?;
        let trials = arguments
            .get(4)
            .map_or(Ok(5), |value| parse_usize(value, "TRIALS"))?;
        let check_every_mutations = arguments
            .get(5)
            .map_or(Ok(64), |value| parse_u64(value, "CHECK_EVERY_MUTATIONS"))?;
        let default_max_input_bytes = segment_bytes
            .checked_mul(2)
            .ok_or_else(|| "default MAX_INPUT_BYTES overflow".to_owned())?;
        let max_input_bytes = arguments
            .get(6)
            .map_or(Ok(default_max_input_bytes), |value| {
                parse_u64(value, "MAX_INPUT_BYTES")
            })?;
        let minimum_reclaim_ratio_bps = arguments
            .get(7)
            .map_or(Ok(7_500), |value| parse_reclaim_ratio(value))?;
        let pause_budget_ms = arguments
            .get(8)
            .map_or(Ok(25), |value| parse_u64(value, "PAUSE_BUDGET_MS"))?;
        let persistent_every = arguments.get(9).map_or(Ok(0), |value| {
            parse_usize_allow_zero(value, "PERSISTENT_EVERY")
        })?;
        let minimum_input_segments = arguments
            .get(10)
            .map_or(Ok(2), |value| parse_usize(value, "MINIMUM_INPUT_SEGMENTS"))?;
        let minimum_segment = u64::try_from(value_bytes)
            .map_err(|_| "VALUE_BYTES is not representable".to_owned())?
            .saturating_add(512);
        if segment_bytes < minimum_segment {
            return Err("SEGMENT_BYTES must exceed VALUE_BYTES by at least 512".to_owned());
        }
        Ok(Self {
            operations,
            value_bytes,
            key_space,
            segment_bytes,
            trials,
            check_every_mutations,
            max_input_bytes,
            minimum_reclaim_ratio_bps,
            pause_budget_ms,
            persistent_every,
            minimum_input_segments,
        })
    }
}

fn parse_usize(value: &str, name: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if value == 0 {
        return Err(format!("{name} must be nonzero"));
    }
    Ok(value)
}

fn parse_u64(value: &str, name: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if value == 0 {
        return Err(format!("{name} must be nonzero"));
    }
    Ok(value)
}

fn parse_usize_allow_zero(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn parse_reclaim_ratio(value: &str) -> Result<u16, String> {
    let value = value
        .parse::<u16>()
        .map_err(|error| format!("invalid MINIMUM_RECLAIM_RATIO_BPS: {error}"))?;
    if value > punah_compaction::FULL_RECLAIM_RATIO_BPS {
        return Err("MINIMUM_RECLAIM_RATIO_BPS cannot exceed 10000".to_owned());
    }
    Ok(value)
}

fn usage() -> String {
    "usage: compaction_churn OPERATIONS VALUE_BYTES KEY_SPACE SEGMENT_BYTES [TRIALS=5] [CHECK_EVERY_MUTATIONS=64] [MAX_INPUT_BYTES=2*SEGMENT_BYTES] [MINIMUM_RECLAIM_RATIO_BPS=7500] [PAUSE_BUDGET_MS=25] [PERSISTENT_EVERY=0] [MINIMUM_INPUT_SEGMENTS=2]".to_owned()
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Strategy {
    Disabled,
    Background,
}

impl Strategy {
    const fn name(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Background => "background",
        }
    }
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new(strategy: Strategy, trial: usize) -> Result<Self, String> {
        let nonce = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "punah-compaction-churn-{}-{}-{trial}-{nonce}",
            std::process::id(),
            strategy.name()
        ));
        fs::create_dir(&directory).map_err(|error| error.to_string())?;
        Ok(Self(directory))
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
struct Row {
    strategy: Strategy,
    operations: usize,
    value_bytes: usize,
    key_space: usize,
    persistent_every: usize,
    minimum_input_segments: usize,
    segment_bytes: u64,
    minimum_reclaim_ratio_bps: u16,
    pause_budget_ms: u64,
    trial: usize,
    elapsed_ns: u128,
    operations_per_second: f64,
    percentiles: [u128; 4],
    maintenance: MaintenanceSnapshot,
    storage: StoreIoTelemetry,
    retained_segment_files: usize,
    retained_segment_bytes: u64,
    retained_allocated_bytes: u64,
    recovery_ns: u128,
}

impl Row {
    fn csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{:.2},{},{},{},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.strategy.name(),
            self.operations,
            self.value_bytes,
            self.key_space,
            self.persistent_every,
            self.segment_bytes,
            self.minimum_reclaim_ratio_bps(),
            self.minimum_input_segments,
            self.pause_budget_ns(),
            self.trial,
            self.elapsed_ns,
            self.operations_per_second,
            self.percentiles[0],
            self.percentiles[1],
            self.percentiles[2],
            self.percentiles[3],
            self.storage.encoded_bytes,
            self.logical_write_amplification(),
            self.maintenance.checks,
            self.maintenance.compactions,
            self.maintenance.steps,
            self.maintenance.copy_slices,
            self.maintenance.output_preparations,
            self.maintenance.input_bytes,
            self.maintenance.copied_bytes,
            self.maintenance.reclaimable_bytes,
            self.actual_reclaim_ratio_bps(),
            self.maintenance.max_input_bytes,
            self.maintenance.max_copied_bytes,
            self.maintenance.planning_failures,
            self.maintenance.compaction_failures,
            self.maintenance.pause_budget_exceeded,
            self.maintenance.max_pause_ns,
            self.retained_segment_files,
            self.retained_segment_bytes,
            self.retained_allocated_bytes,
            self.recovery_ns,
        )
    }

    const fn minimum_reclaim_ratio_bps(&self) -> u16 {
        self.minimum_reclaim_ratio_bps
    }

    const fn pause_budget_ns(&self) -> u64 {
        self.pause_budget_ms.saturating_mul(1_000_000)
    }

    #[allow(clippy::cast_precision_loss)]
    fn logical_write_amplification(&self) -> f64 {
        if self.storage.encoded_bytes == 0 {
            return 0.0;
        }
        (self.storage.encoded_bytes as f64 + self.maintenance.copied_bytes as f64)
            / self.storage.encoded_bytes as f64
    }

    fn actual_reclaim_ratio_bps(&self) -> u64 {
        self.maintenance
            .reclaimable_bytes
            .saturating_mul(10_000)
            .checked_div(self.maintenance.input_bytes)
            .unwrap_or(0)
    }
}

fn run_case(config: &Config, strategy: Strategy, trial: usize) -> Result<Row, String> {
    let directory = TemporaryDirectory::new(strategy, trial)?;
    let runtime_config = RuntimeConfig {
        directory: directory.0.clone(),
        namespace: Namespace::new("benchmark").map_err(|error| format!("{error:?}"))?,
        shard_count: 1,
        queue_capacity: 64,
        segment_capacity: config.segment_bytes,
        durability: Durability::LocalDurable,
    };
    let policy = match strategy {
        Strategy::Disabled => BackgroundCompaction::Disabled,
        Strategy::Background => BackgroundCompaction::bounded_with_policy(
            config.check_every_mutations,
            config.max_input_bytes,
            config.segment_bytes,
            config.minimum_reclaim_ratio_bps,
            config.minimum_input_segments,
            Duration::from_millis(config.pause_budget_ms),
        ),
    };
    let runtime = Runtime::open_with_compaction(&runtime_config, policy)
        .map_err(|error| format!("runtime open: {error:?}"))?;
    let handle = runtime.handle();
    let value = vec![0x5a; config.value_bytes];
    let mut latencies = Vec::with_capacity(config.operations);
    let workload_started = Instant::now();
    for operation in 0..config.operations {
        let key = workload_key(config, operation);
        let started = Instant::now();
        handle
            .set(key.as_bytes(), value.clone(), None)
            .map_err(|error| format!("set {operation}: {error:?}"))?;
        latencies.push(started.elapsed().as_nanos());
    }
    let elapsed = workload_started.elapsed();
    // This owner-thread barrier waits for maintenance that started immediately
    // after the final SET response before taking the telemetry snapshot.
    let verification_key = workload_key(config, 0);
    handle
        .get(verification_key.as_bytes())
        .map_err(|error| format!("final barrier: {error:?}"))?;
    let maintenance = handle.maintenance_snapshots()[0];
    let storage = handle
        .storage_telemetry()
        .map_err(|error| format!("storage telemetry: {error:?}"))?[0]
        .storage;
    runtime.shutdown();
    let shard_directory = directory.0.join("shard-0000");
    let (retained_segment_files, retained_segment_bytes, retained_allocated_bytes) =
        segment_usage(&shard_directory)?;
    let recovery_started = Instant::now();
    let reopened =
        Runtime::open(&runtime_config).map_err(|error| format!("recovery open: {error:?}"))?;
    let recovery_ns = recovery_started.elapsed().as_nanos();
    let recovered = reopened
        .handle()
        .get(verification_key.as_bytes())
        .map_err(|error| format!("recovery verification: {error:?}"))?;
    if recovered.as_deref() != Some(value.as_slice()) {
        return Err("recovery returned the wrong hot value".to_owned());
    }
    reopened.shutdown();
    latencies.sort_unstable();
    let elapsed_ns = elapsed.as_nanos();
    let operations = u32::try_from(config.operations).expect("configuration validated");
    let operations_per_second = f64::from(operations) / elapsed.as_secs_f64();
    Ok(Row {
        strategy,
        operations: config.operations,
        value_bytes: config.value_bytes,
        key_space: config.key_space,
        persistent_every: config.persistent_every,
        minimum_input_segments: config.minimum_input_segments,
        segment_bytes: config.segment_bytes,
        minimum_reclaim_ratio_bps: config.minimum_reclaim_ratio_bps,
        pause_budget_ms: config.pause_budget_ms,
        trial,
        elapsed_ns,
        operations_per_second,
        percentiles: [
            percentile(&latencies, 500),
            percentile(&latencies, 950),
            percentile(&latencies, 990),
            percentile(&latencies, 999),
        ],
        maintenance,
        storage,
        retained_segment_files,
        retained_segment_bytes,
        retained_allocated_bytes,
        recovery_ns,
    })
}

fn workload_key(config: &Config, operation: usize) -> String {
    if config.persistent_every > 0 && operation.is_multiple_of(config.persistent_every) {
        format!("persistent-{operation:08}")
    } else {
        format!("churn-{:08}", operation % config.key_space)
    }
}

fn segment_usage(directory: &std::path::Path) -> Result<(usize, u64, u64), String> {
    let mut files = 0_usize;
    let mut bytes = 0_u64;
    let mut allocated = 0_u64;
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("segment-") && name.ends_with(".punah") {
            files += 1;
            let metadata = entry.metadata().map_err(|error| error.to_string())?;
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| "retained segment byte count overflow".to_owned())?;
            allocated = allocated
                .checked_add(allocated_bytes(&metadata))
                .ok_or_else(|| "allocated segment byte count overflow".to_owned())?;
        }
    }
    Ok((files, bytes, allocated))
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

fn percentile(sorted: &[u128], permille: usize) -> u128 {
    let rank = sorted.len().saturating_mul(permille).saturating_add(999) / 1000;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_rejects_zero_and_values_too_large_for_a_segment() {
        assert!(Config::parse(&["0".into(), "64".into(), "4".into(), "4096".into()]).is_err());
        assert!(Config::parse(&["10".into(), "4096".into(), "4".into(), "4096".into()]).is_err());
        assert!(
            Config::parse(&[
                "10".into(),
                "64".into(),
                "4".into(),
                "4096".into(),
                "1".into(),
                "1".into(),
                "4096".into(),
                "10001".into(),
            ])
            .is_err()
        );
    }

    #[test]
    fn tiny_background_case_reports_real_compaction_telemetry() {
        let config = Config {
            operations: 12,
            value_bytes: 16,
            key_space: 1,
            segment_bytes: 320,
            trials: 1,
            check_every_mutations: 1,
            max_input_bytes: 320,
            minimum_reclaim_ratio_bps: 0,
            pause_budget_ms: 25,
            persistent_every: 0,
            minimum_input_segments: 1,
        };
        let row = run_case(&config, Strategy::Background, 1).expect("case");
        assert!(row.maintenance.compactions > 0);
        assert!(row.maintenance.steps > row.maintenance.compactions);
        assert!(row.maintenance.copy_slices <= row.maintenance.steps);
        assert!(row.maintenance.output_preparations >= row.maintenance.compactions);
        assert!(
            row.maintenance.output_preparations <= row.maintenance.compactions.saturating_add(1)
        );
        assert!(row.maintenance.reclaimable_bytes > 0);
        assert_eq!(
            row.csv_line().split(',').count(),
            CSV_HEADER.split(',').count()
        );
    }
}
