#![forbid(unsafe_code)]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use punah_runtime::{BatchCommand, Runtime, RuntimeConfig};
use punah_types::{Durability, Namespace};

const MAX_BATCH: usize = 64;
const CSV_HEADER: &str = "strategy,payload_bytes,requested_batch_size,operations,append_groups,actual_records,max_records_per_append,encoded_bytes,flushes,rotations,elapsed_ns,ops_per_second,group_p50_ns,group_p95_ns,group_p99_ns,group_p999_ns,trial";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn main() -> Result<(), String> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let config = Config::parse(&arguments)?;
    println!("{CSV_HEADER}");
    for trial in 1..=config.trials {
        let mut cases = vec![Strategy::Singleton];
        cases.extend(
            config
                .batch_sizes
                .iter()
                .copied()
                .map(|batch_size| Strategy::Batch { batch_size }),
        );
        if trial % 2 == 0 {
            cases.reverse();
        }
        let mut rows = Vec::with_capacity(cases.len());
        for strategy in cases {
            rows.push(run_case(&config, strategy, trial)?);
        }
        rows.sort_by_key(|row| row.strategy);
        for row in rows {
            println!("{}", row.csv_line());
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    operations: usize,
    payload_bytes: usize,
    batch_sizes: Vec<usize>,
    trials: usize,
}

impl Config {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        if !(3..=4).contains(&arguments.len()) {
            return Err(usage());
        }
        let operations = parse_nonzero(&arguments[0], "OPERATIONS")?;
        u32::try_from(operations).map_err(|_| "OPERATIONS cannot exceed u32::MAX".to_owned())?;
        let payload_bytes = parse_nonzero(&arguments[1], "PAYLOAD_BYTES")?;
        let mut batch_sizes = arguments[2]
            .split(',')
            .map(|value| parse_nonzero(value, "BATCH_SIZES"))
            .collect::<Result<Vec<_>, _>>()?;
        if batch_sizes.is_empty() || batch_sizes.iter().any(|size| *size > MAX_BATCH) {
            return Err(format!("BATCH_SIZES must be between 1 and {MAX_BATCH}"));
        }
        batch_sizes.sort_unstable();
        batch_sizes.dedup();
        let trials = arguments
            .get(3)
            .map_or(Ok(5), |value| parse_nonzero(value, "TRIALS"))?;
        Ok(Self {
            operations,
            payload_bytes,
            batch_sizes,
            trials,
        })
    }
}

fn parse_nonzero(value: &str, name: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if value == 0 {
        return Err(format!("{name} must be nonzero"));
    }
    Ok(value)
}

fn usage() -> String {
    "usage: runtime_cohort_compare OPERATIONS PAYLOAD_BYTES BATCH_SIZES [TRIALS=5]".to_owned()
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Strategy {
    Singleton,
    Batch { batch_size: usize },
}

impl Strategy {
    const fn name(self) -> &'static str {
        match self {
            Self::Singleton => "singleton",
            Self::Batch { .. } => "runtime_batch",
        }
    }

    const fn batch_size(self) -> usize {
        match self {
            Self::Singleton => 1,
            Self::Batch { batch_size } => batch_size,
        }
    }
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new(trial: usize, strategy: Strategy) -> Result<Self, String> {
        let nonce = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "punah-runtime-cohort-{}-{trial}-{}-{nonce}",
            std::process::id(),
            strategy.batch_size()
        ));
        fs::create_dir(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Row {
    strategy: Strategy,
    payload_bytes: usize,
    operations: usize,
    append_groups: u64,
    actual_records: u64,
    max_records_per_append: u64,
    encoded_bytes: u64,
    flushes: u64,
    rotations: u64,
    elapsed_ns: u128,
    operations_per_second: f64,
    percentiles: [u128; 4],
    trial: usize,
}

impl Row {
    fn csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{:.2},{},{},{},{},{}",
            self.strategy.name(),
            self.payload_bytes,
            self.strategy.batch_size(),
            self.operations,
            self.append_groups,
            self.actual_records,
            self.max_records_per_append,
            self.encoded_bytes,
            self.flushes,
            self.rotations,
            self.elapsed_ns,
            self.operations_per_second,
            self.percentiles[0],
            self.percentiles[1],
            self.percentiles[2],
            self.percentiles[3],
            self.trial,
        )
    }
}

fn run_case(config: &Config, strategy: Strategy, trial: usize) -> Result<Row, String> {
    let directory = TemporaryDirectory::new(trial, strategy)?;
    let bytes_per_record = u64::try_from(config.payload_bytes)
        .map_err(|_| "payload not representable".to_owned())?
        .saturating_add(512);
    let operations =
        u64::try_from(config.operations).map_err(|_| "operations not representable".to_owned())?;
    let segment_capacity = bytes_per_record.saturating_mul(operations).max(1024 * 1024);
    let runtime = Runtime::open(&RuntimeConfig {
        directory: directory.0.clone(),
        namespace: Namespace::new("benchmark").map_err(|error| format!("{error:?}"))?,
        shard_count: 1,
        queue_capacity: 64,
        segment_capacity,
        durability: Durability::LocalDurable,
    })
    .map_err(|error| format!("open: {error:?}"))?;
    let handle = runtime.handle();
    let value = vec![0x5a; config.payload_bytes];
    let mut latencies = Vec::new();
    let started = Instant::now();
    let mut next = 0_usize;
    while next < config.operations {
        let count = strategy.batch_size().min(config.operations - next);
        let group_started = Instant::now();
        match strategy {
            Strategy::Singleton => handle
                .set(key(next).as_bytes(), value.clone(), None)
                .map_err(|error| format!("set {next}: {error:?}"))?,
            Strategy::Batch { .. } => {
                let commands = (next..next + count)
                    .map(|index| BatchCommand::Set {
                        key: key(index).into_bytes(),
                        value: value.clone(),
                        ttl: None,
                    })
                    .collect();
                let results = handle
                    .execute_batch(commands)
                    .map_err(|error| format!("batch {next}: {error:?}"))?;
                if results.iter().any(Result::is_err) {
                    return Err(format!("batch {next} returned a command error"));
                }
            }
        }
        latencies.push(group_started.elapsed().as_nanos());
        next += count;
    }
    let elapsed = started.elapsed();
    let telemetry = handle
        .storage_telemetry()
        .map_err(|error| format!("telemetry: {error:?}"))?[0]
        .storage;
    runtime.shutdown();
    let append_groups = telemetry
        .singleton_appends
        .saturating_add(telemetry.cohort_appends);
    if telemetry.appended_records != u64::try_from(config.operations).unwrap_or(u64::MAX)
        || telemetry.durability_flushes != append_groups.saturating_mul(2)
        || telemetry.rotations != 0
    {
        return Err("runtime telemetry invariant failed".to_owned());
    }
    latencies.sort_unstable();
    let operation_count = u32::try_from(config.operations).expect("configuration validated");
    Ok(Row {
        strategy,
        payload_bytes: config.payload_bytes,
        operations: config.operations,
        append_groups,
        actual_records: telemetry.appended_records,
        max_records_per_append: telemetry.max_records_per_append,
        encoded_bytes: telemetry.encoded_bytes,
        flushes: telemetry.durability_flushes,
        rotations: telemetry.rotations,
        elapsed_ns: elapsed.as_nanos(),
        operations_per_second: f64::from(operation_count) / elapsed.as_secs_f64(),
        percentiles: [
            percentile(&latencies, 500),
            percentile(&latencies, 950),
            percentile(&latencies, 990),
            percentile(&latencies, 999),
        ],
        trial,
    })
}

fn key(index: usize) -> String {
    format!("runtime-cohort-{index:08}")
}

fn percentile(sorted: &[u128], permille: usize) -> u128 {
    let rank = sorted.len().saturating_mul(permille).saturating_add(999) / 1000;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiny_runtime_batch_proves_flush_amortization_with_actual_counters() {
        let config = Config {
            operations: 5,
            payload_bytes: 16,
            batch_sizes: vec![2],
            trials: 1,
        };
        let row = run_case(&config, Strategy::Batch { batch_size: 2 }, 1).expect("case");
        assert_eq!(row.actual_records, 5);
        assert_eq!(row.append_groups, 3);
        assert_eq!(row.flushes, 6);
        assert_eq!(row.max_records_per_append, 2);
        assert_eq!(
            row.csv_line().split(',').count(),
            CSV_HEADER.split(',').count()
        );
    }
}
