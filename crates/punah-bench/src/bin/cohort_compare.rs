#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use punah_device::{
    BlockDevice, DeviceError, FileDevice, IoCompletion, IoOperation, IoRequest, SubmitError,
};
use punah_engine::SegmentWriter;
use punah_record::{Record, RecordType, encoded_len};
use punah_types::{Durability, Namespace, SegmentId};

const MAX_BATCH_SIZE: usize = 64;
const DEFAULT_TRIALS: usize = 5;
const DEFAULT_QUEUE_DEPTH: usize = 64;
const CSV_HEADER: &str = "strategy,payload_bytes,requested_batch_size,operations,append_calls,min_batch,max_batch,mean_records_per_append,encoded_bytes,queue_depth,accepted_writes,write_bytes,flushes,queue_full_rejections,elapsed_ns,ops_per_second,group_p50_ns,group_p95_ns,group_p99_ns,group_p999_ns,trial";
static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

fn main() -> Result<(), String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let config = Config::parse(&args)?;
    let mut results = Vec::new();

    for &payload_bytes in &config.payload_bytes {
        for trial in 0..config.trials {
            let mut strategies = Vec::with_capacity(config.batch_sizes.len() + 1);
            strategies.push(Strategy::Singleton);
            strategies.extend(
                config
                    .batch_sizes
                    .iter()
                    .copied()
                    .map(|batch_size| Strategy::Cohort { batch_size }),
            );
            if trial % 2 == 1 {
                strategies.reverse();
            }
            for strategy in strategies {
                results.push(run_file_case(
                    config.operations,
                    payload_bytes,
                    strategy,
                    config.queue_depth,
                    trial + 1,
                )?);
            }
        }
    }

    results.sort_by_key(|result| {
        (
            result.payload_bytes,
            result.trial,
            result.strategy.sort_key(),
        )
    });
    println!("{CSV_HEADER}");
    for result in results {
        println!("{}", result.csv_line());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    operations: usize,
    payload_bytes: Vec<usize>,
    batch_sizes: Vec<usize>,
    trials: usize,
    queue_depth: usize,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        if !(3..=5).contains(&args.len()) {
            return Err(
                "usage: cohort_compare OPERATIONS PAYLOADS BATCH_SIZES [TRIALS=5] [QUEUE_DEPTH=64]"
                    .to_owned(),
            );
        }
        let operations = parse_nonzero(&args[0], "OPERATIONS")?;
        u32::try_from(operations).map_err(|_| "OPERATIONS cannot exceed u32::MAX".to_owned())?;
        let payload_bytes = parse_list(&args[1], "PAYLOADS")?;
        let mut batch_sizes = parse_list(&args[2], "BATCH_SIZES")?;
        if batch_sizes
            .iter()
            .any(|batch_size| *batch_size > MAX_BATCH_SIZE)
        {
            return Err(format!("BATCH_SIZES cannot exceed {MAX_BATCH_SIZE}"));
        }
        batch_sizes.sort_unstable();
        batch_sizes.dedup();
        let trials = args
            .get(3)
            .map_or(Ok(DEFAULT_TRIALS), |value| parse_nonzero(value, "TRIALS"))?;
        let queue_depth = args.get(4).map_or(Ok(DEFAULT_QUEUE_DEPTH), |value| {
            parse_nonzero(value, "QUEUE_DEPTH")
        })?;
        Ok(Self {
            operations,
            payload_bytes,
            batch_sizes,
            trials,
            queue_depth,
        })
    }
}

fn parse_nonzero(value: &str, name: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be nonzero"));
    }
    Ok(parsed)
}

fn parse_list(value: &str, name: &str) -> Result<Vec<usize>, String> {
    if value.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    value
        .split(',')
        .map(|item| parse_nonzero(item, name))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Strategy {
    Singleton,
    Cohort { batch_size: usize },
}

impl Strategy {
    const fn requested_batch_size(self) -> usize {
        match self {
            Self::Singleton => 1,
            Self::Cohort { batch_size } => batch_size,
        }
    }

    const fn sort_key(self) -> (u8, usize) {
        match self {
            Self::Singleton => (0, 1),
            Self::Cohort { batch_size } => (1, batch_size),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Singleton => "singleton",
            Self::Cohort { .. } => "cohort",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct IoTelemetry {
    accepted_writes: usize,
    write_bytes: usize,
    accepted_flushes: usize,
    queue_full_rejections: usize,
}

struct CountingDevice<D> {
    inner: D,
    telemetry: IoTelemetry,
}

impl<D> CountingDevice<D> {
    const fn new(inner: D) -> Self {
        Self {
            inner,
            telemetry: IoTelemetry {
                accepted_writes: 0,
                write_bytes: 0,
                accepted_flushes: 0,
                queue_full_rejections: 0,
            },
        }
    }

    fn into_parts(self) -> (D, IoTelemetry) {
        (self.inner, self.telemetry)
    }
}

impl<D: BlockDevice> BlockDevice for CountingDevice<D> {
    fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError> {
        let write_bytes = match &request.operation {
            IoOperation::Write { data, .. } => Some(data.len()),
            IoOperation::Read { .. } | IoOperation::Flush => None,
        };
        let is_flush = matches!(request.operation, IoOperation::Flush);
        match self.inner.submit(request) {
            Ok(()) => {
                if let Some(bytes) = write_bytes {
                    self.telemetry.accepted_writes += 1;
                    self.telemetry.write_bytes += bytes;
                }
                self.telemetry.accepted_flushes += usize::from(is_flush);
                Ok(())
            }
            Err(rejected) => {
                self.telemetry.queue_full_rejections +=
                    usize::from(rejected.error == DeviceError::QueueFull);
                Err(rejected)
            }
        }
    }

    fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize {
        self.inner.poll(completions, limit)
    }
}

struct CaseResult {
    strategy: Strategy,
    payload_bytes: usize,
    operations: usize,
    append_calls: usize,
    min_batch: usize,
    max_batch: usize,
    encoded_bytes: usize,
    queue_depth: usize,
    telemetry: IoTelemetry,
    elapsed: Duration,
    group_latencies: Vec<u128>,
    trial: usize,
}

impl CaseResult {
    fn csv_line(mut self) -> String {
        self.group_latencies.sort_unstable();
        format!(
            "{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{},{:.2},{},{},{},{},{}",
            self.strategy.label(),
            self.payload_bytes,
            self.strategy.requested_batch_size(),
            self.operations,
            self.append_calls,
            self.min_batch,
            self.max_batch,
            usize_as_f64(self.operations) / usize_as_f64(self.append_calls),
            self.encoded_bytes,
            self.queue_depth,
            self.telemetry.accepted_writes,
            self.telemetry.write_bytes,
            self.telemetry.accepted_flushes,
            self.telemetry.queue_full_rejections,
            self.elapsed.as_nanos(),
            usize_as_f64(self.operations) / self.elapsed.as_secs_f64(),
            percentile(&self.group_latencies, 500),
            percentile(&self.group_latencies, 950),
            percentile(&self.group_latencies, 990),
            percentile(&self.group_latencies, 999),
            self.trial,
        )
    }
}

fn run_file_case(
    operations: usize,
    payload_bytes: usize,
    strategy: Strategy,
    queue_depth: usize,
    trial: usize,
) -> Result<CaseResult, String> {
    let records = make_records(operations, payload_bytes)?;
    let encoded_bytes = total_encoded_bytes(&records)?;
    let capacity = u64::try_from(encoded_bytes.max(128))
        .map_err(|_| "encoded fixture exceeds u64 capacity".to_owned())?;
    let path = std::env::temp_dir().join(format!(
        "punah-cohort-bench-{}-{}.punah",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let temporary = TemporaryFile(path.clone());
    let device = FileDevice::create(&path, capacity, queue_depth)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    device
        .sync_all()
        .map_err(|error| format!("initialize {}: {error}", path.display()))?;
    let result = execute_case(
        device,
        records,
        encoded_bytes,
        payload_bytes,
        strategy,
        queue_depth,
        trial,
    );
    drop(temporary);
    result
}

fn execute_case<D: BlockDevice>(
    device: D,
    records: Vec<Record>,
    encoded_bytes: usize,
    payload_bytes: usize,
    strategy: Strategy,
    queue_depth: usize,
    trial: usize,
) -> Result<CaseResult, String> {
    let operations = records.len();
    let capacity = u64::try_from(encoded_bytes.max(128))
        .map_err(|_| "encoded fixture exceeds u64 capacity".to_owned())?;
    let groups = partition_records(records, strategy.requested_batch_size());
    let append_calls = groups.len();
    let min_batch = groups.iter().map(Vec::len).min().unwrap_or(0);
    let max_batch = groups.iter().map(Vec::len).max().unwrap_or(0);
    let mut writer = SegmentWriter::new(CountingDevice::new(device), SegmentId(1), 0, capacity);
    let mut group_latencies = Vec::with_capacity(append_calls);
    let started = Instant::now();
    for mut group in groups {
        let actual_batch = group.len();
        let group_started = Instant::now();
        match strategy {
            Strategy::Singleton => {
                let record = group.pop().expect("a singleton group has one record");
                writer
                    .try_append(record, Durability::LocalDurable)
                    .map_err(|error| format!("singleton submission failed: {:?}", error.error))?;
                loop {
                    if let Some(completion) = writer.poll() {
                        completion
                            .map_err(|error| format!("singleton append failed: {error:?}"))?;
                        break;
                    }
                    std::thread::yield_now();
                }
            }
            Strategy::Cohort { .. } => {
                let completions = writer
                    .append_cohort(group, Durability::LocalDurable)
                    .map_err(|error| format!("cohort append failed: {:?}", error.error))?;
                if completions.len() != actual_batch {
                    return Err("cohort completion count does not match admission".to_owned());
                }
            }
        }
        group_latencies.push(group_started.elapsed().as_nanos());
    }
    let elapsed = started.elapsed();
    let (device, telemetry) = writer.into_device().into_parts();
    drop(device);

    let expected_writes = operations
        .checked_mul(2)
        .ok_or_else(|| "write-count overflow".to_owned())?;
    let expected_flushes = append_calls
        .checked_mul(2)
        .ok_or_else(|| "flush-count overflow".to_owned())?;
    if telemetry.accepted_writes != expected_writes
        || telemetry.accepted_flushes != expected_flushes
    {
        return Err(format!(
            "LocalDurable telemetry mismatch: writes {} != {expected_writes}, flushes {} != {expected_flushes}",
            telemetry.accepted_writes, telemetry.accepted_flushes
        ));
    }
    let marker_bytes = operations
        .checked_mul(4)
        .ok_or_else(|| "marker-byte overflow".to_owned())?;
    let expected_write_bytes = encoded_bytes
        .checked_add(marker_bytes)
        .ok_or_else(|| "write-byte overflow".to_owned())?;
    if telemetry.write_bytes != expected_write_bytes {
        return Err(format!(
            "write-byte telemetry mismatch: {} != {expected_write_bytes}",
            telemetry.write_bytes
        ));
    }

    Ok(CaseResult {
        strategy,
        payload_bytes,
        operations,
        append_calls,
        min_batch,
        max_batch,
        encoded_bytes,
        queue_depth,
        telemetry,
        elapsed,
        group_latencies,
        trial,
    })
}

fn partition_records(records: Vec<Record>, batch_size: usize) -> Vec<Vec<Record>> {
    let mut groups = Vec::with_capacity(records.len().div_ceil(batch_size));
    let mut records = records.into_iter();
    loop {
        let group: Vec<_> = records.by_ref().take(batch_size).collect();
        if group.is_empty() {
            break;
        }
        groups.push(group);
    }
    groups
}

fn make_records(operations: usize, payload_bytes: usize) -> Result<Vec<Record>, String> {
    let namespace = Namespace::new("bench").map_err(|error| format!("namespace: {error:?}"))?;
    (0..operations)
        .map(|index| {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| "operation sequence exceeds u64".to_owned())?;
            Ok(Record {
                record_type: RecordType::Artifact,
                flags: 0,
                namespace: namespace.clone(),
                key: format!("bench-{index:016}").into_bytes(),
                sequence,
                created_at_unix_ns: sequence,
                expires_at_unix_ns: None,
                operation_hash: [0; 32],
                content_hash: [u8::try_from(index % 251).expect("modulo fits u8"); 32],
                dependencies: Vec::new(),
                payload: vec![0x5a; payload_bytes],
            })
        })
        .collect()
}

fn total_encoded_bytes(records: &[Record]) -> Result<usize, String> {
    records.iter().try_fold(0_usize, |total, record| {
        let length = encoded_len(record).map_err(|error| format!("encode length: {error:?}"))?;
        total
            .checked_add(length)
            .ok_or_else(|| "encoded fixture size overflow".to_owned())
    })
}

fn percentile(sorted: &[u128], permille: usize) -> u128 {
    let index = sorted.len().saturating_mul(permille).saturating_add(999) / 1_000;
    sorted[index.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

fn usize_as_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).expect("benchmark operation counts are validated as u32"))
}

struct TemporaryFile(PathBuf);

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use punah_device::MemoryDevice;

    fn memory_case(operations: usize, strategy: Strategy, queue_depth: usize) -> CaseResult {
        let records = make_records(operations, 64).expect("records");
        let encoded_bytes = total_encoded_bytes(&records).expect("encoded bytes");
        execute_case(
            MemoryDevice::new(encoded_bytes.max(128), queue_depth),
            records,
            encoded_bytes,
            64,
            strategy,
            queue_depth,
            1,
        )
        .expect("benchmark case")
    }

    #[test]
    fn singleton_telemetry_counts_two_writes_and_flushes_per_record() {
        let result = memory_case(3, Strategy::Singleton, 4);
        assert_eq!(result.append_calls, 3);
        assert_eq!(result.telemetry.accepted_writes, 6);
        assert_eq!(result.telemetry.accepted_flushes, 6);
    }

    #[test]
    fn cohort_telemetry_reports_actual_partial_final_batch() {
        let result = memory_case(5, Strategy::Cohort { batch_size: 2 }, 4);
        assert_eq!(result.append_calls, 3);
        assert_eq!((result.min_batch, result.max_batch), (1, 2));
        assert_eq!(result.telemetry.accepted_writes, 10);
        assert_eq!(result.telemetry.accepted_flushes, 6);
    }

    #[test]
    fn shallow_queue_retries_cohort_backpressure() {
        let result = memory_case(4, Strategy::Cohort { batch_size: 4 }, 1);
        assert_eq!(result.telemetry.accepted_writes, 8);
        assert_eq!(result.telemetry.accepted_flushes, 2);
        assert!(result.telemetry.queue_full_rejections > 0);
    }

    #[test]
    fn parser_rejects_zeroes_and_oversized_batches() {
        let valid = ["10", "64,1024", "2,4", "5", "64"].map(str::to_owned);
        assert!(Config::parse(&valid).is_ok());
        for invalid in [
            ["0", "64", "2", "5", "64"],
            ["10", "0", "2", "5", "64"],
            ["10", "64", "0", "5", "64"],
            ["10", "64", "2", "0", "64"],
            ["10", "64", "2", "5", "0"],
            ["10", "64", "65", "5", "64"],
        ] {
            assert!(Config::parse(&invalid.map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn csv_shape_and_percentiles_are_stable() {
        let result = memory_case(5, Strategy::Cohort { batch_size: 2 }, 4);
        assert_eq!(
            result.csv_line().split(',').count(),
            CSV_HEADER.split(',').count()
        );
        assert_eq!(percentile(&[1, 2, 3, 4], 500), 2);
        assert_eq!(percentile(&[1, 2, 3, 4], 999), 4);
    }
}
