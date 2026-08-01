#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use punah_bench::{Args, CSV_HEADER, Mode, ResultRow, WorkloadMix, format_row, parse_args, usage};
use punah_compaction::CompactionPlan;
use punah_metadata::SegmentRef;
use punah_runtime::{Runtime, RuntimeConfig, RuntimeError, RuntimeHandle};
use punah_store::{Store, StoreConfig};
use punah_types::{Durability, Namespace};

static NEXT_RUN: AtomicU64 = AtomicU64::new(0);

fn main() {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            if error == usage() {
                return;
            }
            std::process::exit(2);
        }
    };
    if cfg!(debug_assertions) {
        eprintln!(
            "warning: running a debug build; use `cargo run --release -p punah-bench -- ...`"
        );
    }
    if let Err(error) = run(&args) {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run(args: &Args) -> Result<(), String> {
    let owned_root = args.root.is_none();
    let root = args.root.clone().unwrap_or_else(temporary_root);
    fs::create_dir_all(&root).map_err(|error| error.to_string())?;
    println!("{CSV_HEADER}");
    let outcome = run_at(args, &root);
    if owned_root {
        fs::remove_dir_all(&root).map_err(|error| error.to_string())?;
    }
    outcome
}

fn run_at(args: &Args, root: &Path) -> Result<(), String> {
    for trial in 1..=args.trials {
        if matches!(args.mode, Mode::Write | Mode::All) {
            for &operations in &args.operations {
                for &payload_size in &args.payload_sizes {
                    for &concurrency in &args.concurrency {
                        for &capacity in &args.segment_capacities {
                            let row = write_benchmark(
                                root,
                                operations,
                                payload_size,
                                concurrency,
                                capacity,
                                args.durable,
                                trial,
                                args.warmup_operations,
                                args.seed,
                            )?;
                            println!("{}", format_row(&row));
                        }
                    }
                }
            }
        }
        if matches!(args.mode, Mode::Recovery | Mode::All) {
            for &operations in &args.operations {
                for &payload_size in &args.payload_sizes {
                    for &capacity in &args.segment_capacities {
                        let row = recovery_benchmark(
                            root,
                            operations,
                            payload_size,
                            capacity,
                            args.durable,
                            trial,
                            args.seed,
                        )?;
                        println!("{}", format_row(&row));
                    }
                }
            }
        }
        if matches!(args.mode, Mode::Mixed | Mode::All) {
            for &operations in &args.operations {
                for &payload_size in &args.payload_sizes {
                    for &concurrency in &args.concurrency {
                        for &capacity in &args.segment_capacities {
                            for row in mixed_benchmark(
                                root,
                                operations,
                                payload_size,
                                concurrency,
                                capacity,
                                args.durable,
                                trial,
                                args.warmup_operations,
                                args.seed,
                                args.mix,
                            )? {
                                println!("{}", format_row(&row));
                            }
                        }
                    }
                }
            }
        }
        if matches!(args.mode, Mode::Shared | Mode::All) {
            for &operations in &args.operations {
                for &payload_size in &args.payload_sizes {
                    for &concurrency in &args.concurrency {
                        for &shards in &args.shards {
                            for &capacity in &args.segment_capacities {
                                for row in shared_benchmark(
                                    root,
                                    operations,
                                    payload_size,
                                    concurrency,
                                    shards,
                                    capacity,
                                    args.durable,
                                    trial,
                                    args.warmup_operations,
                                    args.seed,
                                    args.mix,
                                )? {
                                    println!("{}", format_row(&row));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_benchmark(
    root: &Path,
    operations: usize,
    payload_size: usize,
    concurrency: usize,
    capacity: u64,
    durable: bool,
    trial: usize,
    warmup: usize,
    seed: u64,
) -> Result<ResultRow, String> {
    let case = case_directory(root, "write");
    fs::create_dir(&case).map_err(|error| error.to_string())?;
    let mut workers = Vec::with_capacity(concurrency);
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    for worker in 0..concurrency {
        let directory = case.join(format!("worker-{worker}"));
        let worker_operations =
            operations / concurrency + usize::from(worker < operations % concurrency);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || -> Result<Vec<u128>, String> {
            let mut store = Store::open(config(directory, capacity, durable))
                .map_err(|error| format!("open: {error:?}"))?;
            let payload = vec![0x5a; payload_size];
            for index in 0..warmup {
                store
                    .set(
                        format!("warmup-{worker}-{index}").as_bytes(),
                        payload.clone(),
                        None,
                    )
                    .map_err(|error| format!("warmup: {error:?}"))?;
            }
            barrier.wait();
            let mut latencies = Vec::with_capacity(worker_operations);
            for index in 0..worker_operations {
                let operation_started = Instant::now();
                store
                    .set(
                        format!("worker-{worker}-key-{index}").as_bytes(),
                        payload.clone(),
                        None,
                    )
                    .map_err(|error| format!("set: {error:?}"))?;
                latencies.push(operation_started.elapsed().as_nanos());
            }
            Ok(latencies)
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operations);
    for worker in workers {
        latencies.extend(
            worker
                .join()
                .map_err(|_| "benchmark worker panicked".to_owned())??,
        );
    }
    let elapsed = started.elapsed();
    let segments = count_segments_recursive(&case)?;
    fs::remove_dir_all(&case).map_err(|error| error.to_string())?;
    Ok(row(
        "write",
        durable,
        operations,
        payload_size,
        concurrency,
        capacity,
        segments,
        elapsed,
        &mut latencies,
        trial,
        warmup,
        seed,
        "set",
    ))
}

fn recovery_benchmark(
    root: &Path,
    operations: usize,
    payload_size: usize,
    capacity: u64,
    durable: bool,
    trial: usize,
    seed: u64,
) -> Result<ResultRow, String> {
    let case = case_directory(root, "recovery");
    let config = config(case.clone(), capacity, durable);
    {
        let mut store = Store::open(config.clone()).map_err(|error| format!("open: {error:?}"))?;
        let payload = vec![0x5a; payload_size];
        for index in 0..operations {
            store
                .set(format!("key-{index}").as_bytes(), payload.clone(), None)
                .map_err(|error| format!("prepare: {error:?}"))?;
        }
    }
    let segments = count_segments_recursive(&case)?;
    let started = Instant::now();
    let reopened = Store::open(config).map_err(|error| format!("reopen: {error:?}"))?;
    let elapsed = started.elapsed();
    std::hint::black_box(reopened);
    fs::remove_dir_all(&case).map_err(|error| error.to_string())?;
    Ok(row(
        "recovery",
        durable,
        operations,
        payload_size,
        1,
        capacity,
        segments,
        elapsed,
        &mut [],
        trial,
        0,
        seed,
        "recovery",
    ))
}

#[derive(Default)]
struct ClassLatencies {
    get: Vec<u128>,
    set: Vec<u128>,
    overwrite: Vec<u128>,
    delete: Vec<u128>,
}

#[derive(Default)]
struct MixedWorkerResult {
    latencies: ClassLatencies,
    measured_elapsed: Duration,
    copied_bytes: u64,
    reclaimed_bytes: u64,
    compaction_pause_ns: u128,
    compaction_performed: bool,
}

impl ClassLatencies {
    fn record(&mut self, class: &str, latency: u128) {
        match class {
            "get" => self.get.push(latency),
            "set" => self.set.push(latency),
            "overwrite" => self.overwrite.push(latency),
            "delete" => self.delete.push(latency),
            _ => unreachable!("workload class is closed"),
        }
    }

    fn merge(&mut self, other: Self) {
        self.get.extend(other.get);
        self.set.extend(other.set);
        self.overwrite.extend(other.overwrite);
        self.delete.extend(other.delete);
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn mixed_benchmark(
    root: &Path,
    operations: usize,
    payload_size: usize,
    concurrency: usize,
    capacity: u64,
    durable: bool,
    trial: usize,
    warmup: usize,
    seed: u64,
    mix: WorkloadMix,
) -> Result<Vec<ResultRow>, String> {
    let case = case_directory(root, "mixed");
    fs::create_dir(&case).map_err(|error| error.to_string())?;
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let directory = case.join(format!("worker-{worker}"));
        let worker_operations =
            operations / concurrency + usize::from(worker < operations % concurrency);
        let worker_seed = seed ^ (trial as u64).rotate_left(17) ^ (worker as u64).rotate_left(33);
        workers.push(thread::spawn(
            move || -> Result<MixedWorkerResult, String> {
                let mut store = Store::open(config(directory, capacity, durable))
                    .map_err(|error| format!("open: {error:?}"))?;
                let payload = vec![0x5a; payload_size];
                let key_space = worker_operations.clamp(1, 1_024);
                for index in 0..key_space {
                    store
                        .set(format!("key-{index}").as_bytes(), payload.clone(), None)
                        .map_err(|error| format!("prepare: {error:?}"))?;
                }
                let mut random = DeterministicRandom::new(worker_seed);
                for _ in 0..warmup {
                    execute_mixed(&mut store, &payload, key_space, mix, &mut random)?;
                }
                let mut measured = ClassLatencies::default();
                let measured_started = Instant::now();
                for _ in 0..worker_operations {
                    let sample = random.percentage();
                    let class = mix.class(sample);
                    let operation_started = Instant::now();
                    execute_class(&mut store, &payload, key_space, class, &mut random)?;
                    measured.record(class, operation_started.elapsed().as_nanos());
                }
                let measured_elapsed = measured_started.elapsed();
                let inputs: Vec<_> = store
                    .sealed_segments()
                    .iter()
                    .map(|segment| SegmentRef {
                        id: segment.segment_id.0,
                        generation: segment.generation,
                    })
                    .collect();
                let input_bytes = store
                    .sealed_segments()
                    .iter()
                    .map(|segment| segment.committed_len)
                    .sum::<u64>();
                let required_output_bytes = store
                    .version_snapshots()
                    .iter()
                    .filter(|version| {
                        inputs.iter().any(|input| {
                            input.id == version.location.segment_id.0
                                && input.generation == version.location.generation
                        })
                    })
                    .map(|version| u64::from(version.location.stored_len))
                    .sum::<u64>();
                let mut result = MixedWorkerResult {
                    latencies: measured,
                    measured_elapsed,
                    ..MixedWorkerResult::default()
                };
                if !inputs.is_empty() && required_output_bytes <= capacity {
                    let plan = CompactionPlan {
                        input_segments: inputs,
                        input_bytes,
                        reclaimable_bytes: input_bytes.saturating_sub(required_output_bytes),
                        required_output_bytes,
                        required_headroom_bytes: required_output_bytes,
                    };
                    let compact_started = Instant::now();
                    let report = store
                        .compact(&plan)
                        .map_err(|error| format!("compact: {error:?}"))?;
                    result.compaction_pause_ns = compact_started.elapsed().as_nanos();
                    result.compaction_performed = true;
                    result.copied_bytes = report.copied_bytes;
                    result.reclaimed_bytes = input_bytes.saturating_sub(report.copied_bytes);
                }
                Ok(result)
            },
        ));
    }
    let mut measured = ClassLatencies::default();
    let mut copied_bytes = 0_u64;
    let mut reclaimed_bytes = 0_u64;
    let mut compaction_pause_ns = 0_u128;
    let mut compaction_performed = false;
    let mut elapsed = Duration::ZERO;
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| "benchmark worker panicked".to_owned())??;
        measured.merge(result.latencies);
        copied_bytes = copied_bytes.saturating_add(result.copied_bytes);
        reclaimed_bytes = reclaimed_bytes.saturating_add(result.reclaimed_bytes);
        compaction_pause_ns = compaction_pause_ns.max(result.compaction_pause_ns);
        compaction_performed |= result.compaction_performed;
        elapsed = elapsed.max(result.measured_elapsed);
    }
    let segments = count_segments_recursive(&case)?;
    fs::remove_dir_all(&case).map_err(|error| error.to_string())?;
    let mut rows = Vec::with_capacity(5);
    let mut all = Vec::with_capacity(operations);
    all.extend_from_slice(&measured.get);
    all.extend_from_slice(&measured.set);
    all.extend_from_slice(&measured.overwrite);
    all.extend_from_slice(&measured.delete);
    rows.push(row(
        "mixed",
        durable,
        operations,
        payload_size,
        concurrency,
        capacity,
        segments,
        elapsed,
        &mut all,
        trial,
        warmup,
        seed,
        "all",
    ));
    for (class, mut samples) in [
        ("get", measured.get),
        ("set", measured.set),
        ("overwrite", measured.overwrite),
        ("delete", measured.delete),
    ] {
        let count = samples.len();
        rows.push(row(
            "mixed",
            durable,
            count,
            payload_size,
            concurrency,
            capacity,
            segments,
            elapsed,
            &mut samples,
            trial,
            warmup,
            seed,
            class,
        ));
    }
    let input_bytes = copied_bytes.saturating_add(reclaimed_bytes);
    #[allow(clippy::cast_precision_loss)]
    let write_amplification = if input_bytes == 0 {
        0.0
    } else {
        copied_bytes as f64 / input_bytes as f64
    };
    for row in &mut rows {
        row.copied_bytes = copied_bytes;
        row.reclaimed_bytes = reclaimed_bytes;
        row.write_amplification = write_amplification;
        row.compaction_pause_ns = compaction_pause_ns;
        row.compaction_performed = compaction_performed;
    }
    Ok(rows)
}

fn execute_mixed(
    store: &mut Store,
    payload: &[u8],
    key_space: usize,
    mix: WorkloadMix,
    random: &mut DeterministicRandom,
) -> Result<(), String> {
    execute_class(
        store,
        payload,
        key_space,
        mix.class(random.percentage()),
        random,
    )
}

fn execute_class(
    store: &mut Store,
    payload: &[u8],
    key_space: usize,
    class: &str,
    random: &mut DeterministicRandom,
) -> Result<(), String> {
    let index = random.index(key_space);
    let key = format!("key-{index}");
    match class {
        "get" => {
            let _ = std::hint::black_box(store.get(key.as_bytes()));
        }
        "set" => store
            .set(
                format!("new-{}", random.next()).as_bytes(),
                payload.to_vec(),
                None,
            )
            .map_err(|error| format!("set: {error:?}"))?,
        "overwrite" => store
            .set(key.as_bytes(), payload.to_vec(), None)
            .map_err(|error| format!("overwrite: {error:?}"))?,
        "delete" => {
            store
                .delete(key.as_bytes())
                .map_err(|error| format!("delete: {error:?}"))?;
        }
        _ => unreachable!("workload class is closed"),
    }
    Ok(())
}

struct DeterministicRandom(u64);
impl DeterministicRandom {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        })
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn percentage(&mut self) -> u8 {
        u8::try_from(self.next() % 100).expect("modulo 100 fits")
    }
    fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(len).expect("key space fits u64"))
            .expect("bounded index fits usize")
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn shared_benchmark(
    root: &Path,
    operations: usize,
    payload_size: usize,
    concurrency: usize,
    shards: usize,
    capacity: u64,
    durable: bool,
    trial: usize,
    warmup: usize,
    seed: u64,
    mix: WorkloadMix,
) -> Result<Vec<ResultRow>, String> {
    let case = case_directory(root, "shared");
    let runtime = Runtime::open(&RuntimeConfig {
        directory: case.clone(),
        namespace: Namespace::new("benchmark").expect("static namespace"),
        shard_count: shards,
        queue_capacity: 64,
        segment_capacity: capacity,
        durability: if durable {
            Durability::LocalDurable
        } else {
            Durability::Buffered
        },
    })
    .map_err(|error| format!("runtime open: {error:?}"))?;
    let bootstrap = runtime.handle();
    let key_space = operations.clamp(1, 1_024);
    let payload = vec![0x5a; payload_size];
    let mut bootstrap_retries = 0;
    for index in 0..key_space {
        retry_set(
            &bootstrap,
            format!("key-{index}").as_bytes(),
            &payload,
            &mut bootstrap_retries,
        )?;
    }
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let handle = runtime.handle();
        let payload = payload.clone();
        let barrier = Arc::clone(&barrier);
        let worker_operations =
            operations / concurrency + usize::from(worker < operations % concurrency);
        let worker_seed = seed ^ (trial as u64).rotate_left(17) ^ (worker as u64).rotate_left(33);
        workers.push(thread::spawn(
            move || -> Result<(ClassLatencies, u64), String> {
                let mut random = DeterministicRandom::new(worker_seed);
                let mut retries = 0_u64;
                for _ in 0..warmup {
                    execute_shared(
                        &handle,
                        &payload,
                        key_space,
                        mix.class(random.percentage()),
                        &mut random,
                        &mut retries,
                    )?;
                }
                barrier.wait();
                retries = 0;
                let mut measured = ClassLatencies::default();
                for _ in 0..worker_operations {
                    let class = mix.class(random.percentage());
                    let started = Instant::now();
                    execute_shared(
                        &handle,
                        &payload,
                        key_space,
                        class,
                        &mut random,
                        &mut retries,
                    )?;
                    measured.record(class, started.elapsed().as_nanos());
                }
                Ok((measured, retries))
            },
        ));
    }
    barrier.wait();
    let started = Instant::now();
    let mut measured = ClassLatencies::default();
    let mut retries = 0_u64;
    for worker in workers {
        let (latencies, worker_retries) = worker
            .join()
            .map_err(|_| "shared benchmark worker panicked".to_owned())??;
        measured.merge(latencies);
        retries = retries.saturating_add(worker_retries);
    }
    let elapsed = started.elapsed();
    runtime.shutdown();
    let segments = count_segments_recursive(&case)?;
    fs::remove_dir_all(&case).map_err(|error| error.to_string())?;
    let mut all = Vec::with_capacity(operations);
    all.extend_from_slice(&measured.get);
    all.extend_from_slice(&measured.set);
    all.extend_from_slice(&measured.overwrite);
    all.extend_from_slice(&measured.delete);
    let mut rows = Vec::with_capacity(5);
    rows.push(row(
        "shared",
        durable,
        operations,
        payload_size,
        concurrency,
        capacity,
        segments,
        elapsed,
        &mut all,
        trial,
        warmup,
        seed,
        "all",
    ));
    for (class, mut samples) in [
        ("get", measured.get),
        ("set", measured.set),
        ("overwrite", measured.overwrite),
        ("delete", measured.delete),
    ] {
        let count = samples.len();
        rows.push(row(
            "shared",
            durable,
            count,
            payload_size,
            concurrency,
            capacity,
            segments,
            elapsed,
            &mut samples,
            trial,
            warmup,
            seed,
            class,
        ));
    }
    for row in &mut rows {
        row.topology = "shared_node_sharded";
        row.backpressure_retries = retries;
        row.shards = shards;
    }
    Ok(rows)
}

fn execute_shared(
    handle: &RuntimeHandle,
    payload: &[u8],
    key_space: usize,
    class: &str,
    random: &mut DeterministicRandom,
    retries: &mut u64,
) -> Result<(), String> {
    let key = format!("key-{}", random.index(key_space));
    loop {
        let result = match class {
            "get" => handle.get(key.as_bytes()).map(|value| {
                std::hint::black_box(value);
            }),
            "set" => handle.set(
                format!("new-{}", random.next()).as_bytes(),
                payload.to_vec(),
                None,
            ),
            "overwrite" => handle.set(key.as_bytes(), payload.to_vec(), None),
            "delete" => handle.delete(key.as_bytes()).map(|_| ()),
            _ => unreachable!("workload class is closed"),
        };
        match result {
            Ok(()) => return Ok(()),
            Err(RuntimeError::Backpressure) => {
                *retries = retries.saturating_add(1);
                thread::yield_now();
            }
            Err(error) => return Err(format!("shared {class}: {error:?}")),
        }
    }
}

fn retry_set(
    handle: &RuntimeHandle,
    key: &[u8],
    value: &[u8],
    retries: &mut u64,
) -> Result<(), String> {
    loop {
        match handle.set(key, value.to_owned(), None) {
            Ok(()) => return Ok(()),
            Err(RuntimeError::Backpressure) => {
                *retries = retries.saturating_add(1);
                thread::yield_now();
            }
            Err(error) => return Err(format!("shared prepare: {error:?}")),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn row(
    benchmark: &'static str,
    durable: bool,
    operations: usize,
    payload_size: usize,
    concurrency: usize,
    capacity: u64,
    segments: usize,
    elapsed: Duration,
    latencies: &mut [u128],
    trial: usize,
    warmup_operations: usize,
    seed: u64,
    operation_class: &'static str,
) -> ResultRow {
    latencies.sort_unstable();
    let elapsed_seconds = elapsed.as_secs_f64();
    ResultRow {
        benchmark,
        profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        durability: if durable { "local_durable" } else { "buffered" },
        operations,
        payload_bytes: payload_size,
        concurrency,
        segment_capacity: capacity,
        segments,
        elapsed_ns: elapsed.as_nanos(),
        throughput_ops_per_second: throughput(operations, elapsed_seconds),
        latency_p50_ns: percentile(latencies, 50),
        latency_p99_ns: percentile(latencies, 99),
        trial,
        warmup_operations,
        seed,
        operation_class,
        copied_bytes: 0,
        reclaimed_bytes: 0,
        write_amplification: 0.0,
        compaction_pause_ns: 0,
        compaction_performed: false,
        backpressure_retries: 0,
        shards: 0,
        topology: "independent_stores",
    }
}

#[allow(clippy::cast_precision_loss)]
fn throughput(operations: usize, elapsed_seconds: f64) -> f64 {
    if elapsed_seconds == 0.0 {
        0.0
    } else {
        // Benchmark dimensions are intentionally many orders of magnitude
        // below f64's exact-integer range.
        operations as f64 / elapsed_seconds
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let index = (values.len() - 1).saturating_mul(percentile) / 100;
    values[index]
}

fn config(directory: PathBuf, capacity: u64, durable: bool) -> StoreConfig {
    StoreConfig {
        directory,
        namespace: Namespace::new("benchmark").expect("static namespace is valid"),
        segment_capacity: capacity,
        durability: if durable {
            Durability::LocalDurable
        } else {
            Durability::Buffered
        },
    }
}

fn temporary_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "punah-bench-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn case_directory(root: &Path, kind: &str) -> PathBuf {
    root.join(format!(
        "{kind}-{}",
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn count_segments_recursive(directory: &Path) -> Result<usize, String> {
    let mut count = 0;
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.is_dir() {
            count += count_segments_recursive(&path)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "punah")
        {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_random_repeats() {
        let mut left = DeterministicRandom::new(42);
        let mut right = DeterministicRandom::new(42);
        assert_eq!(
            (0..20).map(|_| left.next()).collect::<Vec<_>>(),
            (0..20).map(|_| right.next()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn tiny_mixed_workload_emits_all_classes() {
        let root = temporary_root();
        fs::create_dir(&root).expect("create temporary benchmark root");
        let rows = mixed_benchmark(
            &root,
            100,
            8,
            1,
            32 * 1024,
            false,
            1,
            3,
            42,
            WorkloadMix::default(),
        )
        .expect("mixed benchmark");
        fs::remove_dir_all(root).expect("remove temporary benchmark root");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].operation_class, "all");
        assert!(rows.iter().all(|row| row.shards == 0));
        assert_eq!(
            rows.iter().skip(1).map(|row| row.operations).sum::<usize>(),
            100
        );
    }

    #[test]
    fn tiny_shared_workload_uses_sharded_topology() {
        let root = temporary_root();
        fs::create_dir(&root).expect("create temporary benchmark root");
        let rows = shared_benchmark(
            &root,
            100,
            8,
            3,
            2,
            32 * 1024,
            false,
            1,
            3,
            42,
            WorkloadMix::default(),
        )
        .expect("shared benchmark");
        fs::remove_dir_all(root).expect("remove temporary benchmark root");
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|row| row.topology == "shared_node_sharded"));
        assert!(rows.iter().all(|row| row.shards == 2));
        assert_eq!(
            rows.iter().skip(1).map(|row| row.operations).sum::<usize>(),
            100
        );
    }
}
