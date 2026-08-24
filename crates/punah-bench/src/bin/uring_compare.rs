#![forbid(unsafe_code)]

#[cfg(not(target_os = "linux"))]
fn main() -> Result<(), String> {
    Err("uring_compare requires Linux with io_uring support".to_owned())
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), String> {
    linux::run()
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use punah_device::{BlockDevice, FileDevice, UringDevice};
    use punah_engine::SegmentWriter;
    use punah_record::{Record, RecordType, encoded_len};
    use punah_types::{Durability, Namespace, SegmentId};

    const CSV_HEADER: &str = "backend,operations,payload_bytes,batch_size,queue_depth,groups,encoded_bytes,elapsed_ns,ops_per_second,group_p50_ns,group_p95_ns,group_p99_ns,group_p999_ns,trial";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    pub(super) fn run() -> Result<(), String> {
        let arguments: Vec<_> = std::env::args().skip(1).collect();
        let config = Config::parse(&arguments)?;
        println!("{CSV_HEADER}");
        for trial in 1..=config.trials {
            let backends = if trial % 2 == 0 {
                [Backend::Uring, Backend::File]
            } else {
                [Backend::File, Backend::Uring]
            };
            let mut rows = Vec::new();
            for backend in backends {
                rows.push(run_case(&config, backend, trial)?);
            }
            rows.sort_by_key(|row| row.backend);
            for row in rows {
                println!("{}", row.csv_line());
            }
        }
        Ok(())
    }

    struct Config {
        operations: usize,
        payload_bytes: usize,
        batch_size: usize,
        queue_depth: usize,
        trials: usize,
    }

    impl Config {
        fn parse(arguments: &[String]) -> Result<Self, String> {
            if !(4..=5).contains(&arguments.len()) {
                return Err(usage());
            }
            let operations = nonzero(&arguments[0], "OPERATIONS")?;
            u32::try_from(operations)
                .map_err(|_| "OPERATIONS cannot exceed u32::MAX".to_owned())?;
            let payload_bytes = nonzero(&arguments[1], "PAYLOAD_BYTES")?;
            let batch_size = nonzero(&arguments[2], "BATCH_SIZE")?;
            let queue_depth = nonzero(&arguments[3], "QUEUE_DEPTH")?;
            let trials = arguments
                .get(4)
                .map_or(Ok(5), |value| nonzero(value, "TRIALS"))?;
            if batch_size > 64 {
                return Err("BATCH_SIZE cannot exceed 64".to_owned());
            }
            Ok(Self {
                operations,
                payload_bytes,
                batch_size,
                queue_depth,
                trials,
            })
        }
    }

    fn nonzero(value: &str, name: &str) -> Result<usize, String> {
        let value = value
            .parse::<usize>()
            .map_err(|error| format!("invalid {name}: {error}"))?;
        if value == 0 {
            return Err(format!("{name} must be nonzero"));
        }
        Ok(value)
    }

    fn usage() -> String {
        "usage: uring_compare OPERATIONS PAYLOAD_BYTES BATCH_SIZE QUEUE_DEPTH [TRIALS=5]".to_owned()
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum Backend {
        File,
        Uring,
    }

    impl Backend {
        const fn name(self) -> &'static str {
            match self {
                Self::File => "file",
                Self::Uring => "io_uring",
            }
        }
    }

    struct TemporaryFile(PathBuf);

    impl TemporaryFile {
        fn new(backend: Backend, trial: usize) -> Self {
            Self(std::env::temp_dir().join(format!(
                "punah-uring-compare-{}-{trial}-{}-{}",
                std::process::id(),
                backend.name(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TemporaryFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    struct Row {
        backend: Backend,
        operations: usize,
        payload_bytes: usize,
        batch_size: usize,
        queue_depth: usize,
        groups: usize,
        encoded_bytes: u64,
        elapsed_ns: u128,
        operations_per_second: f64,
        percentiles: [u128; 4],
        trial: usize,
    }

    impl Row {
        fn csv_line(&self) -> String {
            format!(
                "{},{},{},{},{},{},{},{},{:.2},{},{},{},{},{}",
                self.backend.name(),
                self.operations,
                self.payload_bytes,
                self.batch_size,
                self.queue_depth,
                self.groups,
                self.encoded_bytes,
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

    fn run_case(config: &Config, backend: Backend, trial: usize) -> Result<Row, String> {
        let records = records(config.operations, config.payload_bytes)?;
        let encoded_bytes = records.iter().try_fold(0_u64, |total, record| {
            let length = u64::try_from(encoded_len(record).map_err(|error| format!("{error:?}"))?)
                .map_err(|_| "encoded length overflow".to_owned())?;
            total
                .checked_add(length)
                .ok_or_else(|| "segment capacity overflow".to_owned())
        })?;
        let capacity = encoded_bytes.saturating_add(4096);
        let path = TemporaryFile::new(backend, trial);
        let (groups, latencies, elapsed) = match backend {
            Backend::File => {
                let device = FileDevice::create(&path.0, capacity, config.queue_depth)
                    .map_err(|error| error.to_string())?;
                run_writer(device, capacity, records, config.batch_size)?
            }
            Backend::Uring => {
                let device = UringDevice::create(&path.0, capacity, config.queue_depth)
                    .map_err(|error| error.to_string())?;
                run_writer(device, capacity, records, config.batch_size)?
            }
        };
        let mut latencies = latencies;
        latencies.sort_unstable();
        let operation_count = u32::try_from(config.operations).expect("configuration validated");
        Ok(Row {
            backend,
            operations: config.operations,
            payload_bytes: config.payload_bytes,
            batch_size: config.batch_size,
            queue_depth: config.queue_depth,
            groups,
            encoded_bytes,
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

    fn run_writer<D: BlockDevice>(
        device: D,
        capacity: u64,
        records: Vec<Record>,
        batch_size: usize,
    ) -> Result<(usize, Vec<u128>, std::time::Duration), String> {
        let mut writer = SegmentWriter::new(device, SegmentId(1), 1, capacity);
        let mut pending: VecDeque<_> = records.into();
        let mut groups = 0_usize;
        let mut latencies = Vec::new();
        let started = Instant::now();
        while !pending.is_empty() {
            let count = batch_size.min(pending.len());
            let group: Vec<_> = pending.drain(..count).collect();
            let group_started = Instant::now();
            let completions = writer
                .append_cohort(group, Durability::LocalDurable)
                .map_err(|error| format!("append: {:?}", error.error))?;
            if completions.len() != count {
                return Err("completion count mismatch".to_owned());
            }
            latencies.push(group_started.elapsed().as_nanos());
            groups += 1;
        }
        Ok((groups, latencies, started.elapsed()))
    }

    fn records(count: usize, payload_bytes: usize) -> Result<Vec<Record>, String> {
        let namespace = Namespace::new("benchmark").map_err(|error| format!("{error:?}"))?;
        (0..count)
            .map(|index| {
                Ok(Record {
                    record_type: RecordType::Artifact,
                    flags: 0,
                    namespace: namespace.clone(),
                    key: format!("uring-{index:08}").into_bytes(),
                    sequence: u64::try_from(index + 1)
                        .map_err(|_| "sequence overflow".to_owned())?,
                    created_at_unix_ns: 0,
                    expires_at_unix_ns: None,
                    operation_hash: [0; 32],
                    content_hash: [1; 32],
                    dependencies: Vec::new(),
                    payload: vec![0x5a; payload_bytes],
                })
            })
            .collect()
    }

    fn percentile(sorted: &[u128], permille: usize) -> u128 {
        let rank = sorted.len().saturating_mul(permille).saturating_add(999) / 1000;
        sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
    }

    use std::collections::VecDeque;
}
