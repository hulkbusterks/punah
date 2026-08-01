#![forbid(unsafe_code)]

//! Dependency-free argument parsing and CSV formatting for the benchmark runner.

use std::fmt::Write as _;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Write,
    Recovery,
    Mixed,
    Shared,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkloadMix {
    pub get: u8,
    pub set: u8,
    pub overwrite: u8,
    pub delete: u8,
}

impl Default for WorkloadMix {
    fn default() -> Self {
        Self {
            get: 60,
            set: 25,
            overwrite: 10,
            delete: 5,
        }
    }
}

impl WorkloadMix {
    /// Parses four comma-separated percentages totalling 100.
    ///
    /// # Errors
    /// Returns an error when the shape, values, or total is invalid.
    pub fn parse(input: &str) -> Result<Self, String> {
        let values = parse_list::<u8>(input, "--mix")?;
        if values.len() != 4 {
            return Err("--mix requires GET,SET,OVERWRITE,DELETE".to_owned());
        }
        if values.iter().map(|value| u16::from(*value)).sum::<u16>() != 100 {
            return Err("--mix percentages must total 100".to_owned());
        }
        Ok(Self {
            get: values[0],
            set: values[1],
            overwrite: values[2],
            delete: values[3],
        })
    }

    #[must_use]
    pub fn class(self, sample: u8) -> &'static str {
        if sample < self.get {
            "get"
        } else if sample < self.get + self.set {
            "set"
        } else if sample < self.get + self.set + self.overwrite {
            "overwrite"
        } else {
            "delete"
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Args {
    pub mode: Mode,
    pub operations: Vec<usize>,
    pub payload_sizes: Vec<usize>,
    pub concurrency: Vec<usize>,
    pub segment_capacities: Vec<u64>,
    pub durable: bool,
    pub root: Option<PathBuf>,
    pub trials: usize,
    pub warmup_operations: usize,
    pub seed: u64,
    pub mix: WorkloadMix,
    pub shards: Vec<usize>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            mode: Mode::All,
            operations: vec![1_000, 10_000],
            payload_sizes: vec![64, 1_024],
            concurrency: vec![1, 2, 4],
            segment_capacities: vec![1 << 20],
            durable: false,
            root: None,
            trials: 1,
            warmup_operations: 0,
            seed: 0x0050_554e_4148,
            mix: WorkloadMix::default(),
            shards: vec![1, 2, 4],
        }
    }
}

/// Parses benchmark command-line arguments.
///
/// # Errors
/// Returns an error for an unknown option or malformed value.
pub fn parse_args<I, S>(arguments: I) -> Result<Args, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut result = Args::default();
    let mut arguments = arguments.into_iter().map(Into::into);
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--mode" => {
                result.mode = match value(&mut arguments, &flag)?.as_str() {
                    "write" => Mode::Write,
                    "recovery" => Mode::Recovery,
                    "mixed" => Mode::Mixed,
                    "shared" => Mode::Shared,
                    "all" => Mode::All,
                    other => return Err(format!("invalid mode: {other}")),
                }
            }
            "--operations" => {
                result.operations = parse_list(&value(&mut arguments, &flag)?, &flag)?;
            }
            "--payload-sizes" => {
                result.payload_sizes = parse_list(&value(&mut arguments, &flag)?, &flag)?;
            }
            "--concurrency" => {
                result.concurrency = parse_list(&value(&mut arguments, &flag)?, &flag)?;
            }
            "--segment-capacities" => {
                result.segment_capacities = parse_list(&value(&mut arguments, &flag)?, &flag)?;
            }
            "--trials" => result.trials = parse_positive(&value(&mut arguments, &flag)?, &flag)?,
            "--warmup" => {
                result.warmup_operations = value(&mut arguments, &flag)?
                    .parse()
                    .map_err(|_| "invalid value for --warmup".to_owned())?;
            }
            "--seed" => {
                result.seed = value(&mut arguments, &flag)?
                    .parse()
                    .map_err(|_| "invalid value for --seed".to_owned())?;
            }
            "--mix" => result.mix = WorkloadMix::parse(&value(&mut arguments, &flag)?)?,
            "--shards" => result.shards = parse_list(&value(&mut arguments, &flag)?, &flag)?,
            "--durable" => result.durable = true,
            "--root" => result.root = Some(PathBuf::from(value(&mut arguments, &flag)?)),
            "--help" | "-h" => return Err(usage().to_owned()),
            other => return Err(format!("unknown argument: {other}\n{}", usage())),
        }
    }
    Ok(result)
}

fn value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {flag}"))
}
fn parse_positive<T>(input: &str, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr + Default + PartialEq,
{
    let parsed = input
        .parse()
        .map_err(|_| format!("invalid value for {flag}: {input}"))?;
    if parsed == T::default() {
        Err(format!("{flag} must be greater than zero"))
    } else {
        Ok(parsed)
    }
}
fn parse_list<T>(input: &str, flag: &str) -> Result<Vec<T>, String>
where
    T: std::str::FromStr + Default + PartialEq,
{
    let parsed: Vec<T> = input
        .split(',')
        .map(|item| {
            item.parse()
                .map_err(|_| format!("invalid value in {flag}: {item}"))
        })
        .collect::<Result<_, _>>()?;
    if parsed.is_empty() || parsed.iter().any(|item| *item == T::default()) {
        Err(format!("{flag} values must be greater than zero"))
    } else {
        Ok(parsed)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultRow {
    pub benchmark: &'static str,
    pub profile: &'static str,
    pub durability: &'static str,
    pub operations: usize,
    pub payload_bytes: usize,
    pub concurrency: usize,
    pub segment_capacity: u64,
    pub segments: usize,
    pub elapsed_ns: u128,
    pub throughput_ops_per_second: f64,
    pub latency_p50_ns: u128,
    pub latency_p99_ns: u128,
    pub trial: usize,
    pub warmup_operations: usize,
    pub seed: u64,
    pub operation_class: &'static str,
    pub copied_bytes: u64,
    pub reclaimed_bytes: u64,
    pub write_amplification: f64,
    pub compaction_pause_ns: u128,
    pub compaction_performed: bool,
    pub backpressure_retries: u64,
    /// Runtime shard count; zero means the topology has no shared-node shards.
    pub shards: usize,
    pub topology: &'static str,
}

pub const CSV_HEADER: &str = "benchmark,profile,durability,operations,payload_bytes,concurrency,segment_capacity,segments,elapsed_ns,throughput_ops_per_second,latency_p50_ns,latency_p99_ns,trial,warmup_operations,seed,operation_class,copied_bytes,reclaimed_bytes,write_amplification,compaction_pause_ns,compaction_performed,backpressure_retries,shards,topology";

#[must_use]
pub fn format_row(row: &ResultRow) -> String {
    let mut output = String::new();
    write!(
        output,
        "{},{},{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{}",
        row.benchmark,
        row.profile,
        row.durability,
        row.operations,
        row.payload_bytes,
        row.concurrency,
        row.segment_capacity,
        row.segments,
        row.elapsed_ns,
        row.throughput_ops_per_second,
        row.latency_p50_ns,
        row.latency_p99_ns,
        row.trial,
        row.warmup_operations,
        row.seed,
        row.operation_class,
        row.copied_bytes,
        row.reclaimed_bytes,
        row.write_amplification,
        row.compaction_pause_ns,
        row.compaction_performed,
        row.backpressure_retries,
        row.shards,
        row.topology
    )
    .expect("string write");
    output
}
#[must_use]
pub fn usage() -> &'static str {
    "usage: punah-bench [--mode write|recovery|mixed|shared|all] [--operations N,N] [--payload-sizes N,N] [--concurrency N,N] [--shards N,N] [--segment-capacities N,N] [--trials N] [--warmup N] [--seed N] [--mix GET,SET,OVERWRITE,DELETE] [--durable] [--root PATH]"
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_quality_options() {
        let a = parse_args([
            "--mode",
            "mixed",
            "--trials",
            "3",
            "--warmup",
            "11",
            "--seed",
            "42",
            "--mix",
            "60,25,10,5",
        ])
        .unwrap();
        assert_eq!(a.mode, Mode::Mixed);
        assert_eq!(a.trials, 3);
        assert_eq!(a.warmup_operations, 11);
        assert_eq!(a.seed, 42);
        assert_eq!(a.mix, WorkloadMix::default());
    }

    #[test]
    fn parses_shared_mode_and_shards() {
        let args = parse_args(["--mode", "shared", "--shards", "1,2,8"]).unwrap();
        assert_eq!(args.mode, Mode::Shared);
        assert_eq!(args.shards, [1, 2, 8]);
    }
    #[test]
    fn rejects_invalid_mix() {
        assert!(parse_args(["--mix", "50,20,10,10"]).is_err());
        assert!(parse_args(["--mix", "50,50"]).is_err());
    }
    #[test]
    fn deterministic_class_boundaries() {
        let m = WorkloadMix::default();
        assert_eq!(m.class(0), "get");
        assert_eq!(m.class(59), "get");
        assert_eq!(m.class(60), "set");
        assert_eq!(m.class(85), "overwrite");
        assert_eq!(m.class(95), "delete");
    }
    #[test]
    fn row_shape_matches_header() {
        let r = ResultRow {
            benchmark: "mixed",
            profile: "debug",
            durability: "buffered",
            operations: 1,
            payload_bytes: 1,
            concurrency: 1,
            segment_capacity: 1,
            segments: 1,
            elapsed_ns: 1,
            throughput_ops_per_second: 1.0,
            latency_p50_ns: 1,
            latency_p99_ns: 1,
            trial: 1,
            warmup_operations: 0,
            seed: 1,
            operation_class: "all",
            copied_bytes: 0,
            reclaimed_bytes: 0,
            write_amplification: 0.0,
            compaction_pause_ns: 0,
            compaction_performed: false,
            backpressure_retries: 0,
            shards: 0,
            topology: "independent_stores",
        };
        assert_eq!(
            format_row(&r).split(',').count(),
            CSV_HEADER.split(',').count()
        );
    }
}
