#![forbid(unsafe_code)]

use std::fs;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use punah_config::{Config, StorageDurability};
use punah_network::{FrameBuffer, FrameWriteMode, write_frame};
use punah_protocol::{Frame, FrameRef, Opcode};
use punah_server::Node;

const CSV_HEADER: &str = "mode,payload_bytes,clients,pipeline,key_space,operations,trial,elapsed_ns,ops_per_second,p50_ns,p95_ns,p99_ns,p999_ns";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn main() -> Result<(), String> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let config = BenchConfig::parse(&arguments)?;
    println!("{CSV_HEADER}");
    for trial in 1..=config.trials {
        for &payload in &config.payloads {
            for &mode in config.write_modes(trial) {
                println!("{}", run_case(&config, payload, mode, trial)?.csv());
            }
        }
    }
    Ok(())
}

struct BenchConfig {
    operations: usize,
    payloads: Vec<usize>,
    clients: usize,
    pipeline: usize,
    key_space: usize,
    trials: usize,
    mode: ModeSelection,
}

impl BenchConfig {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        if !(6..=7).contains(&arguments.len()) {
            return Err(
                "usage: tcp_output_compare OPERATIONS PAYLOADS CLIENTS PIPELINE KEY_SPACE TRIALS [MODE=both]"
                    .to_owned(),
            );
        }
        Ok(Self {
            operations: nonzero(&arguments[0], "OPERATIONS")?,
            payloads: parse_payloads(&arguments[1])?,
            clients: nonzero(&arguments[2], "CLIENTS")?,
            pipeline: nonzero(&arguments[3], "PIPELINE")?,
            key_space: nonzero(&arguments[4], "KEY_SPACE")?,
            trials: nonzero(&arguments[5], "TRIALS")?,
            mode: arguments
                .get(6)
                .map_or(Ok(ModeSelection::Both), |mode| ModeSelection::parse(mode))?,
        })
    }

    fn write_modes(&self, trial: usize) -> &'static [FrameWriteMode] {
        const CONTIGUOUS: &[FrameWriteMode] = &[FrameWriteMode::Contiguous];
        const VECTORED: &[FrameWriteMode] = &[FrameWriteMode::Vectored];
        const CONTIGUOUS_FIRST: &[FrameWriteMode] =
            &[FrameWriteMode::Contiguous, FrameWriteMode::Vectored];
        const VECTORED_FIRST: &[FrameWriteMode] =
            &[FrameWriteMode::Vectored, FrameWriteMode::Contiguous];
        match self.mode {
            ModeSelection::Contiguous => CONTIGUOUS,
            ModeSelection::Vectored => VECTORED,
            ModeSelection::Both if trial.is_multiple_of(2) => VECTORED_FIRST,
            ModeSelection::Both => CONTIGUOUS_FIRST,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModeSelection {
    Both,
    Contiguous,
    Vectored,
}

impl ModeSelection {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "both" => Ok(Self::Both),
            "contiguous" => Ok(Self::Contiguous),
            "vectored" => Ok(Self::Vectored),
            _ => Err("MODE must be both, contiguous, or vectored".to_owned()),
        }
    }
}

fn nonzero(value: &str, name: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    (value > 0)
        .then_some(value)
        .ok_or_else(|| format!("{name} must be nonzero"))
}

fn parse_payloads(value: &str) -> Result<Vec<usize>, String> {
    let payloads: Result<Vec<_>, _> = value
        .split(',')
        .map(|part| nonzero(part, "PAYLOADS"))
        .collect();
    let payloads = payloads?;
    (!payloads.is_empty())
        .then_some(payloads)
        .ok_or_else(|| "PAYLOADS must be nonempty".to_owned())
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "punah-tcp-output-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Row {
    mode: FrameWriteMode,
    payload: usize,
    clients: usize,
    pipeline: usize,
    key_space: usize,
    operations: usize,
    trial: usize,
    elapsed_ns: u128,
    ops_per_second: f64,
    percentiles: [u128; 4],
}

impl Row {
    fn csv(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{:.2},{},{},{},{}",
            mode_name(self.mode),
            self.payload,
            self.clients,
            self.pipeline,
            self.key_space,
            self.operations,
            self.trial,
            self.elapsed_ns,
            self.ops_per_second,
            self.percentiles[0],
            self.percentiles[1],
            self.percentiles[2],
            self.percentiles[3]
        )
    }
}

fn mode_name(mode: FrameWriteMode) -> &'static str {
    match mode {
        FrameWriteMode::Auto => "auto",
        FrameWriteMode::Contiguous => "contiguous",
        FrameWriteMode::Vectored => "vectored",
    }
}

fn run_case(
    config: &BenchConfig,
    payload: usize,
    mode: FrameWriteMode,
    trial: usize,
) -> Result<Row, String> {
    let directory = TempDir::new()?;
    let mut node_config = Config::default();
    "127.0.0.1:0".clone_into(&mut node_config.tcp.listen_address);
    node_config.storage.directory.clone_from(&directory.0);
    node_config.storage.durability = StorageDurability::Buffered;
    node_config.engine.shard_count = 1;
    node_config.engine.queue_capacity = 256;
    let node = Node::bind_with_write_mode(node_config, mode)
        .map_err(|error| format!("bind node: {error}"))?;
    let address = node.local_addr().map_err(|error| error.to_string())?;
    let stop = node.stop_handle();
    let server = thread::spawn(move || node.serve());
    preload(address, payload, config.key_space)?;

    let barrier = Arc::new(Barrier::new(config.clients + 1));
    let mut workers = Vec::with_capacity(config.clients);
    let clients = config.clients;
    for client in 0..config.clients {
        let barrier = Arc::clone(&barrier);
        let count = config.operations / config.clients
            + usize::from(client < config.operations % config.clients);
        let pipeline = config.pipeline;
        let key_space = config.key_space;
        workers.push(thread::spawn(move || {
            run_client(
                address,
                barrier.as_ref(),
                count,
                client,
                clients,
                pipeline,
                key_space,
                payload,
            )
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(config.operations);
    for worker in workers {
        latencies.extend(worker.join().map_err(|_| "client panicked".to_owned())??);
    }
    let elapsed = started.elapsed();
    stop.stop();
    server
        .join()
        .map_err(|_| "server panicked".to_owned())?
        .map_err(|error| error.to_string())?;
    latencies.sort_unstable();
    let operations = u32::try_from(config.operations).map_err(|_| "OPERATIONS exceeds u32")?;
    Ok(Row {
        mode,
        payload,
        clients: config.clients,
        pipeline: config.pipeline,
        key_space: config.key_space,
        operations: config.operations,
        trial,
        elapsed_ns: elapsed.as_nanos(),
        ops_per_second: f64::from(operations) / elapsed.as_secs_f64(),
        percentiles: [
            percentile(&latencies, 500),
            percentile(&latencies, 950),
            percentile(&latencies, 990),
            percentile(&latencies, 999),
        ],
    })
}

#[allow(clippy::too_many_arguments)]
fn run_client(
    address: std::net::SocketAddr,
    barrier: &Barrier,
    count: usize,
    client: usize,
    clients: usize,
    pipeline: usize,
    key_space: usize,
    payload: usize,
) -> Result<Vec<u128>, String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_nodelay(true)
        .map_err(|error| error.to_string())?;
    let mut responses = FrameBuffer::new();
    let mut latencies = Vec::with_capacity(count);
    barrier.wait();
    let mut index = 0;
    while index < count {
        let batch = pipeline.min(count - index);
        let mut expected = Vec::with_capacity(batch);
        for offset in 0..batch {
            let global = (index + offset) * clients + client;
            let key = key(global % key_space);
            let started = Instant::now();
            write_frame(
                &mut stream,
                &Frame::new(Opcode::Get, key.clone(), Vec::new()),
            )
            .map_err(|error| error.to_string())?;
            expected.push((started, key));
        }
        for (started, key) in expected {
            let response = responses
                .read(&mut stream)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "server closed connection".to_owned())?;
            validate_get(response, &key, payload)?;
            latencies.push(started.elapsed().as_nanos());
        }
        index += batch;
    }
    Ok(latencies)
}

fn preload(address: std::net::SocketAddr, payload: usize, keys: usize) -> Result<(), String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    let mut responses = FrameBuffer::new();
    let value = vec![0x5a; payload];
    for index in 0..keys {
        let key = key(index);
        write_frame(
            &mut stream,
            &Frame::new(Opcode::Set, key.clone(), value.clone()),
        )
        .map_err(|error| error.to_string())?;
        let response = responses
            .read(&mut stream)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "server closed during preload".to_owned())?;
        if response.opcode != Opcode::Set || response.key != key || !response.value.is_empty() {
            return Err("invalid preload acknowledgement".to_owned());
        }
    }
    Ok(())
}

fn validate_get(response: FrameRef<'_>, key: &[u8], payload: usize) -> Result<(), String> {
    if response.opcode != Opcode::Get || response.key != key || response.value.len() != payload {
        return Err("GET response metadata mismatch".to_owned());
    }
    if response.value.iter().any(|byte| *byte != 0x5a) {
        return Err("GET response payload mismatch".to_owned());
    }
    Ok(())
}

fn key(index: usize) -> Vec<u8> {
    format!("output-{index:08}").into_bytes()
}

fn percentile(sorted: &[u128], permille: usize) -> u128 {
    let rank = sorted.len().saturating_mul(permille).saturating_add(999) / 1_000;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiny_ab_validates_both_output_modes() {
        let config = BenchConfig {
            operations: 20,
            payloads: vec![256],
            clients: 2,
            pipeline: 2,
            key_space: 4,
            trials: 1,
            mode: ModeSelection::Both,
        };
        for mode in [FrameWriteMode::Contiguous, FrameWriteMode::Vectored] {
            let row = run_case(&config, 256, mode, 1).expect("case");
            assert_eq!(row.csv().split(',').count(), CSV_HEADER.split(',').count());
        }
    }

    #[test]
    fn parser_selects_one_mode_for_external_counters() {
        let arguments = ["20", "256", "2", "2", "4", "1", "vectored"].map(str::to_owned);
        let config = BenchConfig::parse(&arguments).expect("configuration");
        assert_eq!(config.write_modes(1), &[FrameWriteMode::Vectored]);

        let mut invalid = arguments;
        invalid[6] = "unknown".to_owned();
        assert!(BenchConfig::parse(&invalid).is_err());
    }
}
