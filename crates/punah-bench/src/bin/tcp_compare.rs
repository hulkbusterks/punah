#![forbid(unsafe_code)]

use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use punah_network::{read_frame, write_frame};
use punah_protocol::{Frame, Opcode};

const DEFAULT_KEY_SPACE: usize = 1_024;

#[derive(Clone, Copy, Debug)]
enum Workload {
    Get,
    Set,
    Mixed,
}

#[allow(clippy::too_many_lines)] // Keep the one-file comparison client easy to copy to Redgo.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if !(6..=8).contains(&args.len()) {
        return Err(
            "usage: tcp_compare ADDRESS get|set|mixed OPS PAYLOAD CLIENTS [PIPELINE] [KEY_SPACE]"
                .into(),
        );
    }
    let address = args[1].clone();
    let workload = match args[2].as_str() {
        "get" => Workload::Get,
        "set" => Workload::Set,
        "mixed" => Workload::Mixed,
        _ => return Err("workload must be get, set, or mixed".into()),
    };
    let operations: usize = args[3].parse()?;
    let payload_bytes: usize = args[4].parse()?;
    let clients: usize = args[5].parse()?;
    let pipeline: usize = args.get(6).map_or(Ok(1), |value| value.parse())?;
    let key_space: usize = args
        .get(7)
        .map_or(Ok(DEFAULT_KEY_SPACE), |value| value.parse())?;
    if operations == 0 || payload_bytes == 0 || clients == 0 || pipeline == 0 || key_space == 0 {
        return Err("OPS, PAYLOAD, CLIENTS, PIPELINE, and KEY_SPACE must be nonzero".into());
    }

    preload(&address, payload_bytes, key_space.min(DEFAULT_KEY_SPACE))?;
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut workers = Vec::with_capacity(clients);
    for client in 0..clients {
        let address = address.clone();
        let barrier = Arc::clone(&barrier);
        let count = operations / clients + usize::from(client < operations % clients);
        workers.push(thread::spawn(move || -> Result<Vec<u128>, String> {
            let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
            stream
                .set_nodelay(true)
                .map_err(|error| error.to_string())?;
            let value = vec![0x5a; payload_bytes];
            barrier.wait();
            let mut latencies = Vec::with_capacity(count);
            let mut index = 0;
            while index < count {
                let batch = pipeline.min(count - index);
                let mut starts = Vec::with_capacity(batch);
                for offset in 0..batch {
                    let global = (index + offset) * clients + client;
                    let opcode = match workload {
                        Workload::Set => Opcode::Set,
                        Workload::Mixed if global.is_multiple_of(5) => Opcode::Set,
                        Workload::Get | Workload::Mixed => Opcode::Get,
                    };
                    let request_value = if opcode == Opcode::Set {
                        value.clone()
                    } else {
                        Vec::new()
                    };
                    starts.push(Instant::now());
                    write_frame(
                        &mut stream,
                        &Frame::new(opcode, key(global % key_space), request_value),
                    )
                    .map_err(|error| error.to_string())?;
                }
                for started in starts {
                    let response = read_frame(&mut stream)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| "server closed connection".to_owned())?;
                    if response.opcode == Opcode::Error {
                        return Err(format!(
                            "server error: {}",
                            String::from_utf8_lossy(&response.value)
                        ));
                    }
                    latencies.push(started.elapsed().as_nanos());
                }
                index += batch;
            }
            Ok(latencies)
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(operations);
    for worker in workers {
        latencies.extend(worker.join().map_err(|_| "client panicked")??);
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    println!(
        "workload,payload_bytes,clients,pipeline,key_space,operations,elapsed_ns,ops_per_second,p50_ns,p99_ns,p999_ns"
    );
    println!(
        "{:?},{payload_bytes},{clients},{pipeline},{key_space},{operations},{},{:.2},{},{},{}",
        workload,
        elapsed.as_nanos(),
        f64::from(u32::try_from(operations).map_err(|_| "OPS exceeds u32")?)
            / elapsed.as_secs_f64(),
        percentile(&latencies, 500),
        percentile(&latencies, 990),
        percentile(&latencies, 999),
    );
    Ok(())
}

fn preload(
    address: &str,
    payload_bytes: usize,
    key_space: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    let value = vec![0x5a; payload_bytes];
    for index in 0..key_space {
        write_frame(
            &mut stream,
            &Frame::new(Opcode::Set, key(index), value.clone()),
        )?;
        let response = read_frame(&mut stream)?.ok_or("server closed during preload")?;
        if response.opcode == Opcode::Error {
            return Err(format!(
                "preload error: {}",
                String::from_utf8_lossy(&response.value)
            )
            .into());
        }
    }
    Ok(())
}

fn key(index: usize) -> Vec<u8> {
    format!("compare-{index:08}").into_bytes()
}

fn percentile(sorted: &[u128], permille: usize) -> u128 {
    let index = sorted.len().saturating_mul(permille).saturating_add(999) / 1_000;
    sorted[index.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}
