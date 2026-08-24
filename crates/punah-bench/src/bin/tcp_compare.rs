#![forbid(unsafe_code)]

use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use punah_network::{FrameBuffer, write_frame};
use punah_protocol::{Frame, FrameRef, Opcode};

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
    if !(6..=9).contains(&args.len()) {
        return Err(
            "usage: tcp_compare ADDRESS get|set|mixed OPS PAYLOAD CLIENTS [PIPELINE] [KEY_SPACE] [PRELOAD_KEYS]".into(),
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
    let preload_keys: usize = args.get(8).map_or_else(
        || {
            Ok::<_, std::num::ParseIntError>(match workload {
                Workload::Get | Workload::Mixed => key_space,
                Workload::Set => key_space.min(DEFAULT_KEY_SPACE),
            })
        },
        |value| value.parse(),
    )?;
    if operations == 0 || payload_bytes == 0 || clients == 0 || pipeline == 0 || key_space == 0 {
        return Err("OPS, PAYLOAD, CLIENTS, PIPELINE, and KEY_SPACE must be nonzero".into());
    }
    if preload_keys > key_space {
        return Err("PRELOAD_KEYS cannot exceed KEY_SPACE".into());
    }

    preload(&address, payload_bytes, preload_keys)?;
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
            let mut responses = FrameBuffer::new();
            barrier.wait();
            let mut latencies = Vec::with_capacity(count);
            let mut index = 0;
            while index < count {
                let batch = pipeline.min(count - index);
                let mut expected = Vec::with_capacity(batch);
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
                    let started = Instant::now();
                    let request = Frame::new(opcode, key(global % key_space), request_value);
                    write_frame(&mut stream, &request).map_err(|error| error.to_string())?;
                    expected.push((started, opcode, request.key));
                }
                for (started, opcode, key) in expected {
                    let response = responses
                        .read(&mut stream)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| "server closed connection".to_owned())?;
                    validate_response(response, opcode, &key, payload_bytes)?;
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
        "workload,payload_bytes,clients,pipeline,key_space,preload_keys,operations,elapsed_ns,ops_per_second,p50_ns,p99_ns,p999_ns"
    );
    println!(
        "{:?},{payload_bytes},{clients},{pipeline},{key_space},{preload_keys},{operations},{},{:.2},{},{},{}",
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
    let mut responses = FrameBuffer::new();
    for index in 0..key_space {
        write_frame(
            &mut stream,
            &Frame::new(Opcode::Set, key(index), value.clone()),
        )?;
        let response = responses
            .read(&mut stream)?
            .ok_or("server closed during preload")?;
        validate_response(response, Opcode::Set, &key(index), payload_bytes)
            .map_err(|error| format!("preload {index}: {error}"))?;
    }
    Ok(())
}

fn validate_response(
    response: FrameRef<'_>,
    expected_opcode: Opcode,
    expected_key: &[u8],
    payload_bytes: usize,
) -> Result<(), String> {
    if response.opcode == Opcode::Error {
        return Err(format!(
            "server error: {}",
            String::from_utf8_lossy(response.value)
        ));
    }
    if response.opcode != expected_opcode {
        return Err(format!(
            "expected {expected_opcode:?} response, received {:?}",
            response.opcode
        ));
    }
    if response.key != expected_key {
        return Err("response key does not match request".to_owned());
    }
    match expected_opcode {
        Opcode::Get if response.value.len() != payload_bytes => Err(format!(
            "GET expected {payload_bytes} value bytes, received {}",
            response.value.len()
        )),
        Opcode::Get if response.value.iter().any(|byte| *byte != 0x5a) => {
            Err("GET response payload does not match the seeded value".to_owned())
        }
        Opcode::Set if !response.value.is_empty() => {
            Err("SET acknowledgement contains an unexpected value".to_owned())
        }
        _ => Ok(()),
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("compare-{index:08}").into_bytes()
}

fn percentile(sorted: &[u128], permille: usize) -> u128 {
    let index = sorted.len().saturating_mul(permille).saturating_add(999) / 1_000;
    sorted[index.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_rejects_misses_and_mismatched_responses() {
        let key = b"compare-00000001";
        assert!(
            validate_response(
                Frame::new(Opcode::Get, key.to_vec(), vec![0x5a; 4]).as_ref(),
                Opcode::Get,
                key,
                4,
            )
            .is_ok()
        );
        assert!(
            validate_response(
                Frame::new(Opcode::Get, key.to_vec(), Vec::new()).as_ref(),
                Opcode::Get,
                key,
                4,
            )
            .is_err()
        );
        assert!(
            validate_response(
                Frame::new(Opcode::Get, key.to_vec(), vec![0; 4]).as_ref(),
                Opcode::Get,
                key,
                4,
            )
            .is_err()
        );
        assert!(
            validate_response(
                Frame::new(Opcode::Set, b"other".to_vec(), Vec::new()).as_ref(),
                Opcode::Set,
                key,
                4,
            )
            .is_err()
        );
    }
}
