use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use punah_config::Config;
use punah_identity::{artifact_id, shard_hash};
use punah_network::{
    StopHandle, VersionedFrame, read_frame, read_versioned_frame, write_frame,
    write_versioned_frame,
};
use punah_protocol::v2;
use punah_protocol::{Frame, Opcode};
use punah_server::Node;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "punah-server-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create temp directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn start(directory: &Path) -> (SocketAddr, StopHandle, JoinHandle<std::io::Result<()>>) {
    let mut config = Config::default();
    config.tcp.listen_address = "127.0.0.1:0".into();
    config.tcp.read_timeout = Duration::from_secs(2);
    config.tcp.write_timeout = Duration::from_secs(2);
    config.storage.directory = directory.to_path_buf();
    config.storage.segment_bytes = 512;
    config.engine.shard_count = 4;
    config.engine.queue_capacity = 64;
    config.limits.max_key_bytes = 128;
    config.limits.max_value_bytes = 256;
    config.limits.max_frame_bytes = 400;
    let node = Node::bind(config).expect("bind node");
    let address = node.local_addr().expect("local address");
    let stop = node.stop_handle();
    let thread = thread::spawn(move || node.serve());
    (address, stop, thread)
}

fn exchange(stream: &mut TcpStream, request: &Frame) -> Frame {
    write_frame(stream, request).expect("write request");
    read_frame(stream)
        .expect("read response")
        .expect("server response")
}

fn exchange_v2(stream: &mut TcpStream, request: v2::Frame) -> v2::Frame {
    write_versioned_frame(stream, &VersionedFrame::V2(request)).expect("write v2 request");
    let response = read_versioned_frame(stream)
        .expect("read v2 response")
        .expect("server response");
    let VersionedFrame::V2(response) = response else {
        panic!("v2 request received v1 response");
    };
    response
}

fn v2_request(
    opcode: v2::Opcode,
    request_id: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    ttl_seconds: u32,
) -> v2::Frame {
    v2::Frame {
        opcode,
        status: v2::Status::None,
        flags: 0,
        request_id,
        ttl_seconds,
        key,
        value,
    }
}

fn stop(stop: &StopHandle, thread: JoinHandle<std::io::Result<()>>) {
    stop.stop();
    thread.join().expect("server thread").expect("serve");
}

#[test]
fn tcp_data_survives_restart_and_delete_stays_deleted() {
    let directory = TempDir::new();
    let keys = keys_for_each_shard(4);

    let (address, stop_handle, server) = start(&directory.0);
    let mut client = TcpStream::connect(address).expect("connect");
    assert_eq!(
        exchange(&mut client, &Frame::new(Opcode::Ping, vec![], vec![])).opcode,
        Opcode::Pong
    );
    for (shard, key) in keys.iter().enumerate() {
        let value = format!("context-{shard}").into_bytes();
        assert_eq!(
            exchange(&mut client, &Frame::new(Opcode::Set, key.clone(), value)).opcode,
            Opcode::Set
        );
    }
    drop(client);
    stop(&stop_handle, server);

    let (address, stop_handle, server) = start(&directory.0);
    let mut client = TcpStream::connect(address).expect("reconnect");
    for (shard, key) in keys.iter().enumerate() {
        assert_eq!(
            exchange(&mut client, &Frame::new(Opcode::Get, key.clone(), vec![])).value,
            format!("context-{shard}").as_bytes()
        );
    }
    assert_eq!(
        exchange(
            &mut client,
            &Frame::new(Opcode::Delete, keys[0].clone(), vec![])
        )
        .value,
        [1]
    );
    drop(client);
    stop(&stop_handle, server);

    let (address, stop_handle, server) = start(&directory.0);
    let mut client = TcpStream::connect(address).expect("second reconnect");
    assert!(
        exchange(
            &mut client,
            &Frame::new(Opcode::Get, keys[0].clone(), vec![])
        )
        .value
        .is_empty()
    );
    drop(client);
    stop(&stop_handle, server);
}

#[test]
fn pipelined_v1_requests_preserve_response_and_same_key_order() {
    let directory = TempDir::new();
    let (address, stop_handle, server) = start(&directory.0);
    let mut client = TcpStream::connect(address).expect("connect");
    client.set_nodelay(true).expect("nodelay");
    for index in 0..16 {
        let request = if index == 0 {
            Frame::new(Opcode::Set, b"pipeline".to_vec(), b"value".to_vec())
        } else {
            Frame::new(Opcode::Get, b"pipeline".to_vec(), Vec::new())
        };
        write_frame(&mut client, &request).expect("pipeline request");
    }
    for index in 0..16 {
        let response = read_frame(&mut client)
            .expect("pipeline response read")
            .expect("pipeline response");
        if index == 0 {
            assert_eq!(response.opcode, Opcode::Set);
        } else {
            assert_eq!(response.opcode, Opcode::Get);
            assert_eq!(response.value, b"value");
        }
    }
    drop(client);
    stop(&stop_handle, server);
}

#[test]
fn v2_tcp_lease_publish_and_resolve_preserves_request_ids() {
    let directory = TempDir::new();
    let (address, stop_handle, server) = start(&directory.0);
    let mut client = TcpStream::connect(address).expect("connect");
    let operation = vec![0xa5; 32];

    let acquire = exchange_v2(
        &mut client,
        v2_request(v2::Opcode::LeaseAcquire, 101, operation.clone(), vec![], 30),
    );
    assert_eq!(acquire.request_id, 101);
    assert_eq!(acquire.status, v2::Status::LeaseGranted);
    assert_eq!(acquire.value.len(), 16);

    let bytes = b"shared agent result";
    let result_id = artifact_id(bytes);
    let mut envelope = acquire.value;
    envelope.extend_from_slice(result_id.as_bytes());
    envelope.extend_from_slice(bytes);
    let publish = exchange_v2(
        &mut client,
        v2_request(
            v2::Opcode::ResultPublish,
            102,
            operation.clone(),
            envelope,
            0,
        ),
    );
    assert_eq!(publish.request_id, 102);
    assert_eq!(publish.status, v2::Status::Stored);

    let resolve = exchange_v2(
        &mut client,
        v2_request(v2::Opcode::OperationResolve, 103, operation, vec![], 0),
    );
    assert_eq!(resolve.request_id, 103);
    assert_eq!(resolve.status, v2::Status::Hit);
    assert_eq!(resolve.value, bytes);

    drop(client);
    stop(&stop_handle, server);
}

fn keys_for_each_shard(shard_count: usize) -> Vec<Vec<u8>> {
    let mut keys = vec![None; shard_count];
    for candidate in 0_u64.. {
        let key = format!("artifact-{candidate}").into_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let shard = shard_hash(&key).get() as usize & (shard_count - 1);
        keys[shard].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            return keys.into_iter().map(Option::unwrap).collect();
        }
    }
    unreachable!("unbounded search finds one key per shard")
}
