use std::{
    io::{self, BufReader},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use punah_protocol::Frame;

#[cfg(test)]
use crate::write_frame;
use crate::{
    BoxError, ConnectionError, FrameWriteMode, VersionedFrame, read_frame, read_versioned_frame,
    write_frame_with_mode, write_versioned_frame_with_mode,
};

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub accept_poll_interval: Duration,
    pub write_mode: FrameWriteMode,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 1_024,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            accept_poll_interval: Duration::from_millis(10),
            write_mode: FrameWriteMode::Auto,
        }
    }
}

pub trait RequestHandler: Send + Sync + 'static {
    /// Handles one complete request without depending on the transport.
    ///
    /// # Errors
    ///
    /// Returns an application error when no response can be produced.
    fn handle(&self, request: Frame) -> Result<Frame, BoxError>;
}

impl<F> RequestHandler for F
where
    F: Fn(Frame) -> Result<Frame, BoxError> + Send + Sync + 'static,
{
    fn handle(&self, request: Frame) -> Result<Frame, BoxError> {
        self(request)
    }
}

pub trait VersionedRequestHandler: Send + Sync + 'static {
    /// Handles one decoded request without transport concerns.
    ///
    /// # Errors
    /// Returns an application error when no response can be produced.
    fn handle_versioned(&self, request: VersionedFrame) -> Result<VersionedFrame, BoxError>;

    /// Handles one connection-local batch while preserving response order.
    ///
    /// The default retains compatibility for handlers without batch support.
    ///
    /// # Errors
    /// Returns the first application error produced by an individual request.
    fn handle_versioned_batch(
        &self,
        requests: Vec<VersionedFrame>,
    ) -> Result<Vec<VersionedFrame>, BoxError> {
        requests
            .into_iter()
            .map(|request| self.handle_versioned(request))
            .collect()
    }
}

impl<F> VersionedRequestHandler for F
where
    F: Fn(VersionedFrame) -> Result<VersionedFrame, BoxError> + Send + Sync + 'static,
{
    fn handle_versioned(&self, request: VersionedFrame) -> Result<VersionedFrame, BoxError> {
        self(request)
    }
}

#[derive(Clone, Debug)]
pub struct StopHandle(Arc<AtomicBool>);

impl StopHandle {
    pub fn stop(&self) {
        self.0.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub struct Server {
    listener: TcpListener,
    config: ServerConfig,
    stopped: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
}

impl Server {
    /// Binds a nonblocking listener.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if address resolution, binding, or listener setup fails.
    pub fn bind(address: impl ToSocketAddrs, config: ServerConfig) -> io::Result<Self> {
        if config.max_connections == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_connections must be nonzero",
            ));
        }
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            config,
            stopped: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Runs until its stop handle is signalled, then joins all connection threads.
    ///
    /// Connections accepted above the configured limit are immediately closed.
    /// Per-connection failures close only that connection.
    ///
    /// # Errors
    ///
    /// Returns an error if accepting a connection fails unexpectedly.
    #[allow(clippy::needless_pass_by_value)]
    pub fn serve<H: RequestHandler>(&self, handler: Arc<H>) -> io::Result<()> {
        let mut workers = Vec::new();
        while !self.stopped.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    // Accepted sockets can inherit the listener's nonblocking
                    // mode on some platforms. Connection workers intentionally
                    // use blocking I/O with deadlines.
                    if stream.set_nonblocking(false).is_err() {
                        drop(stream);
                        continue;
                    }
                    if !try_acquire(&self.active, self.config.max_connections) {
                        drop(stream);
                        continue;
                    }
                    workers.push(spawn_connection(
                        stream,
                        Arc::clone(&handler),
                        Arc::clone(&self.active),
                        self.config,
                    ));
                    reap_finished(&mut workers);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(self.config.accept_poll_interval);
                    reap_finished(&mut workers);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        for worker in workers {
            let _ = worker.join();
        }
        Ok(())
    }

    /// Runs a listener accepting both independently framed protocol versions.
    ///
    /// # Errors
    /// Returns an error if accepting a connection fails unexpectedly.
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub fn serve_versioned<H: VersionedRequestHandler>(&self, handler: Arc<H>) -> io::Result<()> {
        let mut workers = Vec::new();
        while !self.stopped.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if stream.set_nonblocking(false).is_err() {
                        drop(stream);
                        continue;
                    }
                    if !try_acquire(&self.active, self.config.max_connections) {
                        drop(stream);
                        continue;
                    }
                    let active = Arc::clone(&self.active);
                    let handler = Arc::clone(&handler);
                    let config = self.config;
                    workers.push(thread::spawn(move || {
                        let _guard = ActiveGuard(active);
                        if let Err(error) =
                            serve_versioned_connection(stream, handler.as_ref(), config)
                        {
                            eprintln!("Punah connection closed after error: {error}");
                        }
                    }));
                    reap_finished(&mut workers);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(self.config.accept_poll_interval);
                    reap_finished(&mut workers);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        for worker in workers {
            let _ = worker.join();
        }
        Ok(())
    }

    #[must_use]
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle(Arc::clone(&self.stopped))
    }

    /// # Errors
    ///
    /// Returns an error if the listener's local address cannot be queried.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

fn try_acquire(active: &AtomicUsize, maximum: usize) -> bool {
    active
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < maximum).then_some(current + 1)
        })
        .is_ok()
}

struct ActiveGuard(Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn spawn_connection<H: RequestHandler>(
    stream: TcpStream,
    handler: Arc<H>,
    active: Arc<AtomicUsize>,
    config: ServerConfig,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let _guard = ActiveGuard(active);
        if let Err(error) = serve_connection(stream, handler.as_ref(), config) {
            eprintln!("Punah connection closed after error: {error}");
        }
    })
}

fn serve_connection<H: RequestHandler>(
    mut stream: TcpStream,
    handler: &H,
    config: ServerConfig,
) -> Result<(), ConnectionError> {
    stream.set_read_timeout(Some(config.read_timeout))?;
    stream.set_write_timeout(Some(config.write_timeout))?;
    while let Some(request) = read_frame(&mut stream)? {
        let response = handler.handle(request).map_err(ConnectionError::Handler)?;
        write_frame_with_mode(&mut stream, &response, config.write_mode)?;
    }
    Ok(())
}

fn serve_versioned_connection<H: VersionedRequestHandler>(
    stream: TcpStream,
    handler: &H,
    config: ServerConfig,
) -> Result<(), ConnectionError> {
    stream.set_read_timeout(Some(config.read_timeout))?;
    stream.set_write_timeout(Some(config.write_timeout))?;
    let mut reader = BufReader::with_capacity(64 * 1024, stream);
    while let Some(request) = read_versioned_frame(&mut reader)? {
        let mut requests = Vec::with_capacity(32);
        requests.push(request);
        while requests.len() < 32 && buffered_frame_ready(reader.buffer()) {
            let Some(request) = read_versioned_frame(&mut reader)? else {
                break;
            };
            requests.push(request);
        }
        let expected_responses = requests.len();
        let responses = handler
            .handle_versioned_batch(requests)
            .map_err(ConnectionError::Handler)?;
        if responses.len() != expected_responses {
            return Err(ConnectionError::Handler(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "batch handler returned {} responses for {expected_responses} requests",
                    responses.len()
                ),
            ))));
        }
        for response in &responses {
            write_versioned_frame_with_mode(reader.get_mut(), response, config.write_mode)?;
        }
    }
    Ok(())
}

fn buffered_frame_ready(bytes: &[u8]) -> bool {
    let Some(version) = bytes.first() else {
        return false;
    };
    let total = match *version {
        punah_protocol::VERSION if bytes.len() >= punah_protocol::HEADER_LEN => {
            let key = usize::from(u16::from_be_bytes([bytes[2], bytes[3]]));
            let value = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
            punah_protocol::HEADER_LEN
                .checked_add(key)
                .and_then(|length| length.checked_add(value))
        }
        punah_protocol::v2::VERSION if bytes.len() >= punah_protocol::v2::HEADER_LEN => {
            let key = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]) as usize;
            let value = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]) as usize;
            punah_protocol::v2::HEADER_LEN
                .checked_add(key)
                .and_then(|length| length.checked_add(value))
        }
        _ => None,
    };
    total.is_some_and(|length| bytes.len() >= length)
}

fn reap_finished(workers: &mut Vec<JoinHandle<()>>) {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].is_finished() {
            let worker = workers.swap_remove(index);
            let _ = worker.join();
        } else {
            index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::Shutdown,
        sync::{Barrier, mpsc},
        time::Instant,
    };

    use punah_protocol::Opcode;

    use super::*;

    #[allow(clippy::unnecessary_wraps)]
    fn ping_handler(request: Frame) -> Result<Frame, BoxError> {
        assert_eq!(request.opcode, Opcode::Ping);
        Ok(Frame::new(Opcode::Pong, request.key, request.value))
    }

    #[test]
    fn loopback_ping_and_connection_cleanup() {
        let server = Server::bind(
            "127.0.0.1:0",
            ServerConfig {
                read_timeout: Duration::from_millis(100),
                write_timeout: Duration::from_secs(1),
                ..ServerConfig::default()
            },
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let stop = server.stop_handle();
        let active = Arc::clone(&server.active);
        let (done_tx, done_rx) = mpsc::channel();
        let server_thread = thread::spawn(move || {
            let result = server.serve(Arc::new(ping_handler));
            done_tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        let ping = Frame::new(Opcode::Ping, b"hello".to_vec(), b"world".to_vec());
        write_frame(&mut client, &ping).unwrap();
        let response = read_frame(&mut client).unwrap().unwrap();
        assert_eq!(response, Frame::new(Opcode::Pong, ping.key, ping.value));
        let _ = client.shutdown(Shutdown::Both);
        drop(client);

        let deadline = Instant::now() + Duration::from_secs(1);
        while active.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(active.load(Ordering::Acquire), 0);
        stop.stop();
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    fn connection_limit_rejects_excess_client() {
        let server = Server::bind(
            "127.0.0.1:0",
            ServerConfig {
                max_connections: 1,
                // Keep the established client alive throughout the assertion;
                // the test is about the admission cap, not idle timeouts.
                read_timeout: Duration::from_secs(30),
                accept_poll_interval: Duration::from_millis(2),
                ..ServerConfig::default()
            },
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let stop = server.stop_handle();
        let active = Arc::clone(&server.active);
        let barrier = Arc::new(Barrier::new(2));
        let handler_barrier = Arc::clone(&barrier);
        let handler = move |request: Frame| {
            handler_barrier.wait();
            handler_barrier.wait();
            ping_handler(request)
        };
        let server_thread = thread::spawn(move || server.serve(Arc::new(handler)));
        let mut first = TcpStream::connect(address).unwrap();
        let ping = Frame::new(Opcode::Ping, Vec::new(), Vec::new());
        write_frame(&mut first, &ping).unwrap();
        barrier.wait();
        let deadline = Instant::now() + Duration::from_secs(1);
        while active.load(Ordering::Acquire) != 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(active.load(Ordering::Acquire), 1);

        let mut second = TcpStream::connect(address).unwrap();
        second
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let _ = write_frame(&mut second, &ping);
        let result = read_frame(&mut second);
        assert!(matches!(result, Ok(None) | Err(ConnectionError::Io(_))));

        barrier.wait();
        assert_eq!(
            read_frame(&mut first).unwrap().unwrap().opcode,
            Opcode::Pong
        );
        drop(first);
        stop.stop();
        server_thread.join().unwrap().unwrap();
    }
}
