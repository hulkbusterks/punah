#![forbid(unsafe_code)]
//! Assembly boundary for a runnable single-node Punah service.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use punah_config::{Config, StorageDurability, ValidationErrors};
use punah_network::{
    BoxError, FrameWriteMode, Server, ServerConfig, StopHandle, VersionedFrame,
    VersionedRequestHandler,
};
use punah_runtime::{OpenError, Runtime, RuntimeConfig};
use punah_service::{RequestLimits, RuntimeService};
use punah_types::{Durability, Namespace, NamespaceError};

/// Errors encountered while assembling a node.
#[derive(Debug)]
pub enum NodeError {
    InvalidConfig(ValidationErrors),
    InvalidNamespace(NamespaceError),
    Runtime(OpenError),
    Io(io::Error),
    LimitNotRepresentable(&'static str),
}

impl fmt::Display for NodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for NodeError {}

/// A bound TCP listener and its durable command service.
pub struct Node {
    server: Server,
    service: RuntimeService,
    runtime: Runtime,
}

impl Node {
    /// Validates configuration, opens durable storage, and binds TCP.
    ///
    /// # Errors
    /// Returns configuration, namespace, storage, or socket setup failures.
    pub fn bind(config: Config) -> Result<Self, NodeError> {
        Self::bind_with_write_mode(config, FrameWriteMode::Auto)
    }

    /// Binds a node with an explicit response assembly mode for controlled A/B tests.
    ///
    /// # Errors
    /// Returns configuration, namespace, storage, or socket setup failures.
    pub fn bind_with_write_mode(
        config: Config,
        write_mode: FrameWriteMode,
    ) -> Result<Self, NodeError> {
        config.validate().map_err(NodeError::InvalidConfig)?;
        let limits = RequestLimits {
            max_key_bytes: usize::try_from(config.limits.max_key_bytes)
                .map_err(|_| NodeError::LimitNotRepresentable("max_key_bytes"))?,
            max_value_bytes: usize::try_from(config.limits.max_value_bytes)
                .map_err(|_| NodeError::LimitNotRepresentable("max_value_bytes"))?,
            max_frame_bytes: usize::try_from(config.limits.max_frame_bytes)
                .map_err(|_| NodeError::LimitNotRepresentable("max_frame_bytes"))?,
        };
        let runtime = Runtime::open(&RuntimeConfig {
            directory: config.storage.directory,
            namespace: Namespace::new("default").map_err(NodeError::InvalidNamespace)?,
            shard_count: config.engine.shard_count,
            queue_capacity: config.engine.queue_capacity,
            segment_capacity: config.storage.segment_bytes,
            durability: match config.storage.durability {
                StorageDurability::Buffered => Durability::Buffered,
                StorageDurability::LocalDurable => Durability::LocalDurable,
            },
        })
        .map_err(NodeError::Runtime)?;
        let service = RuntimeService::with_limits(runtime.handle(), limits);
        let server = Server::bind(
            config.tcp.listen_address,
            ServerConfig {
                max_connections: config.limits.max_connections,
                read_timeout: config.tcp.read_timeout,
                write_timeout: config.tcp.write_timeout,
                accept_poll_interval: Duration::from_millis(5),
                write_mode,
            },
        )
        .map_err(NodeError::Io)?;
        Ok(Self {
            server,
            service,
            runtime,
        })
    }

    /// Serves until [`StopHandle::stop`] is called, then drains workers.
    ///
    /// # Errors
    /// Returns unexpected listener failures.
    pub fn serve(self) -> io::Result<()> {
        let result = self.server.serve_versioned(Arc::new(NodeHandler {
            service: self.service.clone(),
        }));
        // `Server::serve` has joined every connection worker here. Drop the
        // final service handle before stopping admission and draining shards.
        drop(self.service);
        self.runtime.shutdown();
        result
    }

    #[must_use]
    pub fn stop_handle(&self) -> StopHandle {
        self.server.stop_handle()
    }

    /// # Errors
    /// Returns an error if the listener address cannot be queried.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.server.local_addr()
    }
}

struct NodeHandler {
    service: RuntimeService,
}

impl VersionedRequestHandler for NodeHandler {
    fn handle_versioned(&self, request: VersionedFrame) -> Result<VersionedFrame, BoxError> {
        Ok(match request {
            VersionedFrame::V1(frame) => VersionedFrame::V1(self.service.handle(frame)),
            VersionedFrame::V2(frame) => VersionedFrame::V2(self.service.handle_v2(frame)),
        })
    }

    fn handle_versioned_batch(
        &self,
        requests: Vec<VersionedFrame>,
    ) -> Result<Vec<VersionedFrame>, BoxError> {
        if requests
            .iter()
            .any(|request| matches!(request, VersionedFrame::V2(_)))
        {
            return requests
                .into_iter()
                .map(|request| self.handle_versioned(request))
                .collect();
        }
        let v1 = requests
            .into_iter()
            .map(|request| match request {
                VersionedFrame::V1(frame) => frame,
                VersionedFrame::V2(_) => unreachable!("v2 was rejected above"),
            })
            .collect();
        Ok(self
            .service
            .handle_batch(v1)
            .into_iter()
            .map(VersionedFrame::V1)
            .collect())
    }
}
