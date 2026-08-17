use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::config::{AgentMode, Config};
use crate::path::RecordingIdentity;
use crate::state::ChunkKey;

pub mod grpc;
pub mod log_only;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportResult {
    Logged,
    Accepted { bytes: u64 },
    Duplicate { bytes: u64 },
    Retry { reason: String },
    Rejected { reason: String },
}

pub trait Transport: Send + Sync {
    fn publish<'a>(
        &'a self,
        config: &'a Config,
        identity: &'a RecordingIdentity,
        chunk_index: u32,
        key: &'a ChunkKey,
    ) -> BoxFuture<'a, TransportResult>;
}

pub async fn build_transport(
    config: &Config,
) -> Result<Arc<dyn Transport>, Box<dyn std::error::Error + Send + Sync>> {
    match config.mode {
        AgentMode::LogOnly => Ok(Arc::new(log_only::LogOnlyTransport)),
        AgentMode::Grpc => Ok(grpc::GrpcTransport::connect(config).await?),
    }
}
