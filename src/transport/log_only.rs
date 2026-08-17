use super::{BoxFuture, Transport, TransportResult};
use crate::config::Config;
use crate::path::RecordingIdentity;
use crate::state::ChunkKey;

pub struct LogOnlyTransport;

impl Transport for LogOnlyTransport {
    fn publish<'a>(
        &'a self,
        _config: &'a Config,
        _identity: &'a RecordingIdentity,
        _chunk_index: u32,
        _key: &'a ChunkKey,
    ) -> BoxFuture<'a, TransportResult> {
        Box::pin(async { TransportResult::Logged })
    }
}
