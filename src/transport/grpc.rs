use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use super::{BoxFuture, Transport, TransportResult};
use crate::config::Config;
use crate::identity::{chunk_id, recording_id, CHECKSUM_ALGORITHM, PROTOCOL_VERSION};
use crate::metrics::Metrics;
use crate::path::{file_sequence, RecordingIdentity};
use crate::proto::ingest::v1::jafra_ingest_service_client::JafraIngestServiceClient;
use crate::proto::ingest::v1::upload_request::Payload;
use crate::proto::ingest::v1::{AckStatus, ChunkFrame, CommitChunk, OpenChunk, UploadAck, UploadRequest};
use crate::state::ChunkKey;

pub struct GrpcTransport {
    endpoint: String,
    client: Mutex<Option<JafraIngestServiceClient<Channel>>>,
    metrics: Arc<Metrics>,
}

impl GrpcTransport {
    pub async fn connect(
        config: &Config,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::new(Self {
            endpoint: config.analyzer_endpoint.clone(),
            client: Mutex::new(None),
            metrics: crate::metrics::Metrics::new(),
        }))
    }

    pub fn new(endpoint: String, metrics: Arc<Metrics>) -> Self {
        Self {
            endpoint,
            client: Mutex::new(None),
            metrics,
        }
    }

    async fn client(&self) -> Result<JafraIngestServiceClient<Channel>, String> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.clone() {
            return Ok(client);
        }
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|error| error.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(30));
        let channel = endpoint.connect().await.map_err(|error| error.to_string())?;
        let client = JafraIngestServiceClient::new(channel);
        *guard = Some(client.clone());
        self.metrics.set(&self.metrics.grpc_connected, 1);
        Ok(client)
    }

    async fn upload(
        &self,
        config: &Config,
        identity: &RecordingIdentity,
        _chunk_index: u32,
        key: &ChunkKey,
    ) -> TransportResult {
        match self.upload_inner(config, identity, key).await {
            Ok(result) => result,
            Err(error) => {
                *self.client.lock().await = None;
                self.metrics.set(&self.metrics.grpc_connected, 0);
                TransportResult::Retry { reason: error }
            }
        }
    }

    async fn upload_inner(
        &self,
        config: &Config,
        identity: &RecordingIdentity,
        key: &ChunkKey,
    ) -> Result<TransportResult, String> {
        let metadata = tokio::fs::metadata(&key.path)
            .await
            .map_err(|error| error.to_string())?;
        if metadata.len() < key.offset.saturating_add(key.length) {
            return Ok(TransportResult::Retry {
                reason: "source file no longer contains the finalized chunk".into(),
            });
        }
        let (open_tx, open_rx) = tokio::sync::mpsc::channel(8);
        let sender_config = config.clone();
        let sender_identity = identity.clone();
        let sender_key = key.clone();
        let sender = tokio::spawn(async move {
            send_chunk(sender_config, sender_identity, sender_key, open_tx).await
        });
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(open_rx);
        let mut client = self.client().await?;
        let mut inbound = client
            .upload(Request::new(request_stream))
            .await
            .map_err(status_reason)?
            .into_inner();
        let send_result = sender.await.map_err(|error| error.to_string())??;
        let ack = inbound
            .message()
            .await
            .map_err(status_reason)?
            .ok_or_else(|| "analyzer closed the stream before ACK".to_string())?;
        let _ = send_result;
        Ok(ack_result(&ack, key.length))
    }
}

async fn send_chunk(
    config: Config,
    identity: RecordingIdentity,
    key: ChunkKey,
    open_tx: tokio::sync::mpsc::Sender<UploadRequest>,
) -> Result<(), String> {
    let mut file = File::open(&key.path)
        .await
        .map_err(|error| error.to_string())?;
    file.seek(SeekFrom::Start(key.offset))
        .await
        .map_err(|error| error.to_string())?;
    let recording = recording_id(
        &config.cluster_id,
        &identity.pod_uid,
        &identity.container,
        &identity.filename,
    );
    let chunk = chunk_id(
        &config.cluster_id,
        &identity.pod_uid,
        &identity.container,
        &identity.filename,
        key.offset,
        key.length,
    );
    open_tx
        .send(UploadRequest {
            payload: Some(Payload::Open(OpenChunk {
                protocol_version: PROTOCOL_VERSION,
                cluster_id: config.cluster_id.clone(),
                node_name: config.node_name.clone(),
                namespace: identity.namespace.clone(),
                pod_name: identity.pod_name.clone(),
                pod_uid: identity.pod_uid.clone(),
                container_name: identity.container.clone(),
                recording_id: recording.clone(),
                physical_filename: identity.filename.clone(),
                physical_file_sequence: file_sequence(&identity.filename),
                chunk_sequence: key.chunk_index,
                chunk_offset: key.offset,
                chunk_length: key.length,
                chunk_start_time_ns: key.start_time_ns,
                chunk_duration_ns: key.duration_ns,
            })),
        })
        .await
        .map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut remaining = key.length;
    let mut frame_offset = 0u64;
    let mut buffer = vec![0u8; config.frame_size];
    while remaining > 0 {
        let to_read = std::cmp::min(remaining, config.frame_size as u64) as usize;
        let read = file
            .read_exact_or_eof(&mut buffer[..to_read])
            .await
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("source file disappeared before the chunk was fully read".into());
        }
        hasher.update(&buffer[..read]);
        open_tx
            .send(UploadRequest {
                payload: Some(Payload::Frame(ChunkFrame {
                    recording_id: recording.clone(),
                    chunk_id: chunk.clone(),
                    frame_offset,
                    payload: buffer[..read].to_vec(),
                })),
            })
            .await
            .map_err(|error| error.to_string())?;
        frame_offset += read as u64;
        remaining -= read as u64;
    }
    let checksum = hex::encode(hasher.finalize());
    open_tx
        .send(UploadRequest {
            payload: Some(Payload::Commit(CommitChunk {
                recording_id: recording,
                chunk_id: chunk,
                total_bytes: key.length,
                checksum_algorithm: CHECKSUM_ALGORITHM.to_string(),
                checksum,
            })),
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

impl Transport for GrpcTransport {
    fn publish<'a>(
        &'a self,
        config: &'a Config,
        identity: &'a RecordingIdentity,
        chunk_index: u32,
        key: &'a ChunkKey,
    ) -> BoxFuture<'a, TransportResult> {
        Box::pin(self.upload(config, identity, chunk_index, key))
    }
}

fn ack_result(ack: &UploadAck, bytes: u64) -> TransportResult {
    match AckStatus::try_from(ack.status).unwrap_or(AckStatus::Unspecified) {
        AckStatus::Accepted => TransportResult::Accepted { bytes },
        AckStatus::Duplicate => TransportResult::Duplicate { bytes },
        AckStatus::Retry | AckStatus::Unspecified => TransportResult::Retry {
            reason: ack.message.clone(),
        },
        AckStatus::Rejected => TransportResult::Rejected {
            reason: ack.message.clone(),
        },
    }
}

fn status_reason(status: Status) -> String {
    status.to_string()
}

trait ReadExactOrEof {
    async fn read_exact_or_eof(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
}

impl ReadExactOrEof for File {
    async fn read_exact_or_eof(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut offset = 0;
        while offset < buf.len() {
            let read = self.read(&mut buf[offset..]).await?;
            if read == 0 {
                break;
            }
            offset += read;
        }
        Ok(offset)
    }
}
