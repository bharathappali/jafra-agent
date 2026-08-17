use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use jafra_agent::config::{AgentMode, Config};
use jafra_agent::identity::chunk_id;
use jafra_agent::jfr::header::{write_finalized_header, HEADER_SIZE};
use jafra_agent::path::RecordingIdentity;
use jafra_agent::proto::ingest::v1::jafra_ingest_service_server::{
    JafraIngestService, JafraIngestServiceServer,
};
use jafra_agent::proto::ingest::v1::upload_request::Payload;
use jafra_agent::proto::ingest::v1::{AckStatus, UploadAck, UploadRequest};
use jafra_agent::state::ChunkKey;
use jafra_agent::transport::grpc::GrpcTransport;
use jafra_agent::transport::{Transport, TransportResult};
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

struct ScriptedAnalyzer {
    status: Mutex<AckStatus>,
    seen_ids: Arc<Mutex<Vec<String>>>,
}

#[tonic::async_trait]
impl JafraIngestService for ScriptedAnalyzer {
    type UploadStream = Pin<Box<dyn Stream<Item = Result<UploadAck, Status>> + Send + 'static>>;

    async fn upload(
        &self,
        request: Request<tonic::Streaming<UploadRequest>>,
    ) -> Result<Response<Self::UploadStream>, Status> {
        let mut inbound = request.into_inner();
        let mut chunk = String::new();
        let mut recording = String::new();
        while let Some(message) = inbound.message().await? {
            match message.payload {
                Some(Payload::Open(open)) => {
                    recording = open.recording_id.clone();
                }
                Some(Payload::Commit(commit)) => {
                    chunk = commit.chunk_id.clone();
                    self.seen_ids.lock().await.push(chunk.clone());
                    break;
                }
                _ => {}
            }
        }
        let status = *self.status.lock().await;
        let ack = UploadAck {
            recording_id: recording,
            chunk_id: chunk,
            status: status.into(),
            message: "test".into(),
            received_bytes: 0,
        };
        Ok(Response::new(Box::pin(tokio_stream::once(Ok(ack)))))
    }
}

async fn start_server(service: ScriptedAnalyzer) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(JafraIngestServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

fn sample_file() -> (tempfile::TempDir, ChunkKey, RecordingIdentity) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("default/uid/auth-cache/profile-0.jfr");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut bytes = Vec::new();
    let payload = 32;
    let header = write_finalized_header((HEADER_SIZE + payload) as u64, 11, 5_000_000_000);
    bytes.extend_from_slice(&header);
    bytes.extend(std::iter::repeat(7u8).take(payload));
    std::fs::write(&path, &bytes).unwrap();
    let key = ChunkKey {
        path,
        offset: 0,
        length: bytes.len() as u64,
        chunk_index: 0,
        start_time_ns: 11,
        duration_ns: 5_000_000_000,
    };
    let identity = RecordingIdentity {
        namespace: "default".into(),
        pod_name: "workload".into(),
        pod_uid: "uid".into(),
        container: "auth-cache".into(),
        filename: "profile-0.jfr".into(),
    };
    (dir, key, identity)
}

fn test_config(endpoint: String) -> Config {
    Config {
        recording_root: "/tmp".into(),
        mode: AgentMode::Grpc,
        node_name: "worker-1".into(),
        cluster_id: "local-demo".into(),
        analyzer_endpoint: endpoint,
        rescan_interval: Duration::from_secs(10),
        max_active_readers: 8,
        max_in_flight_chunks: 8,
        frame_size: 16,
        retry_initial_delay: Duration::from_millis(10),
        retry_max_delay: Duration::from_millis(20),
        delete_closed_files: true,
    }
}

#[tokio::test]
async fn successful_upload_and_retry_identity() {
    let seen_ids = Arc::new(Mutex::new(Vec::new()));
    let analyzer = ScriptedAnalyzer {
        status: Mutex::new(AckStatus::Accepted),
        seen_ids: seen_ids.clone(),
    };
    let (addr, _handle) = start_server(analyzer).await;
    let config = test_config(format!("http://{addr}"));
    let (_dir, key, identity) = sample_file();
    let transport = GrpcTransport::new(
        config.analyzer_endpoint.clone(),
        jafra_agent::metrics::Metrics::new(),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        transport.publish(&config, &identity, 0, &key),
    )
    .await
    .expect("upload timed out");
    assert!(matches!(result, TransportResult::Accepted { .. }));
    let ids = seen_ids.lock().await.clone();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        ids[0],
        chunk_id(
            "local-demo",
            "uid",
            "auth-cache",
            "profile-0.jfr",
            0,
            key.length
        )
    );
}

#[tokio::test]
async fn analyzer_unavailable_is_retry() {
    let config = test_config("http://127.0.0.1:1".into());
    let (_dir, key, identity) = sample_file();
    let transport = GrpcTransport::new(
        config.analyzer_endpoint.clone(),
        jafra_agent::metrics::Metrics::new(),
    );
    let result = transport.publish(&config, &identity, 0, &key).await;
    assert!(matches!(result, TransportResult::Retry { .. }));
}
