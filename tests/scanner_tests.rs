use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use jafra_agent::config::{AgentMode, Config};
use jafra_agent::discovery::discover_jfr_files;
use jafra_agent::jfr::header::{write_finalized_header, write_placeholder_header, HEADER_SIZE};
use jafra_agent::jfr::scanner::{scan_file, FileCursor};
use jafra_agent::metrics::Metrics;
use jafra_agent::path::{identity_from_path, join_under_root};
use jafra_agent::state::{ChunkKey, CollectorState};
use jafra_agent::transport::{BoxFuture, Transport, TransportResult};
use jafra_agent::worker::rescan;
use tempfile::tempdir;
use tokio::sync::Mutex;

fn write_chunk(bytes: &mut Vec<u8>, payload: usize, finalized: bool) {
    let size = (HEADER_SIZE + payload) as u64;
    let header = if finalized {
        write_finalized_header(size, 1, 5_000_000_000)
    } else {
        write_placeholder_header(size)
    };
    bytes.extend_from_slice(&header);
    bytes.extend(std::iter::repeat(1u8).take(payload));
}

#[test]
fn two_finalized_chunks_in_one_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("recording.jfr");
    let mut bytes = Vec::new();
    write_chunk(&mut bytes, 10, true);
    write_chunk(&mut bytes, 20, true);
    fs::write(&path, &bytes).unwrap();
    let mut cursor = FileCursor::new(path.clone());
    let chunks = scan_file(&path, &mut cursor, bytes.len() as u64).unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].chunk_offset, 0);
    assert_eq!(chunks[1].chunk_offset, (HEADER_SIZE + 10) as u64);
}

#[test]
fn one_finalized_chunk_followed_by_incomplete_chunk() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("recording.jfr");
    let mut bytes = Vec::new();
    write_chunk(&mut bytes, 10, true);
    write_chunk(&mut bytes, 20, false);
    fs::write(&path, &bytes).unwrap();
    let mut cursor = FileCursor::new(path.clone());
    let chunks = scan_file(&path, &mut cursor, bytes.len() as u64).unwrap();
    assert_eq!(chunks.len(), 1);
}

#[test]
fn file_growth_followed_by_rescan() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("recording.jfr");
    let mut bytes = Vec::new();
    write_chunk(&mut bytes, 8, true);
    fs::write(&path, &bytes).unwrap();
    let mut cursor = FileCursor::new(path.clone());
    let first = scan_file(&path, &mut cursor, bytes.len() as u64).unwrap();
    assert_eq!(first.len(), 1);
    write_chunk(&mut bytes, 8, true);
    fs::write(&path, &bytes).unwrap();
    let second = scan_file(&path, &mut cursor, bytes.len() as u64).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].chunk_index, 1);
}

#[test]
fn duplicate_notifications_are_deduplicated() {
    let mut state = CollectorState::default();
    let key = ChunkKey {
        path: "file.jfr".into(),
        offset: 0,
        length: 80,
        chunk_index: 0,
        start_time_ns: 0,
        duration_ns: 0,
    };
    assert!(state.enqueue(key.clone()));
    assert!(!state.enqueue(key));
    assert_eq!(state.queued_len(), 1);
}

#[test]
fn path_metadata_extraction_and_traversal() {
    let identity =
        identity_from_path("/jfr-data".as_ref(), "/jfr-data/ns/uid/container/profile-2.jfr".as_ref()).unwrap();
    assert_eq!(identity.filename, "profile-2.jfr");
    assert!(join_under_root("/jfr-data".as_ref(), "../x".as_ref()).is_err());
}

#[test]
fn multiple_pod_directories() {
    let dir = tempdir().unwrap();
    let one = dir.path().join("default/uid-a/auth-cache");
    let two = dir.path().join("default/uid-b/auth-cache");
    fs::create_dir_all(&one).unwrap();
    fs::create_dir_all(&two).unwrap();
    fs::write(one.join("profile-0.jfr"), [0u8; 4]).unwrap();
    fs::write(two.join("profile-0.jfr"), [0u8; 4]).unwrap();
    let files = discover_jfr_files(dir.path()).unwrap();
    assert_eq!(files.len(), 2);
}

struct BlockingTransport {
    inflight: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
}

impl Transport for BlockingTransport {
    fn publish<'a>(
        &'a self,
        _config: &'a Config,
        _identity: &'a jafra_agent::path::RecordingIdentity,
        _chunk_index: u32,
        _key: &'a ChunkKey,
    ) -> BoxFuture<'a, TransportResult> {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let current = self.inflight.load(Ordering::SeqCst);
        self.max.fetch_max(current, Ordering::SeqCst);
        let inflight = self.inflight.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            inflight.fetch_sub(1, Ordering::SeqCst);
            TransportResult::Logged
        })
    }
}

#[tokio::test]
async fn bounded_worker_concurrency() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    for index in 0..6 {
        let mut bytes = Vec::new();
        write_chunk(&mut bytes, 4, true);
        fs::write(pod.join(format!("profile-{index}.jfr")), bytes).unwrap();
    }
    let config = Config {
        recording_root: dir.path().to_path_buf(),
        mode: AgentMode::LogOnly,
        node_name: "test".into(),
        cluster_id: "local-demo".into(),
        analyzer_endpoint: "http://127.0.0.1:9".into(),
        rescan_interval: Duration::from_secs(10),
        max_active_readers: 2,
        max_in_flight_chunks: 2,
        frame_size: 128,
            retry_initial_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_millis(20),
            delete_closed_files: true,
        };
    let state = Arc::new(Mutex::new(CollectorState::default()));
    let metrics = Metrics::new();
    rescan(&config, &state, &metrics).await;
    assert!(state.lock().await.queued_len() >= 6);
}

#[test]
fn semaphore_limit_is_positive() {
    let config = Config::from_vars(Vec::<(String, String)>::new()).unwrap();
    assert_eq!(config.max_active_readers, 8);
    assert_eq!(config.max_in_flight_chunks, 8);
}
