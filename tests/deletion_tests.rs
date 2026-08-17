use std::fs;
use std::sync::Arc;
use std::time::Duration;

use jafra_agent::config::{AgentMode, Config};
use jafra_agent::jfr::header::{write_finalized_header, write_placeholder_header, HEADER_SIZE};
use jafra_agent::metrics::Metrics;
use jafra_agent::path::identity_from_path;
use jafra_agent::state::CollectorState;
use jafra_agent::worker::try_reclaim_closed_sources;
use tempfile::tempdir;
use tokio::sync::Mutex;

fn write_closed_chunk() -> Vec<u8> {
    let payload = 8;
    let size = (HEADER_SIZE + payload) as u64;
    let mut bytes = write_finalized_header(size, 1, 5_000_000_000).to_vec();
    bytes.extend(std::iter::repeat(1u8).take(payload));
    bytes
}

fn write_open_tail() -> Vec<u8> {
    let payload = 8;
    let size = (HEADER_SIZE + payload) as u64;
    let mut bytes = write_placeholder_header(size).to_vec();
    bytes.extend(std::iter::repeat(1u8).take(payload));
    bytes
}

fn config(root: &std::path::Path, mode: AgentMode, delete: bool) -> Config {
    Config {
        recording_root: root.to_path_buf(),
        mode,
        node_name: "test".into(),
        cluster_id: "local-demo".into(),
        analyzer_endpoint: "http://127.0.0.1:9".into(),
        rescan_interval: Duration::from_secs(10),
        max_active_readers: 8,
        max_in_flight_chunks: 8,
        frame_size: 128,
        retry_initial_delay: Duration::from_millis(10),
        retry_max_delay: Duration::from_millis(20),
        delete_closed_files: delete,
    }
}

#[tokio::test]
async fn deletes_closed_acked_file_once_a_newer_rotation_exists() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    let older = write_closed_chunk();
    let newer = write_closed_chunk();
    let path0 = pod.join("profile-0.jfr");
    let path1 = pod.join("profile-1.jfr");
    fs::write(&path0, &older).unwrap();
    fs::write(&path1, &newer).unwrap();

    let state = Arc::new(Mutex::new(CollectorState::default()));
    let identity = identity_from_path(dir.path(), &path0).unwrap();
    {
        let mut locked = state.lock().await;
        let file = locked.file_mut(&path0, identity);
        file.closed = true;
        file.discovered_end = older.len() as u64;
        file.acked.insert(0, older.len() as u64);
    }
    let metrics = Metrics::new();
    try_reclaim_closed_sources(&config(dir.path(), AgentMode::Grpc, true), &state, &metrics).await;
    assert!(!path0.exists());
    assert!(path1.exists());
}

#[tokio::test]
async fn keeps_the_live_file_when_it_is_the_newest_rotation() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    let bytes = write_closed_chunk();
    let path0 = pod.join("profile-0.jfr");
    fs::write(&path0, &bytes).unwrap();
    let state = Arc::new(Mutex::new(CollectorState::default()));
    let identity = identity_from_path(dir.path(), &path0).unwrap();
    {
        let mut locked = state.lock().await;
        let file = locked.file_mut(&path0, identity);
        file.closed = true;
        file.discovered_end = bytes.len() as u64;
        file.acked.insert(0, bytes.len() as u64);
    }
    let metrics = Metrics::new();
    try_reclaim_closed_sources(&config(dir.path(), AgentMode::Grpc, true), &state, &metrics).await;
    assert!(path0.exists());
}

#[tokio::test]
async fn keeps_an_incomplete_live_file_even_if_a_newer_file_exists() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    let live = write_open_tail();
    let path0 = pod.join("profile-0.jfr");
    fs::write(&path0, &live).unwrap();
    fs::write(pod.join("profile-1.jfr"), write_closed_chunk()).unwrap();
    let state = Arc::new(Mutex::new(CollectorState::default()));
    let identity = identity_from_path(dir.path(), &path0).unwrap();
    {
        let mut locked = state.lock().await;
        let file = locked.file_mut(&path0, identity);
        file.closed = false;
        file.discovered_end = 0;
        file.acked.insert(0, live.len() as u64);
    }
    let metrics = Metrics::new();
    try_reclaim_closed_sources(&config(dir.path(), AgentMode::Grpc, true), &state, &metrics).await;
    assert!(path0.exists());
}

#[tokio::test]
async fn log_only_mode_never_deletes_source_files() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    let bytes = write_closed_chunk();
    let path0 = pod.join("profile-0.jfr");
    fs::write(&path0, &bytes).unwrap();
    fs::write(pod.join("profile-1.jfr"), write_closed_chunk()).unwrap();
    let state = Arc::new(Mutex::new(CollectorState::default()));
    let identity = identity_from_path(dir.path(), &path0).unwrap();
    {
        let mut locked = state.lock().await;
        let file = locked.file_mut(&path0, identity);
        file.closed = true;
        file.discovered_end = bytes.len() as u64;
        file.acked.insert(0, bytes.len() as u64);
    }
    let metrics = Metrics::new();
    try_reclaim_closed_sources(&config(dir.path(), AgentMode::LogOnly, true), &state, &metrics).await;
    assert!(path0.exists());
}

#[tokio::test]
async fn rejected_files_are_not_deleted() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    let bytes = write_closed_chunk();
    let path0 = pod.join("profile-0.jfr");
    fs::write(&path0, &bytes).unwrap();
    fs::write(pod.join("profile-1.jfr"), write_closed_chunk()).unwrap();
    let state = Arc::new(Mutex::new(CollectorState::default()));
    let identity = identity_from_path(dir.path(), &path0).unwrap();
    {
        let mut locked = state.lock().await;
        let file = locked.file_mut(&path0, identity);
        file.closed = true;
        file.discovered_end = bytes.len() as u64;
        file.acked.insert(0, bytes.len() as u64);
        locked.mark_rejected(&path0);
    }
    let metrics = Metrics::new();
    try_reclaim_closed_sources(&config(dir.path(), AgentMode::Grpc, true), &state, &metrics).await;
    assert!(path0.exists());
}

#[tokio::test]
async fn concurrent_reclaim_deletes_a_closed_file_once() {
    let dir = tempdir().unwrap();
    let pod = dir.path().join("default/uid/app");
    fs::create_dir_all(&pod).unwrap();
    let older = write_closed_chunk();
    let path0 = pod.join("profile-0.jfr");
    fs::write(&path0, &older).unwrap();
    fs::write(pod.join("profile-1.jfr"), write_closed_chunk()).unwrap();

    let state = Arc::new(Mutex::new(CollectorState::default()));
    let identity = identity_from_path(dir.path(), &path0).unwrap();
    {
        let mut locked = state.lock().await;
        let file = locked.file_mut(&path0, identity);
        file.closed = true;
        file.discovered_end = older.len() as u64;
        file.acked.insert(0, older.len() as u64);
    }
    let metrics = Metrics::new();
    let cfg = config(dir.path(), AgentMode::Grpc, true);
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let cfg = cfg.clone();
            let state = state.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move { try_reclaim_closed_sources(&cfg, &state, &metrics).await })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    assert!(!path0.exists());
    assert_eq!(
        metrics
            .source_files_deleted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}
