use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::config::{AgentMode, Config};
use crate::discovery::discover_jfr_files;
use crate::jfr::header::HEADER_SIZE;
use crate::jfr::scanner::{scan_file, FinalizedChunk};
use crate::metrics::Metrics;
use crate::path::identity_from_path;
use crate::state::{ChunkKey, CollectorState};
use crate::transport::{Transport, TransportResult};

pub async fn run_collector(
    config: Config,
    state: Arc<Mutex<CollectorState>>,
    metrics: Arc<Metrics>,
    transport: Arc<dyn Transport>,
    mut wakes: mpsc::Receiver<crate::watcher::WatchWake>,
) {
    let readers = Arc::new(Semaphore::new(config.max_active_readers));
    let in_flight = Arc::new(Semaphore::new(config.max_in_flight_chunks));
    rescan(&config, &state, &metrics).await;
    let mut rescan_interval = tokio::time::interval(config.rescan_interval);
    let mut worker_tick = tokio::time::interval(Duration::from_millis(200));

    loop {
        tokio::select! {
            _ = rescan_interval.tick() => {
                rescan(&config, &state, &metrics).await;
            }
            wake = wakes.recv() => {
                match wake {
                    Some(crate::watcher::WatchWake::Overflow) | None => {
                        metrics.inc(&metrics.watcher_overflows);
                        rescan(&config, &state, &metrics).await;
                    }
                    Some(crate::watcher::WatchWake::Path(path)) => {
                        scan_one(&config, &state, &metrics, path).await;
                    }
                }
            }
            _ = worker_tick.tick() => {
                drain_queue(
                    &config,
                    &state,
                    &metrics,
                    transport.clone(),
                    readers.clone(),
                    in_flight.clone(),
                )
                .await;
            }
        }
    }
}

pub async fn rescan(config: &Config, state: &Arc<Mutex<CollectorState>>, metrics: &Arc<Metrics>) {
    metrics.inc(&metrics.full_rescans);
    let files = match discover_jfr_files(&config.recording_root) {
        Ok(files) => files,
        Err(error) => {
            tracing::error!(error = %error, "failed to discover JFR files");
            return;
        }
    };
    metrics.set(&metrics.files_discovered, files.len() as u64);
    let existing = files.iter().cloned().collect();
    {
        let mut locked = state.lock().await;
        locked.forget_missing(&existing);
    }
    for path in files {
        scan_one(config, state, metrics, path).await;
    }
}

async fn scan_one(
    config: &Config,
    state: &Arc<Mutex<CollectorState>>,
    metrics: &Arc<Metrics>,
    path: std::path::PathBuf,
) {
    if path.extension().and_then(|ext| ext.to_str()) != Some("jfr") {
        if path.is_dir() {
            if let Ok(files) = discover_jfr_files(&path) {
                for child in files {
                    Box::pin(scan_one(config, state, metrics, child)).await;
                }
            }
        }
        return;
    }
    let identity = match identity_from_path(&config.recording_root, &path) {
        Ok(identity) => identity,
        Err(error) => {
            tracing::debug!(error = %error, path = %path.display(), "skipping unmanaged path");
            return;
        }
    };
    let metadata = match tokio::fs::metadata(&path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            tracing::debug!(error = %error, path = %path.display(), "unable to stat JFR file");
            return;
        }
    };
    let file_size = metadata.len();
    let (chunks, incomplete) = {
        let mut locked = state.lock().await;
        let file_state = locked.file_mut(&path, identity.clone());
        match scan_file(&path, &mut file_state.cursor, file_size) {
            Ok(chunks) => {
                let incomplete =
                    file_size > file_state.cursor.next_offset
                        && file_size.saturating_sub(file_state.cursor.next_offset) < HEADER_SIZE as u64
                        || remaining_is_open(file_size, file_state.cursor.next_offset);
                file_state.last_scan = Instant::now();
                file_state.discovered_end = file_state.cursor.next_offset;
                file_state.closed = file_size == file_state.cursor.next_offset && file_size > 0;
                (chunks, incomplete)
            }
            Err(error) => {
                tracing::debug!(error = %error, path = %path.display(), "unable to scan JFR file");
                return;
            }
        }
    };
    if incomplete {
        metrics.inc(&metrics.incomplete_chunks);
    }
    for chunk in chunks {
        enqueue_chunk(config, state, metrics, &identity, chunk).await;
    }
    try_reclaim_closed_sources(config, state, metrics).await;
}

fn remaining_is_open(file_size: u64, next_offset: u64) -> bool {
    file_size > next_offset
}

async fn enqueue_chunk(
    config: &Config,
    state: &Arc<Mutex<CollectorState>>,
    metrics: &Arc<Metrics>,
    identity: &crate::path::RecordingIdentity,
    chunk: FinalizedChunk,
) {
    let key = ChunkKey {
        path: chunk.path.clone(),
        offset: chunk.chunk_offset,
        length: chunk.header.chunk_size,
        chunk_index: chunk.chunk_index,
        start_time_ns: chunk.header.start_time_ns,
        duration_ns: chunk.header.duration_ns,
    };
    let inserted = {
        let mut locked = state.lock().await;
        locked.enqueue(key)
    };
    if !inserted {
        return;
    }
    metrics.inc(&metrics.finalized_chunks);
    let queued = state.lock().await.queued_len();
    metrics.set(&metrics.queued_chunks, queued as u64);
    tracing::info!(
        event = "jfr_chunk_finalized",
        node = %config.node_name,
        namespace = %identity.namespace,
        pod = %identity.pod_name,
        pod_uid = %identity.pod_uid,
        container = %identity.container,
        file = %identity.filename,
        chunk_index = chunk.chunk_index,
        chunk_offset = chunk.chunk_offset,
        chunk_size = chunk.header.chunk_size,
        chunk_start_time_ns = chunk.header.start_time_ns,
        chunk_duration_ns = chunk.header.duration_ns,
        mode = ?config.mode,
        "finalized JFR chunk"
    );
}

async fn drain_queue(
    config: &Config,
    state: &Arc<Mutex<CollectorState>>,
    metrics: &Arc<Metrics>,
    transport: Arc<dyn Transport>,
    readers: Arc<Semaphore>,
    in_flight: Arc<Semaphore>,
) {
    loop {
        let Ok(reader_permit) = readers.clone().try_acquire_owned() else {
            return;
        };
        let Ok(in_flight_permit) = in_flight.clone().try_acquire_owned() else {
            drop(reader_permit);
            return;
        };
        let key = {
            let mut locked = state.lock().await;
            locked.dequeue()
        };
        let Some(key) = key else {
            drop(reader_permit);
            drop(in_flight_permit);
            return;
        };
        let config = config.clone();
        let state = state.clone();
        let metrics = metrics.clone();
        let transport = transport.clone();
        tokio::spawn(async move {
            let _reader_permit = reader_permit;
            let _in_flight_permit = in_flight_permit;
            handle_chunk(config, state, metrics, transport, key).await;
        });
    }
}

async fn handle_chunk(
    config: Config,
    state: Arc<Mutex<CollectorState>>,
    metrics: Arc<Metrics>,
    transport: Arc<dyn Transport>,
    key: ChunkKey,
) {
    let identity = {
        let locked = state.lock().await;
        locked.identity_for(&key.path)
    };
    let Some(identity) = identity else {
        state.lock().await.release_in_flight(&key.path);
        return;
    };
    let chunk_index = key.offset.checked_div(HEADER_SIZE as u64).unwrap_or(0) as u32;
    match transport.publish(&config, &identity, chunk_index, &key).await {
        TransportResult::Accepted { bytes } | TransportResult::Duplicate { bytes } => {
            metrics.add(&metrics.bytes_transmitted, bytes);
            state.lock().await.acknowledge(key.clone());
            try_reclaim_closed_sources(&config, &state, &metrics).await;
        }
        TransportResult::Retry { reason } => {
            metrics.inc(&metrics.upload_retries);
            tracing::warn!(reason = %reason, path = %key.path.display(), "retrying chunk upload");
            tokio::time::sleep(backoff_delay(&config, 0)).await;
            state.lock().await.requeue(key);
        }
        TransportResult::Rejected { reason } => {
            metrics.inc(&metrics.upload_failures);
            tracing::error!(reason = %reason, path = %key.path.display(), "permanent chunk rejection");
            state.lock().await.mark_rejected(&key.path);
        }
        TransportResult::Logged => {
            let _ = AgentMode::LogOnly;
            state.lock().await.acknowledge(key);
        }
    }
}

pub async fn try_reclaim_closed_sources(
    config: &Config,
    state: &Arc<Mutex<CollectorState>>,
    metrics: &Arc<Metrics>,
) {
    if config.mode != AgentMode::Grpc || !config.delete_closed_files {
        return;
    }
    let candidates = state.lock().await.claim_reclaim_candidates();
    for path in candidates {
        if crate::path::identity_from_path(&config.recording_root, &path).is_err()
            || !crate::state::has_newer_rotated_sibling(&path).unwrap_or(false)
        {
            state.lock().await.release_reclaim(&path);
            continue;
        }
        let expected = state.lock().await.discovered_end(&path);
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                state.lock().await.forget_path(&path);
                continue;
            }
            Err(_) => {
                state.lock().await.release_reclaim(&path);
                continue;
            }
        };
        if metadata.len() != expected {
            state.lock().await.release_reclaim(&path);
            continue;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                metrics.inc(&metrics.source_files_deleted);
                tracing::info!(
                    event = "jfr_source_deleted",
                    path = %path.display(),
                    "deleted closed recording after durable acknowledgement"
                );
                state.lock().await.forget_path(&path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                state.lock().await.forget_path(&path);
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    path = %path.display(),
                    "unable to delete closed recording"
                );
                state.lock().await.release_reclaim(&path);
            }
        }
    }
}

fn backoff_delay(config: &Config, attempt: u32) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(5)).unwrap_or(32);
    let candidate = config.retry_initial_delay.saturating_mul(factor);
    candidate.min(config.retry_max_delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_backoff_never_exceeds_max() {
        let config = Config::from_vars(vec![
            ("JAFRA_RETRY_INITIAL_DELAY".into(), "1s".into()),
            ("JAFRA_RETRY_MAX_DELAY".into(), "4s".into()),
        ])
        .unwrap();
        assert_eq!(backoff_delay(&config, 10), Duration::from_secs(4));
    }
}
