use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Metrics {
    pub files_discovered: AtomicU64,
    pub finalized_chunks: AtomicU64,
    pub incomplete_chunks: AtomicU64,
    pub queued_chunks: AtomicU64,
    pub active_readers: AtomicU64,
    pub bytes_transmitted: AtomicU64,
    pub upload_retries: AtomicU64,
    pub upload_failures: AtomicU64,
    pub watcher_overflows: AtomicU64,
    pub active_volume_watches: AtomicU64,
    pub full_rescans: AtomicU64,
    pub grpc_connected: AtomicU64,
    pub source_files_deleted: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc(&self, field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, field: &AtomicU64, value: u64) {
        field.fetch_add(value, Ordering::Relaxed);
    }

    pub fn set(&self, field: &AtomicU64, value: u64) {
        field.store(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "files_discovered": self.files_discovered.load(Ordering::Relaxed),
            "finalized_chunks": self.finalized_chunks.load(Ordering::Relaxed),
            "incomplete_chunks": self.incomplete_chunks.load(Ordering::Relaxed),
            "queued_chunks": self.queued_chunks.load(Ordering::Relaxed),
            "active_readers": self.active_readers.load(Ordering::Relaxed),
            "bytes_transmitted": self.bytes_transmitted.load(Ordering::Relaxed),
            "upload_retries": self.upload_retries.load(Ordering::Relaxed),
            "upload_failures": self.upload_failures.load(Ordering::Relaxed),
            "watcher_overflows": self.watcher_overflows.load(Ordering::Relaxed),
            "active_volume_watches": self.active_volume_watches.load(Ordering::Relaxed),
            "full_rescans": self.full_rescans.load(Ordering::Relaxed),
            "grpc_connected": self.grpc_connected.load(Ordering::Relaxed),
            "source_files_deleted": self.source_files_deleted.load(Ordering::Relaxed),
        })
    }
}

pub fn log_metrics(node: &str, metrics: &Metrics) {
    tracing::info!(
        event = "jafra_agent_metrics",
        node = node,
        metrics = %metrics.snapshot(),
        "collector metrics"
    );
    let _ = Path::new(".");
}
