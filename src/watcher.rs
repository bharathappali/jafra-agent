use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::discovery::discover_recording_volume_roots;
use crate::path::kubelet_volume_marker;

#[derive(Debug, Clone)]
pub enum WatchWake {
    RecordingPath(PathBuf),
    PodTreeChange,
    Overflow,
}

pub struct RecordingWatcher {
    inner: RecommendedWatcher,
    root: PathBuf,
    volume_name: String,
    watched_roots: HashSet<PathBuf>,
    shallow_watch_active: bool,
}

impl RecordingWatcher {
    pub fn new(
        root: PathBuf,
        volume_name: String,
        tx: mpsc::Sender<WatchWake>,
    ) -> notify::Result<Self> {
        let marker = kubelet_volume_marker(&volume_name);
        let root_for_callback = root.clone();
        let marker_for_callback = marker.clone();
        let sender = tx;
        let inner = RecommendedWatcher::new(
            move |result: Result<Event, notify::Error>| match result {
                Ok(event) => {
                    if event.need_rescan() {
                        wake(&sender, WatchWake::Overflow);
                        return;
                    }
                    for path in event.paths {
                        let path_str = path.to_string_lossy();
                        if path_str.contains(&marker_for_callback) {
                            wake(&sender, WatchWake::RecordingPath(path));
                        } else if is_pod_tree_change(&path, &root_for_callback) {
                            wake(&sender, WatchWake::PodTreeChange);
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "filesystem watcher error");
                    wake(&sender, WatchWake::Overflow);
                }
            },
            NotifyConfig::default().with_poll_interval(Duration::from_secs(2)),
        )?;

        let mut watcher = Self {
            inner,
            root,
            volume_name,
            watched_roots: HashSet::new(),
            shallow_watch_active: false,
        };
        if let Err(error) = watcher.ensure_shallow_watch() {
            tracing::warn!(error = %error, "unable to watch kubelet pod tree");
        }
        Ok(watcher)
    }

    pub fn active_watch_count(&self) -> usize {
        self.watched_roots.len()
    }

    pub fn sync_volume_roots(&mut self) -> std::io::Result<()> {
        if let Err(error) = self.ensure_shallow_watch() {
            tracing::warn!(error = %error, "unable to ensure kubelet pod tree watch");
        }

        let discovered = discover_recording_volume_roots(&self.root, &self.volume_name)?;
        let discovered_set: HashSet<_> = discovered.into_iter().collect();

        let to_add: Vec<_> = discovered_set
            .difference(&self.watched_roots)
            .cloned()
            .collect();
        for path in to_add {
            match self.inner.watch(&path, RecursiveMode::Recursive) {
                Ok(()) => {
                    tracing::info!(
                        event = "jafra_recording_watch_added",
                        path = %path.display(),
                        "watching Jafra recording volume"
                    );
                    self.watched_roots.insert(path);
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        path = %path.display(),
                        "unable to watch Jafra recording volume"
                    );
                }
            }
        }

        let stale: Vec<_> = self
            .watched_roots
            .difference(&discovered_set)
            .cloned()
            .collect();
        for path in stale {
            if let Err(error) = self.inner.unwatch(&path) {
                tracing::debug!(
                    error = %error,
                    path = %path.display(),
                    "unable to unwatch stale recording volume"
                );
            }
            tracing::info!(
                event = "jafra_recording_watch_removed",
                path = %path.display(),
                "stopped watching Jafra recording volume"
            );
            self.watched_roots.remove(&path);
        }

        Ok(())
    }

    fn ensure_shallow_watch(&mut self) -> notify::Result<()> {
        if self.shallow_watch_active || !self.root.exists() {
            return Ok(());
        }
        self.inner.watch(&self.root, RecursiveMode::NonRecursive)?;
        self.shallow_watch_active = true;
        tracing::info!(
            event = "jafra_pod_tree_watch_added",
            path = %self.root.display(),
            "watching kubelet pod tree for new recordings"
        );
        Ok(())
    }
}

fn is_pod_tree_change(path: &Path, root: &Path) -> bool {
    path == root || path.parent().is_some_and(|parent| parent == root)
}

fn wake(sender: &mpsc::Sender<WatchWake>, wake: WatchWake) {
    match sender.try_send(wake) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            let _ = sender.try_send(WatchWake::Overflow);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_pod_tree_changes() {
        let root = PathBuf::from("/var/lib/kubelet/pods");
        assert!(is_pod_tree_change(&root, &root));
        assert!(is_pod_tree_change(
            &PathBuf::from("/var/lib/kubelet/pods/pod-uid"),
            &root
        ));
        assert!(!is_pod_tree_change(
            &PathBuf::from("/var/lib/kubelet/pods/pod-uid/volumes/kubernetes.io~empty-dir/jafra-recordings/ns/uid/app/profile-0.jfr"),
            &root
        ));
    }
}
