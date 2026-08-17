use std::path::{Path, PathBuf};
use std::time::Duration;

use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum WatchWake {
    Path(PathBuf),
    Overflow,
}

pub fn spawn_watcher(
    root: PathBuf,
    tx: mpsc::Sender<WatchWake>,
) -> notify::Result<RecommendedWatcher> {
    let sender = tx;
    let mut watcher = RecommendedWatcher::new(
        move |result: Result<Event, notify::Error>| match result {
            Ok(event) => {
                if event.need_rescan() {
                    let _ = sender.blocking_send(WatchWake::Overflow);
                    return;
                }
                for path in event.paths {
                    let _ = sender.blocking_send(WatchWake::Path(path));
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "filesystem watcher error");
                let _ = sender.blocking_send(WatchWake::Overflow);
            }
        },
        NotifyConfig::default().with_poll_interval(Duration::from_secs(2)),
    )?;
    if root.exists() {
        watcher.watch(&root, RecursiveMode::Recursive)?;
    }
    Ok(watcher)
}

pub fn watch_if_needed(watcher: &mut RecommendedWatcher, root: &Path) {
    if root.exists() {
        let _ = watcher.watch(root, RecursiveMode::Recursive);
    }
}
