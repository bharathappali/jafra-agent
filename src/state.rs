use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::jfr::scanner::FileCursor;
use crate::path::RecordingIdentity;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
    pub chunk_index: u32,
    pub start_time_ns: u64,
    pub duration_ns: u64,
}

#[derive(Clone, Debug)]
pub struct FileState {
    pub path: PathBuf,
    pub identity: RecordingIdentity,
    pub cursor: FileCursor,
    pub last_scan: Instant,
    pub closed: bool,
    pub discovered_end: u64,
    pub rejected: bool,
    pub acked: BTreeMap<u64, u64>,
}

#[derive(Debug, Default)]
pub struct CollectorState {
    files: HashMap<PathBuf, FileState>,
    seen: HashSet<ChunkKey>,
    acknowledged: HashSet<ChunkKey>,
    queued: HashSet<ChunkKey>,
    in_flight_files: HashSet<PathBuf>,
    reclaiming: HashSet<PathBuf>,
    queue: VecDeque<ChunkKey>,
}

impl CollectorState {
    pub fn file_mut(&mut self, path: &Path, identity: RecordingIdentity) -> &mut FileState {
        self.files.entry(path.to_path_buf()).or_insert_with(|| FileState {
            path: path.to_path_buf(),
            identity,
            cursor: FileCursor::new(path.to_path_buf()),
            last_scan: Instant::now(),
            closed: false,
            discovered_end: 0,
            rejected: false,
            acked: BTreeMap::new(),
        })
    }

    pub fn enqueue(&mut self, key: ChunkKey) -> bool {
        if self.seen.contains(&key) || self.acknowledged.contains(&key) || self.queued.contains(&key) {
            return false;
        }
        self.seen.insert(key.clone());
        self.queued.insert(key.clone());
        self.queue.push_back(key);
        true
    }

    pub fn dequeue(&mut self) -> Option<ChunkKey> {
        let index = self
            .queue
            .iter()
            .position(|key| !self.in_flight_files.contains(&key.path))?;
        let key = self.queue.remove(index)?;
        self.queued.remove(&key);
        self.in_flight_files.insert(key.path.clone());
        Some(key)
    }

    pub fn requeue(&mut self, key: ChunkKey) {
        self.in_flight_files.remove(&key.path);
        if self.acknowledged.contains(&key) || self.queued.contains(&key) {
            return;
        }
        self.queued.insert(key.clone());
        self.queue.push_back(key);
    }

    pub fn acknowledge(&mut self, key: ChunkKey) {
        self.in_flight_files.remove(&key.path);
        self.queued.remove(&key);
        if let Some(file) = self.files.get_mut(&key.path) {
            file.acked.insert(key.offset, key.length);
        }
        self.acknowledged.insert(key);
    }

    pub fn mark_rejected(&mut self, path: &Path) {
        self.in_flight_files.remove(path);
        if let Some(file) = self.files.get_mut(path) {
            file.rejected = true;
        }
    }

    pub fn mark_scan(&mut self, path: &Path, discovered_end: u64, closed: bool) {
        if let Some(file) = self.files.get_mut(path) {
            file.discovered_end = discovered_end;
            file.closed = closed;
        }
    }

    pub fn fully_acked_closed(&self, path: &Path) -> bool {
        let Some(file) = self.files.get(path) else {
            return false;
        };
        if file.rejected || !file.closed || file.discovered_end == 0 {
            return false;
        }
        if self.queue.iter().any(|key| key.path == path) {
            return false;
        }
        covering_length(&file.acked) == file.discovered_end
    }

    pub fn reclaim_candidates(&self) -> Vec<PathBuf> {
        self.files
            .keys()
            .filter(|path| self.fully_acked_closed(path) && !self.reclaiming.contains(*path))
            .cloned()
            .collect()
    }

    pub fn claim_reclaim_candidates(&mut self) -> Vec<PathBuf> {
        let candidates = self.reclaim_candidates();
        for path in &candidates {
            self.reclaiming.insert(path.clone());
        }
        candidates
    }

    pub fn release_reclaim(&mut self, path: &Path) {
        self.reclaiming.remove(path);
    }

    pub fn discovered_end(&self, path: &Path) -> u64 {
        self.files.get(path).map(|file| file.discovered_end).unwrap_or(0)
    }

    pub fn forget_path(&mut self, path: &Path) {
        self.files.remove(path);
        self.in_flight_files.remove(path);
        self.reclaiming.remove(path);
        self.queued.retain(|key| key.path != path);
        self.queue.retain(|key| key.path != path);
        self.seen.retain(|key| key.path != path);
        self.acknowledged.retain(|key| key.path != path);
    }

    pub fn covering_length_for(&self, path: &Path) -> u64 {
        self.files
            .get(path)
            .map(|file| covering_length(&file.acked))
            .unwrap_or(0)
    }

    pub fn release_in_flight(&mut self, path: &Path) {
        self.in_flight_files.remove(path);
    }

    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_acknowledged(&self, key: &ChunkKey) -> bool {
        self.acknowledged.contains(key)
    }

    pub fn identity_for(&self, path: &Path) -> Option<RecordingIdentity> {
        self.files.get(path).map(|file| file.identity.clone())
    }

    pub fn forget_missing(&mut self, existing: &HashSet<PathBuf>) {
        self.files.retain(|path, _| existing.contains(path));
        self.in_flight_files.retain(|path| existing.contains(path));
        self.reclaiming.retain(|path| existing.contains(path));
    }
}

pub fn covering_length(acked: &BTreeMap<u64, u64>) -> u64 {
    let mut cursor = 0u64;
    for (offset, length) in acked {
        if *offset != cursor {
            break;
        }
        cursor = cursor.saturating_add(*length);
    }
    cursor
}

pub fn has_newer_rotated_sibling(path: &Path) -> std::io::Result<bool> {
    let Some(dir) = path.parent() else {
        return Ok(false);
    };
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return Ok(false);
    };
    let sequence = crate::path::file_sequence(name);
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if file_name.ends_with(".jfr") && crate::path::file_sequence(file_name) > sequence {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::RecordingIdentity;

    fn key(path: &str, offset: u64) -> ChunkKey {
        ChunkKey {
            path: PathBuf::from(path),
            offset,
            length: 68,
            chunk_index: 0,
            start_time_ns: 0,
            duration_ns: 0,
        }
    }

    fn identity() -> RecordingIdentity {
        RecordingIdentity {
            namespace: "default".into(),
            pod_name: "pod".into(),
            pod_uid: "uid".into(),
            container: "c".into(),
            filename: "profile-0.jfr".into(),
        }
    }

    #[test]
    fn dequeue_serializes_same_file_and_allows_other_files() {
        let mut state = CollectorState::default();
        let _ = state.file_mut(Path::new("/a.jfr"), identity());
        assert!(state.enqueue(key("/a.jfr", 0)));
        assert!(state.enqueue(key("/a.jfr", 68)));
        assert!(state.enqueue(key("/b.jfr", 0)));
        let first = state.dequeue().unwrap();
        assert_eq!(first.path, PathBuf::from("/a.jfr"));
        assert_eq!(first.offset, 0);
        let second = state.dequeue().unwrap();
        assert_eq!(second.path, PathBuf::from("/b.jfr"));
        assert!(state.dequeue().is_none());
        state.acknowledge(first);
        let third = state.dequeue().unwrap();
        assert_eq!(third.path, PathBuf::from("/a.jfr"));
        assert_eq!(third.offset, 68);
    }

    #[test]
    fn covering_length_stops_at_the_first_hole() {
        let mut acked = BTreeMap::new();
        acked.insert(0, 10);
        acked.insert(10, 5);
        assert_eq!(covering_length(&acked), 15);
        acked.insert(20, 3);
        assert_eq!(covering_length(&acked), 15);
    }

    #[test]
    fn fully_acked_closed_requires_contiguous_coverage() {
        let mut state = CollectorState::default();
        let path = Path::new("/profile-0.jfr");
        let file = state.file_mut(path, identity());
        file.closed = true;
        file.discovered_end = 136;
        file.acked.insert(0, 68);
        assert!(!state.fully_acked_closed(path));
        state.files.get_mut(path).unwrap().acked.insert(68, 68);
        assert!(state.fully_acked_closed(path));
    }

    #[test]
    fn claim_reclaim_candidates_is_exclusive() {
        let mut state = CollectorState::default();
        let path = Path::new("/profile-0.jfr");
        let file = state.file_mut(path, identity());
        file.closed = true;
        file.discovered_end = 68;
        file.acked.insert(0, 68);
        let first = state.claim_reclaim_candidates();
        assert_eq!(first, vec![path.to_path_buf()]);
        assert!(state.claim_reclaim_candidates().is_empty());
        state.release_reclaim(path);
        assert_eq!(state.claim_reclaim_candidates(), vec![path.to_path_buf()]);
    }
}
