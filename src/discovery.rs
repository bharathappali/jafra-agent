use std::collections::HashSet;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use crate::path::recording_volume_relative_path;

pub fn discover_recording_volume_roots(root: &Path, volume_name: &str) -> std::io::Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let relative = recording_volume_relative_path(volume_name);
    let mut roots = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let recording_root = entry.path().join(&relative);
        if recording_root.is_dir() {
            roots.push(recording_root);
        }
    }
    roots.sort();
    Ok(roots)
}

pub fn discover_jfr_files(root: &Path, volume_name: &str) -> std::io::Result<Vec<PathBuf>> {
    let volume_roots = discover_recording_volume_roots(root, volume_name)?;
    if volume_roots.is_empty() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for volume_root in volume_roots {
        if !volume_root.exists() {
            continue;
        }
        for entry in WalkDir::new(&volume_root).follow_links(false) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("jfr") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

pub fn discover_jfr_files_under(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jfr")
        {
            files.push(entry.into_path());
        }
    }
    files.sort();
    Ok(files)
}

pub fn existing_set(root: &Path, volume_name: &str) -> std::io::Result<HashSet<PathBuf>> {
    Ok(discover_jfr_files(root, volume_name)?.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn discovers_recording_volume_roots() {
        let dir = tempfile::tempdir().unwrap();
        let one = dir.path().join("pod-a/volumes/kubernetes.io~empty-dir/jafra-recordings");
        let two = dir.path().join("pod-b/volumes/kubernetes.io~empty-dir/jafra-recordings");
        fs::create_dir_all(&one).unwrap();
        fs::create_dir_all(&two).unwrap();
        fs::create_dir_all(dir.path().join("pod-c/volumes/kubernetes.io~empty-dir/other")).unwrap();

        let roots = discover_recording_volume_roots(dir.path(), "jafra-recordings").unwrap();
        assert_eq!(roots.len(), 2);
        assert!(roots.contains(&one));
        assert!(roots.contains(&two));
    }

    #[test]
    fn discovers_jfr_files_only_under_recording_roots() {
        let dir = tempfile::tempdir().unwrap();
        let recording = dir.path().join("pod-a/volumes/kubernetes.io~empty-dir/jafra-recordings/ns/uid/app");
        fs::create_dir_all(&recording).unwrap();
        fs::write(recording.join("profile-0.jfr"), [0u8; 4]).unwrap();
        let other = dir.path().join("pod-b/volumes/kubernetes.io~empty-dir/other");
        fs::create_dir_all(&other).unwrap();
        fs::write(other.join("profile-0.jfr"), [0u8; 4]).unwrap();

        let files = discover_jfr_files(dir.path(), "jafra-recordings").unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("profile-0.jfr"));
        assert!(files[0].to_string_lossy().contains("kubernetes.io~empty-dir/jafra-recordings/"));
    }
}
