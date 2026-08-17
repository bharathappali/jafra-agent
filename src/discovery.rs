use std::collections::HashSet;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

pub fn discover_jfr_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file() {
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("jfr") {
                files.push(entry.into_path());
            }
        }
    }
    files.sort();
    Ok(files)
}

pub fn existing_set(root: &Path) -> std::io::Result<HashSet<PathBuf>> {
    Ok(discover_jfr_files(root)?.into_iter().collect())
}
