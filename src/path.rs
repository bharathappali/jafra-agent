use std::path::{Component, Path, PathBuf};

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingIdentity {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub container: String,
    pub filename: String,
}

#[derive(Debug, Error)]
pub enum PathError {
    #[error("path {0} is outside the recording root")]
    OutsideRoot(String),
    #[error("path {0} does not match namespace/podUID/container/filename")]
    InvalidLayout(String),
    #[error("path {0} contains an invalid component")]
    InvalidComponent(String),
}

pub fn identity_from_path(root: &Path, path: &Path) -> Result<RecordingIdentity, PathError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| PathError::OutsideRoot(path.display().to_string()))?;
    identity_from_relative_layout(relative, path)
}

pub fn kubelet_volume_marker(volume_name: &str) -> String {
    format!("kubernetes.io~empty-dir/{volume_name}/")
}

pub fn recording_volume_relative_path(volume_name: &str) -> String {
    format!("volumes/kubernetes.io~empty-dir/{volume_name}")
}

pub fn identity_from_recording_path(path: &Path, volume_name: &str) -> Result<RecordingIdentity, PathError> {
    let marker = kubelet_volume_marker(volume_name);
    let path_str = path.to_string_lossy();
    let relative = path_str
        .split_once(&marker)
        .map(|(_, suffix)| suffix)
        .ok_or_else(|| PathError::InvalidLayout(path.display().to_string()))?;
    identity_from_relative_layout(Path::new(relative), path)
}

fn identity_from_relative_layout(relative: &Path, full_path: &Path) -> Result<RecordingIdentity, PathError> {
    let parts: Vec<_> = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            Component::CurDir => None,
            _ => Some(String::new()),
        })
        .collect();
    if parts.len() != 4 {
        return Err(PathError::InvalidLayout(full_path.display().to_string()));
    }
    for part in &parts {
        validate_component(part)?;
    }
    let mut identity = RecordingIdentity {
        namespace: parts[0].clone(),
        pod_name: String::new(),
        pod_uid: parts[1].clone(),
        container: parts[2].clone(),
        filename: parts[3].clone(),
    };
    if let Some(parent) = full_path.parent() {
        identity.pod_name = load_pod_name(parent).unwrap_or_default();
    }
    Ok(identity)
}

#[derive(Clone, Debug, serde::Deserialize)]
struct WorkloadIdentityFile {
    #[serde(rename = "podName")]
    pod_name: Option<String>,
}

pub fn load_pod_name(dir: &Path) -> Option<String> {
    let bytes = std::fs::read(dir.join(".jafra-identity.json")).ok()?;
    let parsed: WorkloadIdentityFile = serde_json::from_slice(&bytes).ok()?;
    let name = parsed.pod_name?.trim().to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

pub fn join_under_root(root: &Path, relative: &Path) -> Result<PathBuf, PathError> {
    if relative.is_absolute() {
        return Err(PathError::OutsideRoot(relative.display().to_string()));
    }
    for component in relative.components() {
        match component {
            Component::Normal(value) => validate_component(&value.to_string_lossy())?,
            Component::CurDir => {}
            _ => return Err(PathError::InvalidComponent(relative.display().to_string())),
        }
    }
    Ok(root.join(relative))
}

fn validate_component(value: &str) -> Result<(), PathError> {
    if value.is_empty() || value == "." || value == ".." || value.contains('/') || value.contains('\\') {
        return Err(PathError::InvalidComponent(value.to_string()));
    }
    Ok(())
}

pub fn file_sequence(filename: &str) -> u32 {
    filename
        .strip_prefix("profile-")
        .and_then(|rest| rest.strip_suffix(".jfr"))
        .and_then(|digits| digits.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_kubelet_emptydir_path() {
        let path = Path::new(
            "/var/lib/kubelet/pods/abc-uid/volumes/kubernetes.io~empty-dir/jafra-recordings/default/pod-uid/auth-cache/profile-0.jfr",
        );
        let identity = identity_from_recording_path(path, "jafra-recordings").unwrap();
        assert_eq!(identity.namespace, "default");
        assert_eq!(identity.pod_uid, "pod-uid");
        assert_eq!(identity.container, "auth-cache");
        assert_eq!(identity.filename, "profile-0.jfr");
    }

    #[test]
    fn extracts_controlled_path() {
        let identity = identity_from_path(
            Path::new("/jfr-data"),
            Path::new("/jfr-data/default/pod-uid/auth-cache/profile-0.jfr"),
        )
        .unwrap();
        assert_eq!(identity.namespace, "default");
        assert_eq!(identity.pod_uid, "pod-uid");
        assert_eq!(identity.container, "auth-cache");
        assert_eq!(identity.filename, "profile-0.jfr");
        assert!(identity.pod_name.is_empty());
        assert_eq!(file_sequence(identity.filename.as_str()), 0);
    }

    #[test]
    fn loads_pod_name_from_identity_file() {
        let dir = std::env::temp_dir().join(format!("jafra-identity-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("default/pod-uid/auth-cache")).unwrap();
        let recording_dir = dir.join("default/pod-uid/auth-cache");
        std::fs::write(
            recording_dir.join(".jafra-identity.json"),
            r#"{"namespace":"default","podName":"auth-cache-abc","podUid":"pod-uid","container":"auth-cache"}"#,
        )
        .unwrap();
        std::fs::write(recording_dir.join("profile-0.jfr"), [0u8; 4]).unwrap();
        let identity = identity_from_path(&dir, &recording_dir.join("profile-0.jfr")).unwrap();
        assert_eq!(identity.pod_name, "auth-cache-abc");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_traversal() {
        let error = identity_from_path(
            Path::new("/jfr-data"),
            Path::new("/jfr-data/default/../etc/passwd"),
        );
        assert!(error.is_err());
        assert!(join_under_root(Path::new("/jfr-data"), Path::new("../escape")).is_err());
    }
}
