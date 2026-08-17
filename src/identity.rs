use sha2::{Digest, Sha256};

pub const PROTOCOL_VERSION: u32 = 1;
pub const CHECKSUM_ALGORITHM: &str = "sha256";

pub fn chunk_id(
    cluster_id: &str,
    pod_uid: &str,
    container: &str,
    filename: &str,
    chunk_offset: u64,
    chunk_length: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(cluster_id.as_bytes());
    hasher.update(b"|");
    hasher.update(pod_uid.as_bytes());
    hasher.update(b"|");
    hasher.update(container.as_bytes());
    hasher.update(b"|");
    hasher.update(filename.as_bytes());
    hasher.update(b"|");
    hasher.update(chunk_offset.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(chunk_length.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

pub fn recording_id(cluster_id: &str, pod_uid: &str, container: &str, filename: &str) -> String {
    format!("{cluster_id}/{pod_uid}/{container}/{filename}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_deterministic() {
        let first = chunk_id("local-demo", "uid", "auth-cache", "profile-0.jfr", 0, 100);
        let second = chunk_id("local-demo", "uid", "auth-cache", "profile-0.jfr", 0, 100);
        assert_eq!(first, second);
        assert_ne!(
            first,
            chunk_id("local-demo", "uid", "auth-cache", "profile-0.jfr", 68, 100)
        );
    }
}
