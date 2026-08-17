use jafra_agent::proto::ingest::v1::OpenChunk;
use prost::Message;

#[test]
fn unknown_fields_do_not_prevent_decode() {
    let original = OpenChunk {
        protocol_version: 1,
        cluster_id: "local-demo".into(),
        node_name: "worker".into(),
        namespace: "default".into(),
        pod_name: String::new(),
        pod_uid: "uid".into(),
        container_name: "auth-cache".into(),
        recording_id: "local-demo/uid/auth-cache/profile-0.jfr".into(),
        physical_filename: "profile-0.jfr".into(),
        physical_file_sequence: 0,
        chunk_sequence: 0,
        chunk_offset: 0,
        chunk_length: 68,
        chunk_start_time_ns: 0,
        chunk_duration_ns: 0,
    };
    let mut encoded = original.encode_to_vec();
    encoded.extend_from_slice(&[0xC8, 0x06, 0x01]);
    let parsed = OpenChunk::decode(encoded.as_slice()).expect("decode");
    assert_eq!(parsed.protocol_version, 1);
    assert_eq!(parsed.cluster_id, "local-demo");
    assert_eq!(parsed.pod_uid, "uid");
}