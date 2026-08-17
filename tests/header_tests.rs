use jafra_agent::jfr::header::{parse_header, write_finalized_header, write_placeholder_header, HeaderStatus, HEADER_SIZE, MAGIC};

#[test]
fn valid_finalized_header() {
    let header = write_finalized_header(128, 10, 5_000_000_000);
    match parse_header(&header, 128, 0) {
        HeaderStatus::Finalized(parsed) => {
            assert_eq!(&parsed.magic, MAGIC);
            assert_eq!(parsed.chunk_size, 128);
            assert_eq!(parsed.constant_pool_offset, 68);
            assert_eq!(parsed.duration_ns, 5_000_000_000);
        }
        other => panic!("expected finalized, got {other:?}"),
    }
}

#[test]
fn incomplete_placeholder_header() {
    let header = write_placeholder_header(128);
    assert!(matches!(parse_header(&header, 128, 0), HeaderStatus::Incomplete));
}

#[test]
fn zero_constant_pool_offset() {
    let mut header = write_finalized_header(128, 0, 0);
    header[16..24].copy_from_slice(&0u64.to_be_bytes());
    assert!(matches!(parse_header(&header, 128, 0), HeaderStatus::Incomplete));
}

#[test]
fn zero_metadata_offset() {
    let mut header = write_finalized_header(128, 0, 0);
    header[24..32].copy_from_slice(&0u64.to_be_bytes());
    assert!(matches!(parse_header(&header, 128, 0), HeaderStatus::Incomplete));
}

#[test]
fn chunk_extending_beyond_file_size() {
    let header = write_finalized_header(256, 0, 0);
    assert!(matches!(parse_header(&header, 128, 0), HeaderStatus::Incomplete));
}

#[test]
fn invalid_magic() {
    let mut header = write_finalized_header(128, 0, 0);
    header[0] = b'X';
    assert!(matches!(parse_header(&header, 128, 0), HeaderStatus::Invalid(_)));
}

#[test]
fn truncated_header() {
    let header = write_finalized_header(128, 0, 0);
    assert!(matches!(
        parse_header(&header[..HEADER_SIZE - 1], 128, 0),
        HeaderStatus::Incomplete
    ));
}
