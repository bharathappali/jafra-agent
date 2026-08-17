pub const HEADER_SIZE: usize = 68;
pub const MAGIC: &[u8; 4] = b"FLR\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub chunk_size: u64,
    pub constant_pool_offset: u64,
    pub metadata_offset: u64,
    pub start_time_ns: u64,
    pub duration_ns: u64,
    pub start_ticks: u64,
    pub ticks_per_second: u64,
    pub features: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeaderStatus {
    Finalized(ChunkHeader),
    Incomplete,
    Invalid(&'static str),
}

pub fn parse_header(bytes: &[u8], file_size: u64, chunk_offset: u64) -> HeaderStatus {
    if bytes.len() < HEADER_SIZE {
        return HeaderStatus::Incomplete;
    }
    if &bytes[0..4] != MAGIC {
        return HeaderStatus::Invalid("invalid magic");
    }
    let header = ChunkHeader {
        magic: [bytes[0], bytes[1], bytes[2], bytes[3]],
        version: read_u32(bytes, 4),
        chunk_size: read_u64(bytes, 8),
        constant_pool_offset: read_u64(bytes, 16),
        metadata_offset: read_u64(bytes, 24),
        start_time_ns: read_u64(bytes, 32),
        duration_ns: read_u64(bytes, 40),
        start_ticks: read_u64(bytes, 48),
        ticks_per_second: read_u64(bytes, 56),
        features: read_u32(bytes, 64),
    };
    if header.chunk_size < HEADER_SIZE as u64 {
        return HeaderStatus::Invalid("chunk size is smaller than the header");
    }
    if header.constant_pool_offset == 0 || header.metadata_offset == 0 {
        return HeaderStatus::Incomplete;
    }
    match chunk_offset.checked_add(header.chunk_size) {
        Some(end) if end <= file_size => HeaderStatus::Finalized(header),
        Some(_) => HeaderStatus::Incomplete,
        None => HeaderStatus::Invalid("chunk end overflow"),
    }
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("u32 slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
}

pub fn write_finalized_header(chunk_size: u64, start_time_ns: u64, duration_ns: u64) -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(MAGIC);
    header[4..8].copy_from_slice(&2u32.to_be_bytes());
    header[8..16].copy_from_slice(&chunk_size.to_be_bytes());
    header[16..24].copy_from_slice(&68u64.to_be_bytes());
    header[24..32].copy_from_slice(&68u64.to_be_bytes());
    header[32..40].copy_from_slice(&start_time_ns.to_be_bytes());
    header[40..48].copy_from_slice(&duration_ns.to_be_bytes());
    header[48..56].copy_from_slice(&0u64.to_be_bytes());
    header[56..64].copy_from_slice(&1_000_000_000u64.to_be_bytes());
    header[64..68].copy_from_slice(&0u32.to_be_bytes());
    header
}

pub fn write_placeholder_header(claimed_size: u64) -> [u8; HEADER_SIZE] {
    let mut header = write_finalized_header(claimed_size, 0, 0);
    header[16..24].copy_from_slice(&0u64.to_be_bytes());
    header[24..32].copy_from_slice(&0u64.to_be_bytes());
    header
}
