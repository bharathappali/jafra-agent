use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::header::{parse_header, ChunkHeader, HeaderStatus, HEADER_SIZE};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedChunk {
    pub path: PathBuf,
    pub chunk_index: u32,
    pub chunk_offset: u64,
    pub header: ChunkHeader,
}

#[derive(Clone, Debug)]
pub struct FileCursor {
    pub path: PathBuf,
    pub next_offset: u64,
    pub last_size: u64,
    pub next_index: u32,
}

impl FileCursor {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            next_offset: 0,
            last_size: 0,
            next_index: 0,
        }
    }
}

pub fn scan_file(path: &Path, cursor: &mut FileCursor, file_size: u64) -> std::io::Result<Vec<FinalizedChunk>> {
    cursor.last_size = file_size;
    if file_size.saturating_sub(cursor.next_offset) < HEADER_SIZE as u64 {
        return Ok(Vec::new());
    }

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(cursor.next_offset))?;
    let mut discovered = Vec::new();
    loop {
        let remaining = file_size.saturating_sub(cursor.next_offset);
        if remaining < HEADER_SIZE as u64 {
            break;
        }
        let mut header_bytes = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        match parse_header(&header_bytes, file_size, cursor.next_offset) {
            HeaderStatus::Finalized(header) => {
                discovered.push(FinalizedChunk {
                    path: path.to_path_buf(),
                    chunk_index: cursor.next_index,
                    chunk_offset: cursor.next_offset,
                    header: header.clone(),
                });
                cursor.next_offset += header.chunk_size;
                cursor.next_index += 1;
                file.seek(SeekFrom::Start(cursor.next_offset))?;
            }
            HeaderStatus::Incomplete | HeaderStatus::Invalid(_) => break,
        }
    }
    Ok(discovered)
}
