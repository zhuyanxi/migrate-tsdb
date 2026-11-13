//! Corruption handling tests for chunk segments.

#![cfg(feature = "crc")]

use std::io::Write;
use std::path::{Path, PathBuf};

use tsdb_reader::chunks::ChunkReader;

fn temp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "tsdb-reader-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn write_segment(dir: &Path, bad_crc: bool) {
    let mut f = std::fs::File::create(dir.join("000001")).unwrap();
    // magic + version + padding
    f.write_all(&0x85BD40DDu32.to_be_bytes()).unwrap();
    f.write_all(&[1, 0, 0, 0]).unwrap();
    // len=2 (encoding + one data byte? reader treats len as body bytes after
    // encoding, so write encoding + 2 data bytes)
    f.write_all(&[2]).unwrap();
    f.write_all(&[1]).unwrap(); // encoding XOR
    f.write_all(&[0xAA, 0xBB]).unwrap();
    let crc = if bad_crc {
        0u32
    } else {
        crc32c::crc32c(&[1, 0xAA, 0xBB])
    };
    f.write_all(&crc.to_be_bytes()).unwrap();
}

#[test]
fn detects_bad_chunk_crc() {
    let dir = temp_dir("bad-crc");
    write_segment(&dir, true);
    let reader = ChunkReader::open(&dir, true, false).unwrap();
    let err = reader.chunk(8).unwrap_err();
    assert!(
        matches!(err, tsdb_reader::Error::Checksum("chunk")),
        "unexpected error: {err}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn can_skip_crc_verification() {
    let dir = temp_dir("skip-crc");
    write_segment(&dir, true);
    let reader = ChunkReader::open(&dir, false, false).unwrap();
    let slice = reader.chunk(8).unwrap();
    assert_eq!(slice.encoding, 1);
    assert_eq!(slice.data, &[0xAA, 0xBB]);
    std::fs::remove_dir_all(&dir).unwrap();
}
