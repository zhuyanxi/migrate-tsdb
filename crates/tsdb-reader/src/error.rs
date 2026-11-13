//! Error type shared by every parser in the crate.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong while reading a block.
///
/// The variants are deliberately coarse: callers usually only need to tell
/// "the data is corrupt" from "the file is not what we expected" from "the I/O
/// layer failed". All of them are terminal for a single block, so the usual
/// response is to skip or quarantine that block and continue.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wraps a filesystem or memory-mapping failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The file does not follow the Prometheus on-disk layout.
    #[error("invalid format: {0}")]
    Format(String),
    /// CRC32C mismatch in the named section (`index toc`, `index series`,
    /// `index symbols`, `chunk` or `tombstones`).
    #[error("checksum mismatch: {0}")]
    Checksum(&'static str),
    /// A read ran past the end of the buffer, at the given absolute offset.
    #[error("unexpected end of data at byte {0}")]
    Eof(usize),
    /// A chunk payload could not be decoded.
    #[error("chunk decode error: {0}")]
    Decode(String),
    /// A format version or chunk encoding this crate does not implement.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    /// Builds [`Error::Format`] from anything displayable.
    pub fn format(msg: impl fmt::Display) -> Self {
        Error::Format(msg.to_string())
    }

    /// Builds [`Error::Decode`] from anything displayable.
    pub fn decode(msg: impl fmt::Display) -> Self {
        Error::Decode(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_keep_the_message() {
        assert!(matches!(Error::format("x"), Error::Format(ref m) if m == "x"));
        assert!(matches!(Error::decode("y"), Error::Decode(ref m) if m == "y"));
    }

    #[test]
    fn io_errors_keep_their_source() {
        let error = Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert!(matches!(error, Error::Io(_)));
        assert!(std::error::Error::source(&error).is_some());
        assert!(error.to_string().contains("gone"));
    }
}
