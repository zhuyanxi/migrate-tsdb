use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid format: {0}")]
    Format(String),
    #[error("checksum mismatch: {0}")]
    Checksum(&'static str),
    #[error("unexpected end of data at byte {0}")]
    Eof(usize),
    #[error("chunk decode error: {0}")]
    Decode(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    pub fn format(msg: impl fmt::Display) -> Self {
        Error::Format(msg.to_string())
    }

    pub fn decode(msg: impl fmt::Display) -> Self {
        Error::Decode(msg.to_string())
    }
}
