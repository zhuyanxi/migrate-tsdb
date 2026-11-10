pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("reader error: {0}")]
    Reader(#[from] tsdb_reader::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("sink error: {0}")]
    Sink(String),
    #[error("state error: {0}")]
    State(String),
    #[error("job aborted: {0}")]
    Aborted(String),
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn config(msg: impl std::fmt::Display) -> Self {
        Error::Config(msg.to_string())
    }
    pub fn sink(msg: impl std::fmt::Display) -> Self {
        Error::Sink(msg.to_string())
    }
    pub fn state(msg: impl std::fmt::Display) -> Self {
        Error::State(msg.to_string())
    }
}
