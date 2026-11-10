//! Migration orchestration: planning, bounded-memory pipeline, checkpoints,
//! and columnar sinks.

pub mod batch;
pub mod checkpoint;
pub mod config;
pub mod control;
pub mod duckdb;
pub mod engine;
pub mod error;
pub mod manifest;
pub mod planner;
pub mod sink;
pub mod stats;

pub use config::{
    Compression, EngineOptions, FilterConfig, JobConfig, LabelMatcher, SinkConfig, SinkKind,
};
pub use control::{Control, JobState};
pub use engine::{Engine, EventSink, LogLevel, NoopEvents, RunReport};
pub use error::{Error, Result};
pub use stats::{BlockSnapshot, JobSnapshot, JobStats, Totals};
