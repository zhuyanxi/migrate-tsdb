//! Job handles, in-memory state, logs and persistence.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::Utc;
use migrate_core::config::{JobConfig, SinkKind};
use migrate_core::control::{Control, JobState};
use migrate_core::engine::{Engine, EventSink, LogLevel};
use migrate_core::error::{Error, Result};
use migrate_core::stats::{ms_to_rfc3339, BlockSnapshot, JobSnapshot, Totals};
use serde::{Deserialize, Serialize};

pub const LOG_CAPACITY: usize = 2000;

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub seq: u64,
    pub ts: String,
    pub level: String,
    pub msg: String,
}

pub struct JobEvents {
    seq: AtomicU64,
    logs: Mutex<VecDeque<LogEntry>>,
}

impl Default for JobEvents {
    fn default() -> Self {
        Self::new()
    }
}

impl JobEvents {
    pub fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            logs: Mutex::new(VecDeque::with_capacity(LOG_CAPACITY)),
        }
    }

    pub fn entries(&self, after: u64, limit: usize) -> Vec<LogEntry> {
        let logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        logs.iter()
            .filter(|e| e.seq > after)
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn last_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

impl EventSink for JobEvents {
    fn log(&self, level: LogLevel, msg: &str) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let entry = LogEntry {
            seq,
            ts: ms_to_rfc3339(Utc::now().timestamp_millis()),
            level: format!("{level:?}").to_lowercase(),
            msg: msg.to_string(),
        };
        let mut logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        if logs.len() >= LOG_CAPACITY {
            logs.pop_front();
        }
        logs.push_back(entry);
    }
}

pub struct JobHandle {
    pub id: String,
    pub config: JobConfig,
    pub control: Arc<Control>,
    pub events: Arc<JobEvents>,
    pub created_at: i64,
    pub state_dir: PathBuf,
    pub interrupted: AtomicBool,
    pub engine_alive: AtomicBool,
    engine: RwLock<Option<Arc<Engine>>>,
    planning: tokio::sync::Mutex<()>,
    last_snapshot: RwLock<Option<JobSnapshot>>,
    last_blocks: RwLock<Vec<BlockSnapshot>>,
    last_error: Mutex<Option<String>>,
    updated_at: Mutex<i64>,
    persisted_state: Mutex<JobState>,
}

impl JobHandle {
    pub fn new(id: String, config: JobConfig, state_dir: PathBuf) -> Self {
        Self {
            id,
            config,
            control: Arc::new(Control::new()),
            events: Arc::new(JobEvents::new()),
            created_at: Utc::now().timestamp_millis(),
            state_dir,
            interrupted: AtomicBool::new(false),
            engine_alive: AtomicBool::new(false),
            engine: RwLock::new(None),
            planning: tokio::sync::Mutex::new(()),
            last_snapshot: RwLock::new(None),
            last_blocks: RwLock::new(Vec::new()),
            last_error: Mutex::new(None),
            updated_at: Mutex::new(Utc::now().timestamp_millis()),
            persisted_state: Mutex::new(JobState::Created),
        }
    }

    pub fn restore(
        id: String,
        config: JobConfig,
        state_dir: PathBuf,
        created_at: i64,
        state: JobState,
        interrupted: bool,
        snapshot: Option<JobSnapshot>,
        blocks: Vec<BlockSnapshot>,
    ) -> Self {
        let control = Arc::new(Control::new());
        control.restore_state(state);
        let h = Self {
            id,
            config,
            control,
            events: Arc::new(JobEvents::new()),
            created_at,
            state_dir,
            interrupted: AtomicBool::new(interrupted),
            engine_alive: AtomicBool::new(false),
            engine: RwLock::new(None),
            planning: tokio::sync::Mutex::new(()),
            last_snapshot: RwLock::new(snapshot),
            last_blocks: RwLock::new(blocks),
            last_error: Mutex::new(None),
            updated_at: Mutex::new(Utc::now().timestamp_millis()),
            persisted_state: Mutex::new(state),
        };
        h
    }

    pub fn engine(&self) -> Option<Arc<Engine>> {
        self.engine
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_engine(&self, engine: Arc<Engine>) {
        *self.engine.write().unwrap_or_else(|e| e.into_inner()) = Some(engine);
    }

    pub fn state(&self) -> JobState {
        self.control.state()
    }

    pub fn error(&self) -> Option<String> {
        self.control.error().or_else(|| {
            self.last_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        })
    }

    pub fn touch(&self) {
        *self.updated_at.lock().unwrap_or_else(|e| e.into_inner()) = Utc::now().timestamp_millis();
    }

    pub fn updated_at(&self) -> i64 {
        *self.updated_at.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn refresh(&self) {
        let Some(engine) = self.engine() else { return };
        engine.stats.refresh_rate();
        let snapshot = engine
            .stats
            .snapshot(self.control.state(), self.control.error());
        *self
            .last_snapshot
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(snapshot);
        let blocks: Vec<BlockSnapshot> = engine.stats.blocks.iter().map(|b| b.snapshot()).collect();
        *self.last_blocks.write().unwrap_or_else(|e| e.into_inner()) = blocks;
    }

    pub fn snapshot(&self) -> JobSnapshot {
        if let Some(engine) = self.engine() {
            let snapshot = engine
                .stats
                .snapshot(self.control.state(), self.control.error());
            *self
                .last_snapshot
                .write()
                .unwrap_or_else(|e| e.into_inner()) = Some(snapshot.clone());
            return snapshot;
        }
        let mut snap = self
            .last_snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_else(|| minimal_snapshot(&self.config, &self.id, self.created_at));
        let state = self.control.state();
        snap.state = state;
        snap.error = self.error();
        if !matches!(
            state,
            JobState::Completed | JobState::Failed | JobState::Stopped
        ) {
            snap.elapsed_ms = (Utc::now().timestamp_millis() - self.created_at).max(0) as u64;
        }
        snap
    }

    pub fn blocks(&self) -> Vec<BlockSnapshot> {
        if let Some(engine) = self.engine() {
            return engine.stats.blocks.iter().map(|b| b.snapshot()).collect();
        }
        self.last_blocks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_last_error(&self, err: Option<String>) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = err;
    }

    pub fn persist(&self) -> Result<()> {
        let state = self.control.state();
        let persisted = PersistedJob {
            id: self.id.clone(),
            config: self.config.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at(),
            state,
            interrupted: self.interrupted.load(Ordering::Relaxed),
            snapshot: Some(self.snapshot()),
            blocks: self.blocks(),
        };
        let dir = self.state_dir.join("jobs");
        std::fs::create_dir_all(&dir)?;
        let data = serde_json::to_vec_pretty(&persisted)
            .map_err(|e| Error::state(format!("serialize job: {e}")))?;
        let path = dir.join(format!("{}.json", self.id));
        let tmp = dir.join(format!("{}.json.tmp", self.id));
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&data)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        *self
            .persisted_state
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = state;
        Ok(())
    }

    pub fn state_changed_since_persist(&self) -> bool {
        *self
            .persisted_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            != self.control.state()
    }
}

fn minimal_snapshot(config: &JobConfig, id: &str, created_at: i64) -> JobSnapshot {
    JobSnapshot {
        job_id: id.to_string(),
        name: config.name.clone(),
        state: JobState::Created,
        error: None,
        data_dir: config.data_dir.display().to_string(),
        output_dir: config.output_dir.display().to_string(),
        started_at: ms_to_rfc3339(created_at),
        elapsed_ms: 0,
        totals: Totals::default(),
        blocks_total: 0,
        blocks_completed: 0,
        blocks_failed: 0,
        blocks_skipped: 0,
        blocks_running: 0,
        blocks_pending: 0,
        rate: Default::default(),
        eta_seconds: None,
        options: config.options.clone(),
        sink_kind: match config.sink.kind {
            SinkKind::Parquet => "parquet".into(),
            SinkKind::Ndjson => "ndjson".into(),
            SinkKind::Duckdb => "duckdb".into(),
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedJob {
    pub id: String,
    pub config: JobConfig,
    pub created_at: i64,
    pub updated_at: i64,
    pub state: JobState,
    pub interrupted: bool,
    pub snapshot: Option<JobSnapshot>,
    #[serde(default)]
    pub blocks: Vec<BlockSnapshot>,
}

pub fn load_persisted(state_dir: &Path) -> Vec<PersistedJob> {
    let dir = state_dir.join("jobs");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<PersistedJob> = Vec::new();
    for e in rd.flatten() {
        let p = e.path();
        if p.extension().map(|x| x == "json").unwrap_or(false) {
            match std::fs::read(&p)
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
            {
                Some(j) => out.push(j),
                None => tracing::warn!("ignoring unreadable job state {}", p.display()),
            }
        }
    }
    out.sort_by_key(|j| j.created_at);
    out
}

pub fn remove_persisted(state_dir: &Path, id: &str) -> Result<()> {
    let path = state_dir.join("jobs").join(format!("{id}.json"));
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Builds the engine (planning + stats hydration) on first use.
pub async fn ensure_engine(handle: &Arc<JobHandle>) -> std::result::Result<(), String> {
    if handle.engine().is_some() {
        return Ok(());
    }
    let _guard = handle.planning.lock().await;
    if handle.engine().is_some() {
        return Ok(());
    }
    let cfg = handle.config.clone();
    let control = handle.control.clone();
    let events: Arc<dyn EventSink> = handle.events.clone();
    let id = handle.id.clone();
    let created_at = handle.created_at;
    let engine = tokio::task::spawn_blocking(move || {
        Engine::new_with_id(cfg, control, events, id, Some(created_at))
    })
    .await
    .map_err(|e| format!("planning task failed: {e}"))?
    .map_err(|e| e.to_string())?;
    if let Err(e) = engine.hydrate_progress() {
        tracing::warn!("hydrate progress failed: {e}");
    }
    // For jobs restored in a terminal state, keep the elapsed time frozen.
    if matches!(
        handle.control.state(),
        JobState::Completed | JobState::Failed | JobState::Stopped
    ) {
        let mut latest_finished: Option<i64> = None;
        for b in &engine.stats.blocks {
            if let Some(iso) = b.snapshot().finished_at {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&iso) {
                    latest_finished = Some(
                        latest_finished
                            .unwrap_or(i64::MIN)
                            .max(dt.timestamp_millis()),
                    );
                }
            }
        }
        if let Some(ms) = latest_finished {
            engine.stats.set_finished_ms(ms);
        } else if let Some(snap) = handle
            .last_snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            engine
                .stats
                .set_finished_ms(created_at + snap.elapsed_ms as i64);
        }
    }
    handle.set_engine(Arc::new(engine));
    Ok(())
}

pub fn spawn_engine(handle: Arc<JobHandle>) {
    if handle.engine_alive.swap(true, Ordering::SeqCst) {
        return;
    }
    std::thread::spawn(move || {
        let engine = handle.engine();
        let result = match engine {
            Some(e) => e.run(),
            None => Err(Error::state("engine is not ready")),
        };
        match result {
            Ok(_) => {}
            Err(e) => {
                handle.set_last_error(Some(e.to_string()));
                handle.control.fail(e.to_string());
                handle.events.log(LogLevel::Error, &e.to_string());
            }
        }
        handle.engine_alive.store(false, Ordering::SeqCst);
        handle.refresh();
        if let Err(e) = handle.persist() {
            tracing::warn!("persist job {} failed: {e}", handle.id);
        }
    });
}

pub type JobMap = Mutex<HashMap<String, Arc<JobHandle>>>;

pub fn new_job_id() -> String {
    let s = uuid::Uuid::new_v4().simple().to_string();
    s[..12].to_string()
}

/// Stops all active jobs and waits briefly for their engine threads.
pub fn shutdown_all(jobs: &JobMap, timeout: Duration) {
    let handles: Vec<Arc<JobHandle>> = {
        let map = jobs.lock().unwrap_or_else(|e| e.into_inner());
        map.values().cloned().collect()
    };
    for h in &handles {
        if h.state().is_active() {
            h.control.request_stop();
        }
    }
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let alive = handles
            .iter()
            .any(|h| h.engine_alive.load(Ordering::SeqCst));
        if !alive {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    for h in &handles {
        let _ = h.persist();
    }
}
