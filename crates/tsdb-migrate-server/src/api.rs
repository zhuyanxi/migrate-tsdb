//! HTTP API handlers.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use migrate_core::config::{
    EngineOptions, FilterConfig, JobConfig, MimirBackend, MimirConfig, SinkConfig,
};
use migrate_core::control::job_state_str;
use migrate_core::control::JobState;
use migrate_core::engine::EventSink;
use migrate_core::mimir::MimirSource;
use migrate_core::stats::block_status_str;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::state::{ensure_engine, new_job_id, remove_persisted, spawn_engine, JobHandle, JobMap};
use tsdb_reader::{discover_blocks, BlockReader, ReaderOptions};

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<Inner>,
}

pub struct Inner {
    pub jobs: JobMap,
    pub state_dir: PathBuf,
    pub allow_roots: Vec<PathBuf>,
    pub cwd: PathBuf,
    pub started_at: i64,
    pub token: Option<String>,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    msg: String,
}

impl ApiError {
    pub fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self {
            status,
            msg: msg.into(),
        }
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, msg)
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, msg)
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, msg)
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": self.msg, "status": self.status.as_u16()})),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Serialize)]
pub struct JobDetail {
    job: migrate_core::stats::JobSnapshot,
    config: JobConfig,
    interrupted: bool,
    engine_alive: bool,
    engine_ready: bool,
    logs_last_seq: u64,
}

fn detail(h: &JobHandle) -> JobDetail {
    JobDetail {
        job: h.snapshot(),
        config: h.config.clone(),
        interrupted: h.interrupted.load(Ordering::Relaxed),
        engine_alive: h.engine_alive.load(Ordering::Relaxed),
        engine_ready: h.engine().is_some(),
        logs_last_seq: h.events.last_seq(),
    }
}

fn get_handle(state: &AppState, id: &str) -> ApiResult<Arc<JobHandle>> {
    state
        .inner
        .jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(id)
        .cloned()
        .ok_or_else(|| ApiError::not_found(format!("job {id} not found")))
}

pub async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let jobs = state.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
    let mut running = 0;
    let mut paused = 0;
    let mut completed = 0;
    let mut failed = 0;
    for j in jobs.values() {
        match j.state() {
            JobState::Running | JobState::Pausing => running += 1,
            JobState::Paused | JobState::Stopped => paused += 1,
            JobState::Completed => completed += 1,
            JobState::Failed => failed += 1,
            _ => {}
        }
    }
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": (chrono::Utc::now().timestamp_millis() - state.inner.started_at).max(0) / 1000,
        "jobs": {"total": jobs.len(), "running": running, "paused": paused, "completed": completed, "failed": failed},
    }))
}

#[derive(Deserialize)]
pub struct BlocksQuery {
    pub data_dir: String,
    #[serde(default)]
    pub scan: bool,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn list_blocks(
    State(state): State<AppState>,
    Query(q): Query<BlocksQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let dir = resolve_existing_dir(&q.data_dir, &state.inner.allow_roots, &state.inner.cwd)?;
    let scan = q.scan;
    let limit = q.limit.unwrap_or(5000).min(20000);
    let blocks = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let infos = discover_blocks(&dir).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for b in infos.iter().take(limit) {
            let mut series = b.meta.stats.num_series;
            let mut chunks = b.meta.stats.num_chunks;
            let mut warnings = b.warnings.clone();
            if (scan || series == 0) && b.path.exists() {
                match BlockReader::open(
                    &b.path,
                    ReaderOptions {
                        verify_crc: false,
                        mmap_advise: false,
                    },
                )
                .and_then(|r| r.scan_stats())
                {
                    Ok(s) => {
                        series = s.series;
                        chunks = s.chunks;
                    }
                    Err(e) => warnings.push(format!("scan failed: {e}")),
                }
            }
            out.push(json!({
                "ulid": b.meta.ulid,
                "path": b.path.display().to_string(),
                "min_time": b.meta.min_time,
                "max_time": b.meta.max_time,
                "min_time_iso": migrate_core::stats::ms_to_rfc3339(b.meta.min_time),
                "max_time_iso": migrate_core::stats::ms_to_rfc3339(b.meta.max_time),
                "series": series,
                "chunks": chunks,
                "samples": b.meta.samples_hint(),
                "chunks_bytes": b.chunks_bytes,
                "index_bytes": b.index_bytes,
                "tombstones_bytes": b.tombstones_bytes,
                "warnings": warnings,
            }));
        }
        Ok(json!({"blocks": out, "total": infos.len(), "scanned": scan}))
    })
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .map_err(ApiError::bad_request)?;
    Ok(Json(blocks))
}

#[derive(Deserialize)]
pub struct CreateJobRequest {
    pub name: Option<String>,
    #[serde(default)]
    pub data_dir: Option<String>,
    pub output_dir: String,
    /// Mimir / object-store source; mutually exclusive with `data_dir`.
    #[serde(default)]
    pub mimir: Option<MimirConfig>,
    #[serde(default)]
    pub blocks: Vec<String>,
    #[serde(default)]
    pub filter: FilterConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    #[serde(default)]
    pub options: EngineOptions,
}

pub async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> ApiResult<Json<JobDetail>> {
    let output_dir =
        resolve_output_dir(&req.output_dir, &state.inner.allow_roots, &state.inner.cwd)?;
    let (data_dir, mimir) = match (&req.data_dir, &req.mimir) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "provide either data_dir or mimir, not both",
            ))
        }
        (None, None) => {
            return Err(ApiError::bad_request(
                "data_dir or mimir source is required",
            ))
        }
        (Some(dir), None) => (
            resolve_existing_dir(dir, &state.inner.allow_roots, &state.inner.cwd)?,
            None,
        ),
        (None, Some(m)) => {
            let mut m = m.clone();
            prepare_mimir_config(&mut m, &state.inner.allow_roots, &state.inner.cwd, false)?;
            (PathBuf::new(), Some(m))
        }
    };
    let cfg = JobConfig {
        name: req
            .name
            .unwrap_or_else(|| format!("migrate-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"))),
        data_dir,
        output_dir,
        mimir,
        blocks: req.blocks,
        filter: req.filter,
        sink: req.sink,
        options: req.options,
    };
    cfg.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    {
        let jobs = state.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = jobs
            .values()
            .find(|j| j.config.output_dir == cfg.output_dir)
        {
            return Err(ApiError::conflict(format!(
                "output_dir is already used by job {}; resume that job or choose another directory",
                existing.id
            )));
        }
    }
    let id = new_job_id();
    let handle = Arc::new(JobHandle::new(
        id.clone(),
        cfg,
        state.inner.state_dir.clone(),
    ));
    {
        let mut jobs = state.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.insert(id.clone(), handle.clone());
    }
    if let Err(e) = ensure_engine(&handle).await {
        let mut jobs = state.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.remove(&id);
        let _ = remove_persisted(&state.inner.state_dir, &id);
        return Err(ApiError::bad_request(format!("planning failed: {e}")));
    }
    handle.refresh();
    handle
        .persist()
        .map_err(|e| ApiError::internal(format!("persist job: {e}")))?;
    Ok(Json(detail(&handle)))
}

pub async fn list_jobs(
    State(state): State<AppState>,
) -> Json<Vec<migrate_core::stats::JobSnapshot>> {
    let handles: Vec<Arc<JobHandle>> = {
        let jobs = state.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.values().cloned().collect()
    };
    let mut out: Vec<_> = handles.iter().map(|h| h.snapshot()).collect();
    out.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    Json(out)
}

pub async fn get_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<JobDetail>> {
    let handle = get_handle(&state, &id)?;
    if handle.engine().is_none() {
        if let Err(e) = ensure_engine(&handle).await {
            handle.set_last_error(Some(e.clone()));
            handle.events.log(migrate_core::LogLevel::Error, &e);
        }
    }
    handle.refresh();
    Ok(Json(detail(&handle)))
}

pub async fn delete_job(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let handle = get_handle(&state, &id)?;
    if handle.engine_alive.load(Ordering::Relaxed) || handle.state().is_active() {
        return Err(ApiError::conflict(
            "job is active; stop it before deleting the job record",
        ));
    }
    {
        let mut jobs = state.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.remove(&id);
    }
    remove_persisted(&state.inner.state_dir, &id)
        .map_err(|e| ApiError::internal(format!("remove state: {e}")))?;
    Ok(Json(json!({"deleted": id, "output_kept": true})))
}

#[derive(Deserialize)]
pub struct ActionRequest {
    pub action: String,
}

pub async fn job_action(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<ActionRequest>,
) -> ApiResult<Json<JobDetail>> {
    let handle = get_handle(&state, &id)?;
    let action = req.action.to_ascii_lowercase();
    match action.as_str() {
        "start" => {
            ensure_engine(&handle)
                .await
                .map_err(ApiError::bad_request)?;
            if !matches!(handle.state(), JobState::Created | JobState::Stopped) {
                return Err(ApiError::conflict(format!(
                    "cannot start from state {}",
                    job_state_str(handle.state())
                )));
            }
            spawn_engine(handle.clone());
        }
        "pause" => {
            if !handle.control.request_pause() {
                return Err(ApiError::conflict(format!(
                    "cannot pause from state {}",
                    job_state_str(handle.state())
                )));
            }
        }
        "resume" => {
            ensure_engine(&handle)
                .await
                .map_err(ApiError::bad_request)?;
            let alive = handle.engine_alive.load(Ordering::Relaxed);
            if alive {
                if !handle.control.request_resume() {
                    return Err(ApiError::conflict(format!(
                        "cannot resume from state {}",
                        job_state_str(handle.state())
                    )));
                }
            } else {
                if matches!(handle.state(), JobState::Completed | JobState::Failed) {
                    return Err(ApiError::conflict(
                        "job is finished; create a new job or rerun with force",
                    ));
                }
                handle.interrupted.store(false, Ordering::Relaxed);
                spawn_engine(handle.clone());
            }
        }
        "stop" => {
            if !handle.control.request_stop() {
                return Err(ApiError::conflict(format!(
                    "cannot stop from state {}",
                    job_state_str(handle.state())
                )));
            }
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "unknown action {other:?}; expected start|pause|resume|stop"
            )))
        }
    }
    handle.touch();
    handle.refresh();
    if let Err(e) = handle.persist() {
        tracing::warn!("persist after action failed: {e}");
    }
    Ok(Json(detail(&handle)))
}

#[derive(Deserialize)]
pub struct JobBlocksQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub sort: Option<String>,
}

pub async fn job_blocks(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<JobBlocksQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let handle = get_handle(&state, &id)?;
    if handle.engine().is_none() {
        if let Err(e) = ensure_engine(&handle).await {
            handle.set_last_error(Some(e));
        }
    }
    handle.refresh();
    let mut blocks = handle.blocks();
    if let Some(status) = &q.status {
        let s = status.to_ascii_lowercase();
        blocks.retain(|b| block_status_str(b.status) == s);
    }
    match q.sort.as_deref().unwrap_or("min_time") {
        "progress" => blocks.sort_by(|a, b| {
            let ra = ratio(a.chunks_done, a.chunks_total);
            let rb = ratio(b.chunks_done, b.chunks_total);
            rb.partial_cmp(&ra).unwrap_or(std::cmp::Ordering::Equal)
        }),
        "status" => blocks.sort_by_key(|b| block_status_str(b.status).to_string()),
        _ => blocks.sort_by_key(|b| b.min_time),
    }
    let total = blocks.len();
    let offset = q.offset.unwrap_or(0).min(total);
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let page: Vec<_> = blocks.into_iter().skip(offset).take(limit).collect();
    Ok(Json(json!({
        "total": total,
        "offset": offset,
        "limit": limit,
        "blocks": page,
    })))
}

fn ratio(done: u64, total: u64) -> f32 {
    if total == 0 {
        0.0
    } else {
        done as f32 / total as f32
    }
}

#[derive(Deserialize)]
pub struct LogsQuery {
    #[serde(default)]
    pub after: Option<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn job_logs(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<LogsQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let handle = get_handle(&state, &id)?;
    let after = q.after.unwrap_or(0);
    let limit = q.limit.unwrap_or(300).clamp(1, 1000);
    let entries = handle.events.entries(after, limit);
    Ok(Json(json!({
        "last_seq": handle.events.last_seq(),
        "entries": entries,
    })))
}

#[derive(Deserialize)]
pub struct MimirTenantsRequest {
    #[serde(flatten)]
    pub mimir: MimirConfig,
}

pub async fn mimir_tenants(
    State(state): State<AppState>,
    Json(req): Json<MimirTenantsRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    let mut mimir = req.mimir;
    mimir.tenants.clear();
    prepare_mimir_config(&mut mimir, &state.inner.allow_roots, &state.inner.cwd, true)?;
    let cache_root = state.inner.state_dir.join("cache");
    let started = std::time::Instant::now();
    let tenants = tokio::task::spawn_blocking(
        move || -> std::result::Result<Vec<serde_json::Value>, String> {
            let source = MimirSource::new(&mimir, &cache_root).map_err(|e| e.to_string())?;
            let infos = source.list_tenant_infos().map_err(|e| e.to_string())?;
            Ok(infos
                .into_iter()
                .map(|t| {
                    json!({
                        "id": t.id,
                        "blocks": t.blocks,
                        "objects": t.objects,
                        "bytes": t.bytes,
                    })
                })
                .collect())
        },
    )
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .map_err(ApiError::bad_request)?;
    Ok(Json(json!({
        "tenants": tenants,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })))
}

/// Validates a Mimir config and canonicalises local paths so job state never
/// contains a relative/escaping path.
fn prepare_mimir_config(
    mimir: &mut MimirConfig,
    roots: &[PathBuf],
    cwd: &Path,
    discovery: bool,
) -> ApiResult<()> {
    mimir
        .validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if mimir.backend == MimirBackend::Local {
        let p = resolve_existing_dir(&mimir.bucket, roots, cwd)?;
        mimir.bucket = p.display().to_string();
    }
    if !mimir.cache_dir.trim().is_empty() {
        let p = resolve_output_dir(&mimir.cache_dir, roots, cwd)?;
        mimir.cache_dir = p.display().to_string();
    }
    let _ = discovery;
    Ok(())
}

// ---------------------------------------------------------------------------
// Path safety
// ---------------------------------------------------------------------------

pub fn resolve_existing_dir(raw: &str, roots: &[PathBuf], cwd: &Path) -> ApiResult<PathBuf> {
    let p = absolutize(raw, cwd);
    let canon = p
        .canonicalize()
        .map_err(|e| ApiError::bad_request(format!("invalid path {raw:?}: {e}")))?;
    if !canon.is_dir() {
        return Err(ApiError::bad_request(format!(
            "not a directory: {}",
            canon.display()
        )));
    }
    ensure_under_roots(&canon, roots, "data_dir")?;
    Ok(canon)
}

pub fn resolve_output_dir(raw: &str, roots: &[PathBuf], cwd: &Path) -> ApiResult<PathBuf> {
    let p = absolutize(raw, cwd);
    let mut ancestor = p.clone();
    while !ancestor.exists() {
        ancestor = ancestor
            .parent()
            .ok_or_else(|| ApiError::bad_request("output_dir has no existing ancestor"))?
            .to_path_buf();
    }
    let canon = ancestor
        .canonicalize()
        .map_err(|e| ApiError::bad_request(format!("invalid output_dir: {e}")))?;
    let rel = p.strip_prefix(&ancestor).unwrap_or_else(|_| Path::new(""));
    let mut full = canon.clone();
    for comp in rel.components() {
        match comp {
            std::path::Component::Normal(c) => full.push(c),
            std::path::Component::CurDir => {}
            _ => {
                return Err(ApiError::bad_request(
                    "output_dir must not contain '..' or root components",
                ))
            }
        }
    }
    ensure_under_roots(&full, roots, "output_dir")?;
    Ok(full)
}

fn absolutize(raw: &str, cwd: &Path) -> PathBuf {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        cwd.join(p)
    }
}

fn ensure_under_roots(path: &Path, roots: &[PathBuf], field: &str) -> ApiResult<()> {
    if roots.iter().any(|r| path.starts_with(r)) {
        Ok(())
    } else {
        Err(ApiError::forbidden(format!(
            "{field} {} is outside the allowed roots",
            path.display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "tsdb-api-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(p.join("sub")).unwrap();
        p.canonicalize().unwrap()
    }

    #[test]
    fn output_inside_root_is_allowed() {
        let root = temp_root("allow");
        let cwd = root.clone();
        let got = resolve_output_dir(
            root.join("sub/new-dir").to_str().unwrap(),
            &[root.clone()],
            &cwd,
        )
        .unwrap();
        assert!(got.starts_with(&root));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn output_with_parent_components_is_rejected() {
        let root = temp_root("reject");
        let cwd = root.clone();
        let raw = root.join("sub/../../etc/evil");
        let err = resolve_output_dir(raw.to_str().unwrap(), &[root.clone()], &cwd);
        assert!(err.is_err(), "path traversal must be rejected");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
