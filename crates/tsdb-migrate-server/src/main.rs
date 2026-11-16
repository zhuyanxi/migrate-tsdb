//! `tsdb-migrate-server`: HTTP API + embedded web console.

mod api;
mod state;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use api::AppState;
use state::{load_persisted, shutdown_all, JobHandle, JobMap};

const INDEX_HTML: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/index.html"));
const APP_JS: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/app.js"));
const STYLES_CSS: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/styles.css"));

#[derive(Parser, Debug)]
#[command(
    name = "tsdb-migrate-server",
    version,
    about = "HTTP control plane for tsdb-migrate"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,
    /// Directory that jobs are allowed to read/write. Repeatable. Defaults to cwd.
    #[arg(long = "allow-root")]
    allow_roots: Vec<PathBuf>,
    /// Bearer token. Falls back to TSDB_MIGRATE_TOKEN.
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    cors_any: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,tower_http=warn,axum=warn")),
        )
        .init();

    let args = Args::parse();
    let cwd = std::env::current_dir().expect("cwd");
    let cwd = cwd.canonicalize().unwrap_or(cwd);

    let mut roots: Vec<PathBuf> = if args.allow_roots.is_empty() {
        vec![cwd.clone()]
    } else {
        args.allow_roots
            .iter()
            .map(|p| {
                let p = if p.is_absolute() {
                    p.clone()
                } else {
                    cwd.join(p)
                };
                p.canonicalize()
                    .unwrap_or_else(|e| panic!("allow-root {} is not accessible: {e}", p.display()))
            })
            .collect()
    };
    roots.sort();
    roots.dedup();

    let state_dir = if args.state_dir.is_absolute() {
        args.state_dir
    } else {
        cwd.join(args.state_dir)
    };
    std::fs::create_dir_all(state_dir.join("jobs")).expect("create state dir");

    let jobs: JobMap = std::sync::Mutex::new(std::collections::HashMap::new());
    for p in load_persisted(&state_dir) {
        let interrupted = matches!(
            p.state,
            migrate_core::control::JobState::Running
                | migrate_core::control::JobState::Pausing
                | migrate_core::control::JobState::Stopping
        );
        let restored_state = if interrupted {
            migrate_core::control::JobState::Paused
        } else {
            p.state
        };
        let h = JobHandle::restore(
            p.id.clone(),
            p.config,
            state_dir.clone(),
            p.created_at,
            restored_state,
            interrupted,
            p.snapshot,
            p.blocks,
        );
        jobs.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(p.id, Arc::new(h));
    }
    let job_count = jobs.lock().unwrap_or_else(|e| e.into_inner()).len();
    tracing::info!(
        "loaded {job_count} job(s), state dir {}, {} allowed root(s)",
        state_dir.display(),
        roots.len()
    );

    let token = args
        .token
        .or_else(|| std::env::var("TSDB_MIGRATE_TOKEN").ok())
        .filter(|t| !t.is_empty());

    let app_state = AppState {
        inner: Arc::new(api::Inner {
            jobs,
            state_dir: state_dir.clone(),
            allow_roots: roots,
            cwd,
            started_at: chrono::Utc::now().timestamp_millis(),
            token: token.clone(),
        }),
    };

    spawn_persistence_ticker(app_state.clone());

    let mut app = Router::new()
        .route("/api/health", get(api::health))
        .route("/api/blocks", get(api::list_blocks))
        .route("/api/mimir/tenants", post(api::mimir_tenants))
        .route("/api/jobs", get(api::list_jobs).post(api::create_job))
        .route("/api/jobs/{id}", get(api::get_job).delete(api::delete_job))
        .route("/api/jobs/{id}/actions", post(api::job_action))
        .route("/api/jobs/{id}/blocks", get(api::job_blocks))
        .route("/api/jobs/{id}/logs", get(api::job_logs))
        .fallback(static_handler)
        .with_state(app_state.clone())
        .layer(middleware::from_fn_with_state(app_state.clone(), auth))
        .layer(TraceLayer::new_for_http());

    if args.cors_any {
        app = app.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    }

    let addr: SocketAddr = args
        .bind
        .parse()
        .unwrap_or_else(|e| panic!("invalid --bind {}: {e}", args.bind));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr}: {e}"));
    tracing::info!(
        "listening on http://{addr}  (token: {})",
        if token.is_some() { "on" } else { "off" }
    );

    let shutdown_state = app_state.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down: stopping active jobs");
            let inner = shutdown_state.inner.clone();
            let _ = tokio::task::spawn_blocking(move || {
                shutdown_all(&inner.jobs, Duration::from_secs(20));
            })
            .await;
        })
        .await
        .expect("server error");
}

fn spawn_persistence_ticker(app: AppState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        let mut tick: u64 = 0;
        loop {
            ticker.tick().await;
            tick += 1;
            let handles: Vec<Arc<JobHandle>> = {
                let jobs = app.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
                jobs.values().cloned().collect()
            };
            for h in handles {
                h.refresh();
                let changed = h.state_changed_since_persist();
                if changed || tick % 5 == 0 {
                    let h2 = h.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        if let Err(e) = h2.persist() {
                            tracing::warn!("persist job {} failed: {e}", h2.id);
                        }
                    })
                    .await;
                }
            }
        }
    });
}

async fn auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let path = req.uri().path();
    let needs_auth = path.starts_with("/api/") && path != "/api/health";
    if needs_auth {
        if let Some(token) = state.inner.token.as_deref() {
            let ok = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v: &HeaderValue| v.to_str().ok())
                .map(|v| {
                    v.strip_prefix("Bearer ")
                        .map(|t| t == token)
                        .unwrap_or(false)
                })
                .unwrap_or(false);
            if !ok {
                return (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({"error": "missing or invalid bearer token"})),
                )
                    .into_response();
            }
        }
    }
    next.run(req).await
}

async fn static_handler(uri: axum::http::Uri) -> Response {
    let path = uri.path();
    let (body, mime) = match path {
        "/" | "/index.html" => (INDEX_HTML, "text/html; charset=utf-8"),
        "/app.js" => (APP_JS, "application/javascript; charset=utf-8"),
        "/styles.css" => (STYLES_CSS, "text/css; charset=utf-8"),
        _ => {
            return (StatusCode::NOT_FOUND, "not found").into_response();
        }
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}
