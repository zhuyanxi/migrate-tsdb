//! `tsdb-migrate` command line interface.

use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDate, TimeZone, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use migrate_core::config::{
    Compression, EngineOptions, FilterConfig, JobConfig, LabelMatcher, MimirBackend, MimirConfig,
    SinkConfig, SinkKind,
};
use migrate_core::control::{Control, JobState};
use migrate_core::engine::{Engine, EventSink, LogLevel};
use migrate_core::mimir::MimirSource;
use migrate_core::planner;
use migrate_core::stats::ms_to_rfc3339;
use serde_json::json;
use tsdb_reader::{discover_blocks, BlockReader, ReaderOptions};

#[derive(Parser, Debug)]
#[command(
    name = "tsdb-migrate",
    version,
    about = "Streaming exporter for Prometheus TSDB blocks (Parquet / NDJSON / DuckDB)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List blocks in a Prometheus data directory.
    Inspect(InspectArgs),
    /// Build a migration plan without writing data.
    Plan(PlanArgs),
    /// Run a migration.
    Migrate(MigrateArgs),
    /// Verify Parquet row counts against block completion markers.
    Verify(VerifyArgs),
    /// Generate and execute the DuckDB load script.
    DuckdbLoad(DuckdbLoadArgs),
}

#[derive(Args, Debug)]
struct InspectArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[command(flatten)]
    mimir: MimirArgs,
    /// Scan the index when meta.json has no stats (exact, slower).
    #[arg(long)]
    scan: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct PlanArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[command(flatten)]
    mimir: MimirArgs,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, value_delimiter = ',')]
    blocks: Vec<String>,
    #[arg(long)]
    min_time: Option<String>,
    #[arg(long)]
    max_time: Option<String>,
    #[arg(long = "match")]
    matchers: Vec<String>,
    #[arg(long, default_value_t = 50_000)]
    unit_series: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct MigrateArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[command(flatten)]
    mimir: MimirArgs,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, value_delimiter = ',')]
    blocks: Vec<String>,
    #[arg(long)]
    min_time: Option<String>,
    #[arg(long)]
    max_time: Option<String>,
    #[arg(long = "match", help = "exact label matcher, e.g. job=api")]
    matchers: Vec<String>,
    #[arg(long, value_enum, default_value_t = SinkArg::Parquet)]
    sink: SinkArg,
    #[arg(long, value_enum, default_value_t = CompressionArg::Zstd)]
    compression: CompressionArg,
    #[arg(long, default_value_t = 65_536)]
    row_group_size: usize,
    #[arg(long, default_value_t = 2)]
    block_parallel: usize,
    #[arg(long, default_value_t = 4)]
    decode_workers: usize,
    #[arg(long, default_value_t = 65_536)]
    batch_samples: usize,
    #[arg(long, default_value_t = 32)]
    batch_bytes_mb: usize,
    #[arg(long, default_value_t = 50_000)]
    unit_series: usize,
    #[arg(long, default_value_t = 2048)]
    channel_capacity: usize,
    #[arg(long)]
    no_verify_crc: bool,
    #[arg(long)]
    no_mmap_advise: bool,
    #[arg(long)]
    skip_bad_chunks: bool,
    #[arg(long)]
    no_resume: bool,
    #[arg(long)]
    force: bool,
    #[arg(long)]
    fail_fast: bool,
    #[arg(long)]
    duckdb_path: Option<String>,
    #[arg(long)]
    no_duckdb_exec: bool,
    #[arg(long)]
    json_progress: bool,
    /// Print progress every N seconds.
    #[arg(long, default_value_t = 2)]
    progress_interval: u64,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SinkArg {
    Parquet,
    Ndjson,
    Duckdb,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum CompressionArg {
    Zstd,
    Snappy,
    None,
}

#[derive(Args, Debug, Clone, Default)]
struct MimirArgs {
    /// S3 bucket name.
    #[arg(long)]
    mimir_bucket: Option<String>,
    /// Storage prefix inside the bucket (Mimir -blocks-storage.storage-prefix).
    #[arg(long, default_value = "")]
    mimir_prefix: String,
    #[arg(long, default_value = "us-east-1")]
    mimir_region: String,
    /// Custom S3 endpoint (MinIO / Ceph / LocalStack).
    #[arg(long)]
    mimir_endpoint: Option<String>,
    /// Tenant ID to migrate; repeatable or comma separated. Empty = all.
    #[arg(long = "mimir-tenant", value_delimiter = ',')]
    mimir_tenants: Vec<String>,
    /// Local cache directory (default: <out>/.cache).
    #[arg(long)]
    mimir_cache_dir: Option<PathBuf>,
    #[arg(long)]
    mimir_keep_cache: bool,
    /// Use a local directory that mirrors the bucket layout instead of S3.
    #[arg(long)]
    mimir_local_root: Option<PathBuf>,
    #[arg(long, default_value_t = 8)]
    mimir_downloads: usize,
    #[arg(long)]
    mimir_allow_http: bool,
    #[arg(long)]
    mimir_path_style: bool,
}

impl MimirArgs {
    fn to_config(&self) -> Result<Option<MimirConfig>, Box<dyn std::error::Error>> {
        let (backend, bucket) = match (&self.mimir_local_root, &self.mimir_bucket) {
            (Some(root), None) => (MimirBackend::Local, root.display().to_string()),
            (None, Some(bucket)) => (MimirBackend::S3, bucket.clone()),
            (Some(_), Some(_)) => {
                return Err("use either --mimir-local-root or --mimir-bucket, not both".into())
            }
            (None, None) => {
                if self.mimir_tenants.is_empty()
                    && self.mimir_prefix.is_empty()
                    && self.mimir_endpoint.is_none()
                {
                    return Ok(None);
                }
                return Err("--mimir-bucket or --mimir-local-root is required".into());
            }
        };
        let cfg = MimirConfig {
            backend,
            bucket,
            prefix: self.mimir_prefix.clone(),
            region: self.mimir_region.clone(),
            endpoint: self.mimir_endpoint.clone().unwrap_or_default(),
            tenants: self.mimir_tenants.clone(),
            cache_dir: self
                .mimir_cache_dir
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            keep_cache: self.mimir_keep_cache,
            max_concurrent_downloads: self.mimir_downloads,
            allow_http: self.mimir_allow_http,
            force_path_style: self.mimir_path_style,
        };
        cfg.validate()?;
        Ok(Some(cfg))
    }
}

#[derive(Args, Debug)]
struct VerifyArgs {
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct DuckdbLoadArgs {
    #[arg(long)]
    out: PathBuf,
    /// DB file for a Prometheus export, or DB directory for a multi-tenant Mimir export.
    #[arg(long)]
    db: Option<PathBuf>,
    /// Generate SQL only; do not invoke the duckdb CLI.
    #[arg(long)]
    no_exec: bool,
    /// Also print the generated SQL (can be large: one statement per metric).
    #[arg(long)]
    print_sql: bool,
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Inspect(a) => cmd_inspect(a),
        Command::Plan(a) => cmd_plan(a),
        Command::Migrate(a) => cmd_migrate(a),
        Command::Verify(a) => cmd_verify(a),
        Command::DuckdbLoad(a) => cmd_duckdb_load(a),
    };
    if let Err(e) = result {
        eprintln!("错误：{e}");
        std::process::exit(1);
    }
}

fn cmd_inspect(args: InspectArgs) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(mimir) = args.mimir.to_config()? {
        if args.data_dir.is_some() {
            return Err("use either --data-dir or Mimir flags, not both".into());
        }
        return inspect_mimir(&mimir, args.json);
    }
    let data_dir = args
        .data_dir
        .ok_or("--data-dir is required (or use --mimir-bucket / --mimir-local-root)")?;
    let blocks = discover_blocks(&data_dir)?;
    let opts = ReaderOptions {
        verify_crc: false,
        mmap_advise: false,
    };
    let mut rows = Vec::new();
    for b in &blocks {
        let mut series = b.meta.stats.num_series;
        let mut chunks = b.meta.stats.num_chunks;
        let samples = b.meta.samples_hint();
        let mut warnings = b.warnings.clone();
        if (args.scan || series == 0) && b.path.exists() {
            match BlockReader::open(&b.path, opts).and_then(|r| r.scan_stats()) {
                Ok(s) => {
                    series = s.series;
                    chunks = s.chunks;
                }
                Err(e) => warnings.push(format!("scan failed: {e}")),
            }
        }
        rows.push(json!({
            "ulid": b.meta.ulid,
            "path": b.path.display().to_string(),
            "min_time": b.meta.min_time,
            "max_time": b.meta.max_time,
            "min_time_iso": ms_to_rfc3339(b.meta.min_time),
            "max_time_iso": ms_to_rfc3339(b.meta.max_time),
            "series": series,
            "chunks": chunks,
            "samples": samples,
            "index_bytes": b.index_bytes,
            "chunks_bytes": b.chunks_bytes,
            "tombstones_bytes": b.tombstones_bytes,
            "warnings": warnings,
        }));
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    println!(
        "{:<28} {:<20} {:<20} {:>10} {:>10} {:>14} {:>10}",
        "BLOCK", "MIN TIME", "MAX TIME", "SERIES", "CHUNKS", "SAMPLES", "CHUNK GiB"
    );
    for r in &rows {
        println!(
            "{:<28} {:<20} {:<20} {:>10} {:>10} {:>14} {:>10.2}",
            r["ulid"].as_str().unwrap_or(""),
            short_time(r["min_time_iso"].as_str().unwrap_or("")),
            short_time(r["max_time_iso"].as_str().unwrap_or("")),
            r["series"].as_u64().unwrap_or(0),
            r["chunks"].as_u64().unwrap_or(0),
            r["samples"].as_u64().unwrap_or(0),
            r["chunks_bytes"].as_u64().unwrap_or(0) as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        for w in r["warnings"].as_array().into_iter().flatten() {
            println!("    ! {}", w.as_str().unwrap_or(""));
        }
    }
    println!("\n{} blocks", rows.len());
    Ok(())
}

fn inspect_mimir(mimir: &MimirConfig, as_json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cache = std::env::temp_dir().join("tsdb-migrate-inspect");
    let source = MimirSource::new(mimir, &cache)?;
    let infos = source.list_tenant_infos()?;
    if as_json {
        let rows: Vec<_> = infos
            .iter()
            .map(|t| {
                json!({
                    "tenant": t.id,
                    "blocks": t.blocks,
                    "objects": t.objects,
                    "bytes": t.bytes,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    println!(
        "Mimir bucket={} prefix={} backend={:?}",
        mimir.bucket,
        mimir.normalized_prefix(),
        mimir.backend
    );
    println!(
        "\n{:<32} {:>10} {:>12} {:>12}",
        "TENANT", "BLOCKS", "OBJECTS", "BYTES"
    );
    for t in &infos {
        println!(
            "{:<32} {:>10} {:>12} {:>12}",
            t.id, t.blocks, t.objects, t.bytes
        );
    }
    println!("\n{} tenant(s)", infos.len());
    Ok(())
}

fn cmd_plan(args: PlanArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mimir = args.mimir.to_config()?;
    let cfg = build_config(
        args.data_dir,
        args.out,
        args.blocks,
        args.min_time,
        args.max_time,
        args.matchers,
        mimir,
        EngineOptions {
            unit_series: args.unit_series,
            ..Default::default()
        },
        SinkConfig::default(),
    )?;
    let plan = match &cfg.mimir {
        Some(m) => MimirSource::new(m, &cfg.output_dir)?.plan(&cfg)?,
        None => planner::plan(&cfg)?,
    };
    let estimate = planner::estimated_output_bytes(&plan);
    if args.json {
        let blocks: Vec<_> = plan
            .blocks
            .iter()
            .map(|b| {
                json!({
                    "tenant": b.info.tenant_opt(),
                    "ulid": b.info.meta.ulid,
                    "min_time": b.info.meta.min_time,
                    "max_time": b.info.meta.max_time,
                    "series": b.stats.series,
                    "chunks": b.stats.chunks,
                    "samples_expected": b.samples_expected,
                    "chunks_bytes": b.info.chunks_bytes,
                    "units": (b.stats.series + cfg.options.unit_series as u64 - 1) / cfg.options.unit_series as u64,
                    "warnings": b.info.warnings,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "blocks": blocks,
                "series_total": plan.series_total(),
                "chunks_total": plan.chunks_total(),
                "samples_expected": plan.samples_expected(),
                "estimated_output_bytes": estimate,
                "warnings": plan.warnings,
            }))?
        );
        return Ok(());
    }
    println!("blocks        : {}", plan.blocks.len());
    println!("series        : {}", plan.series_total());
    println!("chunks        : {}", plan.chunks_total());
    println!("samples (meta): {}", plan.samples_expected());
    println!(
        "output estimate: {:.2} GiB",
        estimate as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "\n{:<24} {:<28} {:>12} {:>12} {:>14} {:>10}",
        "TENANT", "BLOCK", "SERIES", "CHUNKS", "SAMPLES", "UNITS"
    );
    for b in &plan.blocks {
        let units = if b.stats.series == 0 {
            0
        } else {
            (b.stats.series + cfg.options.unit_series as u64 - 1) / cfg.options.unit_series as u64
        };
        println!(
            "{:<24} {:<28} {:>12} {:>12} {:>14} {:>10}",
            if b.info.tenant.is_empty() {
                "-"
            } else {
                &b.info.tenant
            },
            b.info.meta.ulid,
            b.stats.series,
            b.stats.chunks,
            b.samples_expected,
            units
        );
    }
    for w in &plan.warnings {
        println!("! {w}");
    }
    Ok(())
}

fn cmd_migrate(args: MigrateArgs) -> Result<(), Box<dyn std::error::Error>> {
    let sink = match args.sink {
        SinkArg::Parquet => SinkKind::Parquet,
        SinkArg::Ndjson => SinkKind::Ndjson,
        SinkArg::Duckdb => SinkKind::Duckdb,
    };
    let compression = match args.compression {
        CompressionArg::Zstd => Compression::Zstd,
        CompressionArg::Snappy => Compression::Snappy,
        CompressionArg::None => Compression::None,
    };
    let opts = EngineOptions {
        block_parallel: args.block_parallel,
        decode_workers: args.decode_workers,
        batch_samples: args.batch_samples,
        batch_bytes: args.batch_bytes_mb * 1024 * 1024,
        unit_series: args.unit_series,
        channel_capacity: args.channel_capacity,
        verify_crc: !args.no_verify_crc,
        mmap_advise: !args.no_mmap_advise,
        skip_bad_chunks: args.skip_bad_chunks,
        resume: !args.no_resume,
        force: args.force,
        continue_on_error: !args.fail_fast,
    };
    let sink_cfg = SinkConfig {
        kind: sink,
        compression,
        row_group_size: args.row_group_size,
        duckdb_path: args.duckdb_path.clone().unwrap_or_default(),
        duckdb_exec: !args.no_duckdb_exec,
    };
    let mimir = args.mimir.to_config()?;
    let cfg = build_config(
        args.data_dir,
        args.out,
        args.blocks,
        args.min_time,
        args.max_time,
        args.matchers,
        mimir,
        opts,
        sink_cfg,
    )?;

    let events = Arc::new(ConsoleEvents {
        json_progress: args.json_progress,
    });
    let control = Arc::new(Control::new());
    let engine = Engine::new(cfg, control.clone(), events)?;
    let stats = engine.stats.clone();

    let done = Arc::new(AtomicBool::new(false));
    let interval = args.progress_interval.max(1);
    let progress = {
        let done = done.clone();
        let stats = stats.clone();
        let control = control.clone();
        let json = args.json_progress;
        std::thread::spawn(move || {
            let tty = std::io::stdout().is_terminal();
            let mut wrote = false;
            while !done.load(Ordering::Relaxed) {
                for _ in 0..(interval * 10) {
                    if done.load(Ordering::Relaxed) || control.is_stop() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if done.load(Ordering::Relaxed) {
                    break;
                }
                let snap = stats.snapshot(control.state(), None);
                if json {
                    let line = json!({
                        "state": snap.state,
                        "elapsed_ms": snap.elapsed_ms,
                        "series_done": snap.totals.series_done,
                        "series_total": snap.totals.series_total,
                        "chunks_done": snap.totals.chunks_done,
                        "chunks_total": snap.totals.chunks_total,
                        "samples": snap.totals.samples,
                        "samples_skipped_tombstone": snap.totals.samples_skipped_tombstone,
                        "samples_skipped_range": snap.totals.samples_skipped_range,
                        "bytes_read": snap.totals.bytes_read,
                        "blocks_completed": snap.blocks_completed,
                        "blocks_total": snap.blocks_total,
                        "samples_per_sec": snap.rate.samples_per_sec,
                        "eta_seconds": snap.eta_seconds,
                    });
                    println!("{line}");
                } else {
                    let line = format_progress(&snap);
                    if tty {
                        print!("\r{line}");
                        let _ = std::io::stdout().flush();
                        wrote = true;
                    } else {
                        println!("{line}");
                    }
                }
            }
            if tty && wrote {
                println!();
            }
        })
    };

    let report = engine.run();
    done.store(true, Ordering::Relaxed);
    let _ = progress.join();

    match report {
        Ok(r) => {
            if args.json_progress {
                println!(
                    "{}",
                    json!({
                        "event": "finished",
                        "state": r.state,
                        "failed_blocks": r.failed_blocks,
                        "skipped_blocks": r.skipped_blocks,
                        "samples": r.totals.samples,
                        "floats": r.totals.floats,
                        "histograms": r.totals.histograms,
                        "samples_skipped_tombstone": r.totals.samples_skipped_tombstone,
                        "samples_skipped_range": r.totals.samples_skipped_range,
                        "bytes_read": r.totals.bytes_read,
                        "bytes_written": r.totals.bytes_written,
                    })
                );
            } else {
                println!(
                    "完成：state={:?}，blocks failed={} skipped={}，samples={}，tombstone 过滤={}，范围过滤={}",
                    r.state,
                    r.failed_blocks,
                    r.skipped_blocks,
                    r.totals.samples,
                    r.totals.samples_skipped_tombstone,
                    r.totals.samples_skipped_range
                );
            }
            if r.state != JobState::Completed {
                std::process::exit(2);
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn format_progress(s: &migrate_core::stats::JobSnapshot) -> String {
    let t = &s.totals;
    let pct = if t.chunks_total > 0 {
        t.chunks_done as f64 / t.chunks_total as f64 * 100.0
    } else if t.series_total > 0 {
        t.series_done as f64 / t.series_total as f64 * 100.0
    } else {
        0.0
    };
    let eta = s
        .eta_seconds
        .map(format_duration)
        .unwrap_or_else(|| "--".to_string());
    format!(
        "[{:>8}] {:?} {:>5.1}% | series {}/{} | chunks {}/{} | samples {} | {:.2}M/s | {:.1} MiB/s | blocks {}/{} | ETA {}",
        format_duration(s.elapsed_ms / 1000),
        s.state,
        pct,
        t.series_done,
        t.series_total,
        t.chunks_done,
        t.chunks_total,
        t.samples,
        s.rate.samples_per_sec / 1e6,
        s.rate.bytes_per_sec / (1024.0 * 1024.0),
        s.blocks_completed + s.blocks_skipped,
        s.blocks_total,
        eta
    )
}

fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

fn short_time(iso: &str) -> String {
    iso.replace('T', " ")
        .trim_end_matches('Z')
        .chars()
        .take(19)
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn build_config(
    data_dir: Option<PathBuf>,
    out: PathBuf,
    blocks: Vec<String>,
    min_time: Option<String>,
    max_time: Option<String>,
    matchers: Vec<String>,
    mimir: Option<MimirConfig>,
    options: EngineOptions,
    sink: SinkConfig,
) -> Result<JobConfig, Box<dyn std::error::Error>> {
    let mut filter = FilterConfig {
        min_time: min_time.map(|s| parse_time(&s)).transpose()?,
        max_time: max_time.map(|s| parse_time(&s)).transpose()?,
        matchers: Vec::new(),
    };
    for m in matchers {
        let (name, value) = m
            .split_once('=')
            .ok_or_else(|| format!("matcher must be name=value, got {m:?}"))?;
        if name.is_empty() {
            return Err("matcher name is empty".into());
        }
        filter.matchers.push(LabelMatcher {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    if mimir.is_some() && data_dir.is_some() {
        return Err("use either --data-dir or Mimir flags, not both".into());
    }
    let data_dir = data_dir.unwrap_or_default();
    if mimir.is_none() && data_dir.as_os_str().is_empty() {
        return Err("--data-dir is required (or use --mimir-bucket / --mimir-local-root)".into());
    }
    let cfg = JobConfig {
        name: format!("migrate-{}", Utc::now().format("%Y%m%d-%H%M%S")),
        data_dir,
        output_dir: out,
        mimir,
        blocks,
        filter,
        sink,
        options,
    };
    cfg.validate()?;
    Ok(cfg)
}

fn parse_time(s: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if let Ok(ms) = s.parse::<i64>() {
        return Ok(ms);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(Utc
            .from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap())
            .timestamp_millis());
    }
    Err(format!("invalid time {s:?}: use RFC3339, YYYY-MM-DD, or Unix milliseconds").into())
}

struct ConsoleEvents {
    json_progress: bool,
}

impl EventSink for ConsoleEvents {
    fn log(&self, level: LogLevel, msg: &str) {
        if level == LogLevel::Debug && std::env::var_os("TSDB_VERBOSE").is_none() {
            return;
        }
        let ts = Utc::now().format("%H:%M:%S");
        if self.json_progress {
            eprintln!("[{ts}] {level:?} {msg}");
        } else {
            eprintln!("[{ts}] {level:?}  {msg}");
        }
    }
}

fn cmd_verify(args: VerifyArgs) -> Result<(), Box<dyn std::error::Error>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let manifest = migrate_core::manifest::read(&args.out)?;
    let mut problems: Vec<String> = Vec::new();
    let mut report = Vec::new();
    for block in &manifest.blocks {
        let dir =
            migrate_core::sink::output_dir_for(&args.out, block.tenant.as_deref(), &block.ulid);
        let marker_path = dir.join("_complete.json");
        let marker: Option<serde_json::Value> = std::fs::read(&marker_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok());
        let mut tables = serde_json::Map::new();
        for (name, expected) in [("samples", block.floats), ("histograms", block.histograms)] {
            let mut total = 0u64;
            let mut files = 0u64;
            if let Ok(rd) = std::fs::read_dir(dir.join(name)) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.extension().map(|x| x == "parquet").unwrap_or(false) {
                        files += 1;
                        let r = SerializedFileReader::new(std::fs::File::open(&p)?)?;
                        total += r.metadata().file_metadata().num_rows() as u64;
                    }
                }
            }
            if files > 0 && total != expected {
                problems.push(format!(
                    "{} {}: rows {} != expected {}",
                    block.ulid, name, total, expected
                ));
            }
            if files == 0 && expected > 0 {
                problems.push(format!(
                    "{} {}: no parquet files but expected {} rows",
                    block.ulid, name, expected
                ));
            }
            tables.insert(name.to_string(), json!({"files": files, "rows": total}));
        }
        let mut series_files = 0u64;
        let mut series_rows = 0u64;
        if let Ok(rd) = std::fs::read_dir(dir.join("series")) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().map(|x| x == "parquet").unwrap_or(false) {
                    series_files += 1;
                    let r = SerializedFileReader::new(std::fs::File::open(&p)?)?;
                    series_rows += r.metadata().file_metadata().num_rows() as u64;
                }
            }
        }
        if block.status == migrate_core::stats::BlockStatus::Completed && marker.is_none() {
            problems.push(format!(
                "{}: completed but _complete.json missing",
                block.ulid
            ));
        }
        report.push(json!({
            "ulid": block.ulid,
            "tenant": block.tenant,
            "status": block.status,
            "series": {"files": series_files, "rows": series_rows},
            "tables": tables,
            "marker": marker.is_some(),
            "error": block.error,
        }));
    }
    let ok = problems.is_empty();
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "ok": ok,
                "problems": problems,
                "blocks": report,
                "totals": manifest.totals,
            }))?
        );
    } else {
        for b in &report {
            println!(
                "{:<20} {} {:<10} series_rows={} samples_rows={} hists_rows={} marker={}",
                b["tenant"].as_str().unwrap_or("-"),
                b["ulid"].as_str().unwrap_or(""),
                b["status"].as_str().unwrap_or("?"),
                b["series"]["rows"],
                b["tables"]["samples"]["rows"],
                b["tables"]["histograms"]["rows"],
                b["marker"]
            );
        }
        println!(
            "\nsamples(total)={} histograms={} bytes_written={:.2} MiB",
            manifest.totals.samples,
            manifest.totals.histograms,
            manifest.totals.bytes_written as f64 / (1024.0 * 1024.0)
        );
        if ok {
            println!("verify OK");
        } else {
            println!("\nverify FAILED:");
            for p in &problems {
                println!("  - {p}");
            }
        }
    }
    if !ok {
        std::process::exit(3);
    }
    Ok(())
}

fn cmd_duckdb_load(args: DuckdbLoadArgs) -> Result<(), Box<dyn std::error::Error>> {
    let configured = args
        .db
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let plans = migrate_core::duckdb::plan(&args.out, &configured)?;
    for p in &plans {
        let scope = match &p.tenant {
            Some(t) => format!("tenant={t}"),
            None => "prometheus".to_string(),
        };
        println!(
            "[{scope}] metrics={} sql={} db={}",
            p.metrics,
            p.sql_path.display(),
            p.db_path.display()
        );
        for w in &p.warnings {
            println!("  ! {w}");
        }
        if args.print_sql {
            println!("{}", std::fs::read_to_string(&p.sql_path)?);
        }
        if !args.no_exec {
            migrate_core::duckdb::execute(p)?;
        }
    }
    Ok(())
}
