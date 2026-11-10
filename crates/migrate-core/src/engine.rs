//! The migration engine: block-level parallelism, bounded intrab-block
//! pipeline, cooperative pause/stop, unit checkpoints and sinks.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use serde::{Deserialize, Serialize};
use tsdb_reader::chunkenc::{self, HistogramSample, SampleSink};
use tsdb_reader::chunks::ChunkReader;
use tsdb_reader::tombstones::is_deleted;
use tsdb_reader::Tombstones;
use tsdb_reader::{BlockReader, ReaderOptions, SeriesRef};

use crate::batch::{Batch, BatchLimits, FloatRow, HistRow, SeriesRow};
use crate::checkpoint::{self, BlockCheckpoint};
use crate::config::{EngineOptions, FilterConfig, JobConfig, SinkKind};
use crate::control::{Control, JobState};
use crate::error::{Error, Result};
use crate::manifest;
use crate::mimir::MimirSource;
use crate::planner::{self, Plan, PlannedBlock};
use crate::sink::{self, BatchSink};
use crate::stats::{ms_to_rfc3339, BlockStats, JobStats, Totals};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

pub trait EventSink: Send + Sync {
    fn log(&self, level: LogLevel, msg: &str);
}

pub struct NoopEvents;

impl EventSink for NoopEvents {
    fn log(&self, _level: LogLevel, _msg: &str) {}
}

#[derive(Debug, Clone)]
pub struct RunReport {
    pub state: JobState,
    pub totals: Totals,
    pub failed_blocks: usize,
    pub skipped_blocks: usize,
}

pub struct Engine {
    pub cfg: JobConfig,
    pub plan: Plan,
    pub stats: Arc<JobStats>,
    pub control: Arc<Control>,
    events: Arc<dyn EventSink>,
    /// Present when the job reads from a Mimir/object-store source.
    pub mimir: Option<Arc<MimirSource>>,
}

impl Engine {
    pub fn new(cfg: JobConfig, control: Arc<Control>, events: Arc<dyn EventSink>) -> Result<Self> {
        let job_id = format!("job-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S-%3f"));
        Self::new_with_id(cfg, control, events, job_id, None)
    }

    pub fn new_with_id(
        cfg: JobConfig,
        control: Arc<Control>,
        events: Arc<dyn EventSink>,
        job_id: String,
        started_ms: Option<i64>,
    ) -> Result<Self> {
        cfg.validate()?;
        let mimir = match &cfg.mimir {
            Some(m) => Some(Arc::new(MimirSource::new(m, &cfg.output_dir)?)),
            None => None,
        };
        let plan = match &mimir {
            Some(source) => source.plan(&cfg)?,
            None => planner::plan(&cfg)?,
        };
        let stats = Arc::new(JobStats::new(job_id, cfg.clone(), &plan.blocks, started_ms));
        for w in &plan.warnings {
            events.log(LogLevel::Warn, w);
        }
        events.log(
            LogLevel::Info,
            &format!(
                "计划完成：{} 个 block，{} series，{} chunks，{} samples（{:.2} GiB 压缩 chunk）",
                plan.blocks.len(),
                plan.series_total(),
                plan.chunks_total(),
                plan.samples_expected(),
                plan.blocks
                    .iter()
                    .map(|b| b.info.chunks_bytes as f64)
                    .sum::<f64>()
                    / (1024.0 * 1024.0 * 1024.0)
            ),
        );
        Ok(Self {
            cfg,
            plan,
            stats,
            control,
            events,
            mimir,
        })
    }

    /// Restores progress counters from completion markers and checkpoints
    /// without migrating anything. Used by the server after a restart so the
    /// console shows the real position before the user resumes.
    pub fn hydrate_progress(&self) -> Result<()> {
        for b in &self.stats.blocks {
            let tenant = b.tenant.as_deref().unwrap_or("");
            let out_dir = sink::block_output_dir(&self.cfg, tenant, &b.ulid);
            let marker = out_dir.join("_complete.json");
            if marker.exists() {
                if let Ok(bytes) = std::fs::read(&marker) {
                    if let Ok(m) = serde_json::from_slice::<BlockComplete>(&bytes) {
                        b.restore(
                            m.series_done,
                            m.chunks_done,
                            m.units_done,
                            m.floats,
                            m.histograms,
                            m.skipped_tombstone,
                            m.skipped_range,
                            m.bytes_read,
                            m.next_offset,
                        );
                        b.bytes_written.store(m.bytes_written, Ordering::Relaxed);
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&m.completed_at) {
                            b.set_finished_ms(dt.timestamp_millis());
                        }
                        b.set_status(crate::stats::BlockStatus::Completed);
                        continue;
                    }
                }
            }
            if let Some(ckpt) = checkpoint::load(&self.cfg.state_dir(), tenant, &b.ulid)? {
                b.restore(
                    ckpt.series_done,
                    ckpt.chunks_done,
                    ckpt.units_done,
                    ckpt.floats,
                    ckpt.histograms,
                    ckpt.skipped_tombstone,
                    ckpt.skipped_range,
                    ckpt.bytes_read,
                    ckpt.next_offset,
                );
                b.set_status(crate::stats::BlockStatus::Paused);
            }
        }
        Ok(())
    }

    pub fn run(&self) -> Result<RunReport> {
        if !self.control.start() {
            return Err(Error::state(format!(
                "job cannot start from state {:?}",
                self.control.state()
            )));
        }
        self.stats.clear_finished();
        sink::check_writable(&self.cfg.output_dir)?;
        std::fs::create_dir_all(self.cfg.state_dir().join("blocks"))?;
        std::fs::create_dir_all(self.cfg.blocks_dir())?;
        let estimate = planner::estimated_output_bytes(&self.plan);
        self.events.log(
            LogLevel::Info,
            &format!(
                "预计输出 {:.2} GiB（Parquet/zstd 估算，实际以落盘为准）",
                estimate as f64 / (1024.0 * 1024.0 * 1024.0)
            ),
        );

        let n_workers = self.cfg.options.block_parallel.max(1);
        self.control.set_active_blocks(n_workers);
        let queue = Mutex::new(VecDeque::from(self.plan.blocks.clone()));
        let failures = AtomicU64::new(0);
        let skipped = AtomicU64::new(0);
        let done = Arc::new(AtomicBool::new(false));

        // The monitor lives outside the block-worker scope so that it can be
        // signalled after all workers have joined.
        let monitor = {
            let stats = self.stats.clone();
            let control = self.control.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) && !control.is_stop() {
                    stats.tick_rates();
                    for _ in 0..10 {
                        if done.load(Ordering::Relaxed) || control.is_stop() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            })
        };

        std::thread::scope(|scope| {
            for _ in 0..n_workers {
                let queue = &queue;
                let failures = &failures;
                let skipped = &skipped;
                scope.spawn(move || {
                    loop {
                        if self.control.is_stop() {
                            break;
                        }
                        self.control.pause_point();
                        if self.control.is_stop() {
                            break;
                        }
                        let next = {
                            let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
                            q.pop_front()
                        };
                        let Some(planned) = next else { break };
                        match self.process_block(planned) {
                            Ok(BlockOutcome::Completed) => {}
                            Ok(BlockOutcome::Skipped) => {
                                skipped.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(BlockOutcome::Stopped) => break,
                            Err(e) => {
                                failures.fetch_add(1, Ordering::Relaxed);
                                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                                self.events
                                    .log(LogLevel::Error, &format!("block 迁移失败：{e}"));
                                if !self.cfg.options.continue_on_error {
                                    self.control.request_stop();
                                    break;
                                }
                            }
                        }
                    }
                    self.control.block_finished();
                });
            }
        });

        done.store(true, Ordering::Relaxed);
        let _ = monitor.join();
        self.stats.tick_rates();

        self.stats.mark_finished();
        let failed = failures.load(Ordering::Relaxed) as usize;
        let skipped_n = skipped.load(Ordering::Relaxed) as usize;
        let state = if self.control.is_stop() {
            self.control.finish();
            JobState::Stopped
        } else if failed > 0 {
            let msg = format!("{failed} 个 block 迁移失败，其余已完成");
            self.control.fail(msg);
            JobState::Failed
        } else {
            self.control.finish();
            JobState::Completed
        };

        // Manifest is always written, even for stopped/failed runs.
        let warnings = self.plan.warnings.clone();
        if let Err(e) = manifest::write(&self.cfg.output_dir, &self.stats, state, warnings) {
            self.events
                .log(LogLevel::Warn, &format!("写 manifest 失败：{e}"));
        }

        if state == JobState::Completed && self.cfg.sink.kind != SinkKind::Ndjson {
            if let Err(e) = crate::duckdb::write_sql(&self.cfg.output_dir) {
                self.events
                    .log(LogLevel::Warn, &format!("生成 DuckDB 装载 SQL 失败：{e}"));
            }
            if self.cfg.sink.kind == SinkKind::Duckdb && self.cfg.sink.duckdb_exec {
                let db = if self.cfg.sink.duckdb_path.is_empty() {
                    self.cfg.output_dir.join("migration.duckdb")
                } else {
                    PathBuf::from(&self.cfg.sink.duckdb_path)
                };
                match crate::duckdb::execute(&self.cfg.output_dir, &db) {
                    Ok(()) => self.events.log(
                        LogLevel::Info,
                        &format!("DuckDB 装载完成：{}", db.display()),
                    ),
                    Err(e) => self
                        .events
                        .log(LogLevel::Warn, &format!("DuckDB 装载跳过：{e}")),
                }
            }
        }

        let totals = self.stats.totals();
        self.events.log(
            LogLevel::Info,
            &format!(
                "任务结束：state={state:?}，samples={}，跳过 tombstone={}，范围外={}，输出 {:.2} MiB",
                totals.samples,
                totals.samples_skipped_tombstone,
                totals.samples_skipped_range,
                totals.bytes_written as f64 / (1024.0 * 1024.0)
            ),
        );
        Ok(RunReport {
            state,
            totals,
            failed_blocks: failed,
            skipped_blocks: skipped_n,
        })
    }

    fn process_block(&self, planned: PlannedBlock) -> Result<BlockOutcome> {
        let block_id = planned.info.meta.ulid.clone();
        let tenant = planned.info.tenant.clone();
        let block_key = planned.info.key();
        let block_stats = self
            .stats
            .blocks
            .iter()
            .find(|b| b.key == block_key)
            .ok_or_else(|| Error::state(format!("block {block_key} missing from job stats")))?
            .clone();
        let out_dir = sink::block_output_dir(&self.cfg, &tenant, &block_id);
        let marker_path = out_dir.join("_complete.json");

        if marker_path.exists() {
            if self.cfg.options.force || !self.cfg.options.resume {
                self.events.log(
                    LogLevel::Info,
                    &format!("block {block_id}: 强制重迁，清理已有输出"),
                );
                sink::remove_block_output(&self.cfg, &tenant, &block_id)?;
                checkpoint::remove(&self.cfg.state_dir(), &tenant, &block_id)?;
            } else {
                if let Ok(bytes) = std::fs::read(&marker_path) {
                    if let Ok(m) = serde_json::from_slice::<BlockComplete>(&bytes) {
                        block_stats.restore(
                            m.series_done,
                            m.chunks_done,
                            m.units_done,
                            m.floats,
                            m.histograms,
                            m.skipped_tombstone,
                            m.skipped_range,
                            m.bytes_read,
                            m.next_offset,
                        );
                        block_stats
                            .bytes_written
                            .store(m.bytes_written, Ordering::Relaxed);
                    }
                }
                block_stats.set_status(crate::stats::BlockStatus::Skipped);
                self.events
                    .log(LogLevel::Info, &format!("block {block_id}: 已完成，跳过"));
                return Ok(BlockOutcome::Skipped);
            }
        } else if self.cfg.options.force || !self.cfg.options.resume {
            sink::remove_block_output(&self.cfg, &tenant, &block_id)?;
            checkpoint::remove(&self.cfg.state_dir(), &tenant, &block_id)?;
        }

        block_stats.set_status(crate::stats::BlockStatus::Running);
        self.events.log(
            LogLevel::Info,
            &format!(
                "block {}: 开始（tenant={} series={} chunks={} 时间=[{},{}]）",
                block_id,
                if tenant.is_empty() { "-" } else { &tenant },
                block_stats.series_total,
                block_stats.chunks_total,
                block_stats.min_time,
                block_stats.max_time
            ),
        );

        if let Some(source) = &self.mimir {
            let t0 = std::time::Instant::now();
            self.events.log(
                LogLevel::Info,
                &format!("block {block_id}: 从对象存储拉取到本地缓存（tenant={tenant}）"),
            );
            if let Err(e) = source.materialize(&tenant, &block_id) {
                block_stats.set_error(e.to_string());
                block_stats.set_status(crate::stats::BlockStatus::Failed);
                return Err(e);
            }
            self.events.log(
                LogLevel::Debug,
                &format!(
                    "block {block_id}: 下载完成，用时 {:.2}s",
                    t0.elapsed().as_secs_f64()
                ),
            );
        }

        let reader = match BlockReader::open_info(
            planned.info.clone(),
            ReaderOptions {
                verify_crc: self.cfg.options.verify_crc,
                mmap_advise: self.cfg.options.mmap_advise,
            },
        ) {
            Ok(r) => r,
            Err(e) => {
                block_stats.set_error(e.to_string());
                block_stats.set_status(crate::stats::BlockStatus::Failed);
                return Err(e.into());
            }
        };

        let series_start = reader.index().series_start();
        let ckpt = if self.cfg.options.resume {
            checkpoint::load(&self.cfg.state_dir(), &tenant, &block_id)?
                .unwrap_or_else(|| BlockCheckpoint::empty(&tenant, &block_id, series_start))
        } else {
            BlockCheckpoint::empty(&tenant, &block_id, series_start)
        };
        let mut start_offset = ckpt.next_offset;
        if start_offset == 0 {
            start_offset = series_start;
        }
        if ckpt.completed_unit >= 0 {
            block_stats.restore(
                ckpt.series_done,
                ckpt.chunks_done,
                ckpt.units_done,
                ckpt.floats,
                ckpt.histograms,
                ckpt.skipped_tombstone,
                ckpt.skipped_range,
                ckpt.bytes_read,
                ckpt.next_offset,
            );
            let removed = checkpoint::cleanup_uncommitted_parts(&out_dir, ckpt.completed_unit)?;
            if removed > 0 {
                self.events.log(
                    LogLevel::Info,
                    &format!("block {block_id}: 清理 {} 个未提交 part 文件", removed),
                );
            }
            self.events.log(
                LogLevel::Info,
                &format!(
                    "block {block_id}: 从 checkpoint 恢复，已完成 {} 个单元，offset={}",
                    ckpt.units_done, start_offset
                ),
            );
        }

        if block_stats.series_total == 0 {
            let s = reader.scan_stats()?;
            if s.series == 0 {
                block_stats.set_status(crate::stats::BlockStatus::Completed);
                write_complete_marker(&out_dir, &block_stats, 0, 0)?;
                if let Some(source) = &self.mimir {
                    let _ = source.cleanup(&tenant, &block_id);
                }
                return Ok(BlockOutcome::Completed);
            }
            // plan should have caught this; keep going with live progress only.
        }

        let start_unit = if ckpt.completed_unit >= 0 {
            ckpt.start_unit()
        } else {
            0
        };

        let (index, chunks, tombstones, _info) = reader.into_parts();
        let outcome = match self.run_block_pipeline(
            index,
            &chunks,
            &tombstones,
            &block_stats,
            start_offset,
            start_unit,
        ) {
            Ok(o) => o,
            Err(e) => {
                block_stats.set_error(e.to_string());
                block_stats.set_status(crate::stats::BlockStatus::Failed);
                return Err(e);
            }
        };
        let bytes_written = sink::dir_size(&out_dir);
        block_stats
            .bytes_written
            .store(bytes_written, Ordering::Relaxed);

        match outcome {
            PipelineOutcome::Completed => {
                let next_offset = block_stats.next_offset.load(Ordering::Relaxed);
                let units = block_stats.units_done.load(Ordering::Relaxed);
                write_complete_marker(&out_dir, &block_stats, units, next_offset)?;
                block_stats.set_status(crate::stats::BlockStatus::Completed);
                self.events.log(
                    LogLevel::Info,
                    &format!(
                        "block {block_id}: 完成（samples={}，跳过 tombstone={}，输出 {:.2} MiB）",
                        block_stats.floats_done.load(Ordering::Relaxed)
                            + block_stats.histograms_done.load(Ordering::Relaxed),
                        block_stats.skipped_tombstone.load(Ordering::Relaxed),
                        bytes_written as f64 / (1024.0 * 1024.0)
                    ),
                );
                let _ = manifest::write(
                    &self.cfg.output_dir,
                    &self.stats,
                    self.control.state(),
                    self.plan.warnings.clone(),
                );
                if let Some(source) = &self.mimir {
                    if let Err(e) = source.cleanup(&tenant, &block_id) {
                        self.events.log(
                            LogLevel::Warn,
                            &format!("block {block_id}: 清理本地缓存失败：{e}"),
                        );
                    }
                }
                Ok(BlockOutcome::Completed)
            }
            PipelineOutcome::Stopped => {
                block_stats.set_status(crate::stats::BlockStatus::Paused);
                Ok(BlockOutcome::Stopped)
            }
        }
    }

    fn run_block_pipeline(
        &self,
        index: tsdb_reader::IndexReader,
        chunks: &ChunkReader,
        tombstones: &Tombstones,
        block_stats: &Arc<BlockStats>,
        start_offset: u64,
        start_unit: u64,
    ) -> Result<PipelineOutcome> {
        let n = self.cfg.options.decode_workers.max(1);
        let tenant = block_stats.tenant.clone().unwrap_or_default();
        let capacity = (self.cfg.options.channel_capacity / n).max(1);
        let mut job_txs = Vec::with_capacity(n);
        let mut job_rxs = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = bounded::<Job>(capacity);
            job_txs.push(tx);
            job_rxs.push(rx);
        }
        let (coord_tx, coord_rx) = unbounded::<CoordMsg>();
        let (permit_tx, permit_rx) = bounded::<Permit>(1);
        let block_stop = Arc::new(AtomicBool::new(false));
        let abort = Arc::new(AtomicBool::new(false));
        let block_id = block_stats.ulid.clone();

        let coord_result: Result<PipelineOutcome> = std::thread::scope(|scope| {
            // Reader.
            {
                let block_stop = block_stop.clone();
                let abort = abort.clone();
                let coord_tx = coord_tx.clone();
                let control = self.control.clone();
                let block_id = block_id.clone();
                let unit_series = self.cfg.options.unit_series;
                scope.spawn(move || {
                    let result = reader_loop(
                        &index,
                        start_offset,
                        start_unit,
                        unit_series,
                        job_txs,
                        coord_tx.clone(),
                        permit_rx,
                        block_stop.clone(),
                        abort.clone(),
                        control.clone(),
                    );
                    if let Err(e) = result {
                        abort.store(true, Ordering::SeqCst);
                        let _ = coord_tx.send(CoordMsg::Error(format!(
                            "block {block_id}: reader error: {e}"
                        )));
                    }
                    let _ = coord_tx.send(CoordMsg::ReaderDone);
                });
            }

            // Workers.
            for (worker, rx) in job_rxs.into_iter().enumerate() {
                let coord_tx = coord_tx.clone();
                let block_stop = block_stop.clone();
                let abort = abort.clone();
                let cfg = self.cfg.clone();
                let stats = block_stats.clone();
                let events = self.events.clone();
                let block_id = block_id.clone();
                let tenant = tenant.clone();
                scope.spawn(move || {
                    let sink = match sink::create_sink(&cfg, &tenant, &block_id, worker) {
                        Ok(s) => s,
                        Err(e) => {
                            abort.store(true, Ordering::SeqCst);
                            let _ = coord_tx.send(CoordMsg::Error(format!(
                                "block {block_id}: create sink: {e}"
                            )));
                            return;
                        }
                    };
                    if let Err(e) = worker_loop(
                        rx,
                        sink,
                        &cfg,
                        &stats,
                        chunks,
                        tombstones,
                        start_unit,
                        &coord_tx,
                        &block_stop,
                        &abort,
                        &*events,
                    ) {
                        abort.store(true, Ordering::SeqCst);
                        let _ = coord_tx.send(CoordMsg::Error(format!(
                            "block {block_id}: worker error: {e}"
                        )));
                    }
                });
            }

            // Coordinator (this thread).
            let mut result = coordinator_loop(
                &coord_rx,
                &permit_tx,
                &block_stop,
                &abort,
                block_stats,
                &self.cfg,
                &self.control,
                &*self.events,
                start_unit,
            );
            if result.is_err() {
                block_stop.store(true, Ordering::SeqCst);
                let _ = permit_tx.try_send(Permit::Stop);
            }
            if matches!(result, Ok(PipelineOutcome::Stopped)) {
                block_stop.store(true, Ordering::SeqCst);
            }
            match &mut result {
                Ok(_) => {}
                Err(_) => {}
            }
            result
        });

        coord_result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockOutcome {
    Completed,
    Skipped,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipelineOutcome {
    Completed,
    Stopped,
}

#[derive(Debug, Clone, Copy)]
enum Permit {
    Continue,
    Stop,
}

enum Job {
    Series(SeriesRef),
    EndUnit(u64),
}

#[derive(Debug, Clone)]
struct UnitMeta {
    unit: u64,
    end_offset: u64,
    #[allow(dead_code)]
    series: u64,
    #[allow(dead_code)]
    chunks: u64,
}

enum CoordMsg {
    Sealed(UnitMeta),
    Done(u64),
    Error(String),
    ReaderDone,
}

#[derive(Default)]
struct PendingUnit {
    meta: Option<UnitMeta>,
    acks: usize,
}

#[allow(clippy::too_many_arguments)]
fn reader_loop(
    reader: &tsdb_reader::IndexReader,
    start_offset: u64,
    start_unit: u64,
    unit_series: usize,
    job_txs: Vec<Sender<Job>>,
    coord_tx: Sender<CoordMsg>,
    permit_rx: Receiver<Permit>,
    block_stop: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    control: Arc<Control>,
) -> Result<()> {
    let n = job_txs.len();
    let dbg = std::env::var_os("TSDB_DEBUG_ENGINE").is_some();
    if dbg {
        eprintln!("[reader] start unit={start_unit} offset={start_offset}");
    }
    let mut unit = start_unit;
    let mut iter = reader.series(start_offset);
    loop {
        if block_stop.load(Ordering::Relaxed) || abort.load(Ordering::Relaxed) || control.is_stop()
        {
            break;
        }
        let mut count: u64 = 0;
        let mut chunk_count: u64 = 0;
        while (count as usize) < unit_series {
            if block_stop.load(Ordering::Relaxed) || abort.load(Ordering::Relaxed) {
                break;
            }
            match iter.next() {
                Some(Ok(series)) => {
                    chunk_count += series.chunks.len() as u64;
                    let worker = (count as usize) % n;
                    if job_txs[worker].send(Job::Series(series)).is_err() {
                        return Ok(());
                    }
                    count += 1;
                }
                Some(Err(e)) => {
                    let _ = coord_tx.send(CoordMsg::Error(format!(
                        "index parse error at offset {}: {e}",
                        iter.position()
                    )));
                    abort.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                None => break,
            }
        }
        if count == 0 {
            if dbg {
                eprintln!("[reader] no more series at unit={unit}");
            }
            break;
        }
        let end_offset = iter.position();
        if coord_tx
            .send(CoordMsg::Sealed(UnitMeta {
                unit,
                end_offset,
                series: count,
                chunks: chunk_count,
            }))
            .is_err()
        {
            break;
        }
        for tx in &job_txs {
            let _ = tx.send(Job::EndUnit(unit));
        }
        if dbg {
            eprintln!(
                "[reader] unit={unit} sealed count={count} chunks={chunk_count} waiting permit"
            );
        }
        match permit_rx.recv() {
            Ok(Permit::Continue) => {
                if dbg {
                    eprintln!("[reader] permit continue after unit={unit}");
                }
                unit += 1;
            }
            Ok(Permit::Stop) | Err(_) => {
                if dbg {
                    eprintln!("[reader] permit stop at unit={unit}");
                }
                break;
            }
        }
    }
    if dbg {
        eprintln!("[reader] exit unit={unit}");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn worker_loop(
    rx: Receiver<Job>,
    mut sink: Box<dyn BatchSink>,
    cfg: &JobConfig,
    stats: &BlockStats,
    chunks: &ChunkReader,
    tombstones: &Tombstones,
    start_unit: u64,
    coord_tx: &Sender<CoordMsg>,
    block_stop: &AtomicBool,
    abort: &AtomicBool,
    events: &dyn EventSink,
) -> Result<()> {
    let limits = BatchLimits {
        max_samples: cfg.options.batch_samples,
        max_bytes: cfg.options.batch_bytes,
    };
    let filter = SampleFilter { cfg: &cfg.filter };
    let dbg = std::env::var_os("TSDB_DEBUG_ENGINE").is_some();
    if dbg {
        eprintln!("[worker] start unit={start_unit}");
    }
    let mut batch = Batch::new();
    let mut current_unit = start_unit;

    loop {
        if abort.load(Ordering::Relaxed) {
            // Keep draining briefly so the reader is not blocked on a full
            // channel; the coordinator is already unwinding.
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(Job::Series(_)) | Ok(Job::EndUnit(_)) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {
                    if block_stop.load(Ordering::Relaxed) {
                        break;
                    }
                    continue;
                }
            }
        }
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(Job::Series(series)) => {
                process_series(
                    cfg, stats, &filter, &series, chunks, tombstones, &mut batch, events,
                )?;
                stats.series_done.fetch_add(1, Ordering::Relaxed);
                if batch.is_full(limits) {
                    sink.write(current_unit, &mut batch)?;
                }
            }
            Ok(Job::EndUnit(unit)) => {
                if !batch.is_empty() {
                    sink.write(unit, &mut batch)?;
                }
                sink.finish_unit(unit)?;
                let _ = coord_tx.send(CoordMsg::Done(unit));
                current_unit = unit + 1;
            }
            Err(RecvTimeoutError::Timeout) => {
                if block_stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    // Finalize anything left (e.g. on stop after the last ack).
    let _ = sink.end_block();
    if dbg {
        eprintln!("[worker] exit unit={current_unit}");
    }
    Ok(())
}

struct SampleFilter<'a> {
    cfg: &'a FilterConfig,
}

#[allow(clippy::too_many_arguments)]
fn process_series(
    cfg: &JobConfig,
    stats: &BlockStats,
    filter: &SampleFilter<'_>,
    series: &SeriesRef,
    chunks: &ChunkReader,
    tombstones: &Tombstones,
    batch: &mut Batch,
    events: &dyn EventSink,
) -> Result<()> {
    if !filter.cfg.matches_labels(&series.labels) {
        stats.series_skipped_filter.fetch_add(1, Ordering::Relaxed);
        stats
            .chunks_done
            .fetch_add(series.chunks.len() as u64, Ordering::Relaxed);
        return Ok(());
    }

    let uid = series_uid(&series.labels);
    let intervals = tombstones.intervals(series.id);
    let mut adapter = BatchAdapter {
        batch,
        uid,
        series_ref: series.id,
        labels: &series.labels,
        written: false,
        filter: filter.cfg,
        tombstones: intervals,
        stats,
    };

    for meta in &series.chunks {
        stats.chunks_done.fetch_add(1, Ordering::Relaxed);
        if !filter.cfg.chunk_relevant(meta.min_time, meta.max_time) {
            continue;
        }
        let slice = match chunks.chunk(meta.reference) {
            Ok(s) => s,
            Err(e) => {
                classify_error(stats, &e);
                if cfg.options.skip_bad_chunks {
                    events.log(
                        LogLevel::Warn,
                        &format!(
                            "block {}: 跳过损坏 chunk ref={:#x}（series ref={}）：{e}",
                            stats.ulid, meta.reference, series.id
                        ),
                    );
                    continue;
                }
                return Err(Error::Reader(e));
            }
        };
        stats
            .bytes_read
            .fetch_add(slice.encoded_len as u64, Ordering::Relaxed);
        if let Err(e) = chunkenc::decode(slice.encoding, slice.data, &mut adapter) {
            classify_error(stats, &e);
            if cfg.options.skip_bad_chunks {
                events.log(
                    LogLevel::Warn,
                    &format!(
                        "block {}: chunk 解码失败 ref={:#x} encoding={}：{e}",
                        stats.ulid,
                        meta.reference,
                        chunkenc::encoding_name(slice.encoding)
                    ),
                );
                continue;
            }
            return Err(Error::Reader(e));
        }
    }
    Ok(())
}

fn classify_error(stats: &BlockStats, e: &tsdb_reader::Error) {
    match e {
        tsdb_reader::Error::Checksum(_) => {
            stats.crc_errors.fetch_add(1, Ordering::Relaxed);
        }
        tsdb_reader::Error::Unsupported(_) => {
            stats.decode_errors.fetch_add(1, Ordering::Relaxed);
            stats.unknown_encodings.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            stats.decode_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn series_uid(labels: &[(std::sync::Arc<str>, std::sync::Arc<str>)]) -> u64 {
    let mut buf = Vec::with_capacity(
        labels
            .iter()
            .map(|(k, v)| k.len() + v.len() + 2)
            .sum::<usize>(),
    );
    for (k, v) in labels {
        buf.extend_from_slice(k.as_bytes());
        buf.push(0);
        buf.extend_from_slice(v.as_bytes());
        buf.push(0);
    }
    xxhash_rust::xxh3::xxh3_64(&buf)
}

struct BatchAdapter<'a> {
    batch: &'a mut Batch,
    uid: u64,
    series_ref: u64,
    labels: &'a [(std::sync::Arc<str>, std::sync::Arc<str>)],
    written: bool,
    filter: &'a FilterConfig,
    tombstones: &'a [(i64, i64)],
    stats: &'a BlockStats,
}

impl BatchAdapter<'_> {
    fn ensure_series(&mut self) {
        if !self.written {
            self.batch.push_series(SeriesRow {
                uid: self.uid,
                series_ref: self.series_ref,
                labels: self.labels.to_vec(),
            });
            self.written = true;
        }
    }
}

impl SampleSink for BatchAdapter<'_> {
    fn push_float(&mut self, ts: i64, value: f64) {
        if !self.filter.accepts_ts(ts) {
            self.stats.skipped_range.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if is_deleted(self.tombstones, ts) {
            self.stats.skipped_tombstone.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.ensure_series();
        self.batch.push_float(FloatRow {
            uid: self.uid,
            ts,
            value,
        });
        self.stats.floats_done.fetch_add(1, Ordering::Relaxed);
    }

    fn push_histogram(&mut self, h: &HistogramSample<'_>) {
        if !self.filter.accepts_ts(h.ts) {
            self.stats.skipped_range.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if is_deleted(self.tombstones, h.ts) {
            self.stats.skipped_tombstone.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.ensure_series();
        self.batch.push_hist(HistRow {
            uid: self.uid,
            ts: h.ts,
            st: h.st,
            count: h.count,
            zero_count: h.zero_count,
            sum: h.sum,
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            pos_spans: h.pos_spans.to_vec(),
            neg_spans: h.neg_spans.to_vec(),
            pos_buckets: h.pos_buckets.to_vec(),
            neg_buckets: h.neg_buckets.to_vec(),
            custom_values: h.custom_values.to_vec(),
            reset_hint: h.reset_hint,
        });
        self.stats.histograms_done.fetch_add(1, Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
fn coordinator_loop(
    coord_rx: &Receiver<CoordMsg>,
    permit_tx: &Sender<Permit>,
    block_stop: &AtomicBool,
    abort: &AtomicBool,
    stats: &Arc<BlockStats>,
    cfg: &JobConfig,
    control: &Control,
    events: &dyn EventSink,
    start_unit: u64,
) -> Result<PipelineOutcome> {
    let mut pending: BTreeMap<u64, PendingUnit> = BTreeMap::new();
    let mut next_commit = start_unit;
    let mut reader_done = false;
    let workers = cfg.options.decode_workers.max(1);

    loop {
        // Commit every unit whose metadata and worker acks have arrived.
        loop {
            let ready = pending
                .get(&next_commit)
                .map(|p| p.meta.is_some() && p.acks >= workers)
                .unwrap_or(false);
            if !ready {
                break;
            }
            let meta = pending
                .get_mut(&next_commit)
                .and_then(|p| p.meta.take())
                .unwrap();
            commit_unit(cfg, stats, next_commit, meta.end_offset)?;
            pending.remove(&next_commit);
            if std::env::var_os("TSDB_DEBUG_ENGINE").is_some() {
                eprintln!("[coord] committed unit={}", next_commit);
            }
            next_commit += 1;

            if control.is_stop() {
                block_stop.store(true, Ordering::SeqCst);
                let _ = permit_tx.try_send(Permit::Stop);
                return Ok(PipelineOutcome::Stopped);
            }
            control.pause_point();
            if control.is_stop() {
                block_stop.store(true, Ordering::SeqCst);
                let _ = permit_tx.try_send(Permit::Stop);
                return Ok(PipelineOutcome::Stopped);
            }
            events.log(
                LogLevel::Debug,
                &format!("block {}: 单元 {} 已提交", stats.ulid, next_commit - 1),
            );
            if permit_tx.send(Permit::Continue).is_err() {
                reader_done = true;
                break;
            }
        }

        if abort.load(Ordering::Relaxed) {
            block_stop.store(true, Ordering::SeqCst);
            let _ = permit_tx.try_send(Permit::Stop);
            return Err(Error::Aborted(format!("block {} 迁移被中止", stats.ulid)));
        }

        if reader_done && pending.is_empty() {
            return Ok(PipelineOutcome::Completed);
        }
        if control.is_stop() {
            block_stop.store(true, Ordering::SeqCst);
            let _ = permit_tx.try_send(Permit::Stop);
            return Ok(PipelineOutcome::Stopped);
        }

        match coord_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(CoordMsg::Sealed(meta)) => {
                if std::env::var_os("TSDB_DEBUG_ENGINE").is_some() {
                    eprintln!("[coord] sealed unit={} end={}", meta.unit, meta.end_offset);
                }
                let unit = meta.unit;
                pending.entry(unit).or_default().meta = Some(meta);
            }
            Ok(CoordMsg::Done(unit)) => {
                let e = pending.entry(unit).or_default();
                e.acks += 1;
                if std::env::var_os("TSDB_DEBUG_ENGINE").is_some() {
                    eprintln!("[coord] ack unit={} acks={}", unit, e.acks);
                }
            }
            Ok(CoordMsg::Error(e)) => {
                abort.store(true, Ordering::SeqCst);
                block_stop.store(true, Ordering::SeqCst);
                let _ = permit_tx.try_send(Permit::Stop);
                return Err(Error::Aborted(e));
            }
            Ok(CoordMsg::ReaderDone) => {
                if std::env::var_os("TSDB_DEBUG_ENGINE").is_some() {
                    eprintln!("[coord] reader done, pending={}", pending.len());
                }
                reader_done = true;
                // The final unit may not have been sealed if the reader was
                // aborted; pending units still get their acks and commit.
                if pending.is_empty() {
                    // Give workers a moment to deliver late acks before
                    // declaring completion.
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                reader_done = true;
            }
        }
    }
}

fn commit_unit(cfg: &JobConfig, stats: &BlockStats, unit: u64, end_offset: u64) -> Result<()> {
    let ckpt = BlockCheckpoint {
        tenant: stats.tenant.clone().unwrap_or_default(),
        block: stats.ulid.clone(),
        completed_unit: unit as i64,
        next_offset: end_offset,
        series_done: stats.series_done.load(Ordering::Relaxed),
        chunks_done: stats.chunks_done.load(Ordering::Relaxed),
        units_done: stats.units_done.load(Ordering::Relaxed) + 1,
        floats: stats.floats_done.load(Ordering::Relaxed),
        histograms: stats.histograms_done.load(Ordering::Relaxed),
        skipped_tombstone: stats.skipped_tombstone.load(Ordering::Relaxed),
        skipped_range: stats.skipped_range.load(Ordering::Relaxed),
        bytes_read: stats.bytes_read.load(Ordering::Relaxed),
        updated_at: ms_to_rfc3339(chrono::Utc::now().timestamp_millis()),
    };
    checkpoint::save(&cfg.state_dir(), &ckpt)?;
    stats.units_done.store(ckpt.units_done, Ordering::Relaxed);
    stats.current_unit.store(unit, Ordering::Relaxed);
    stats.next_offset.store(end_offset, Ordering::Relaxed);
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct BlockComplete {
    ulid: String,
    completed_at: String,
    units_done: u64,
    next_offset: u64,
    series_done: u64,
    chunks_done: u64,
    floats: u64,
    histograms: u64,
    skipped_tombstone: u64,
    skipped_range: u64,
    bytes_read: u64,
    bytes_written: u64,
}

fn write_complete_marker(
    out_dir: &std::path::Path,
    stats: &BlockStats,
    units_done: u64,
    next_offset: u64,
) -> Result<()> {
    let marker = BlockComplete {
        ulid: stats.ulid.clone(),
        completed_at: ms_to_rfc3339(chrono::Utc::now().timestamp_millis()),
        units_done,
        next_offset,
        series_done: stats.series_done.load(Ordering::Relaxed),
        chunks_done: stats.chunks_done.load(Ordering::Relaxed),
        floats: stats.floats_done.load(Ordering::Relaxed),
        histograms: stats.histograms_done.load(Ordering::Relaxed),
        skipped_tombstone: stats.skipped_tombstone.load(Ordering::Relaxed),
        skipped_range: stats.skipped_range.load(Ordering::Relaxed),
        bytes_read: stats.bytes_read.load(Ordering::Relaxed),
        bytes_written: stats.bytes_written.load(Ordering::Relaxed),
    };
    std::fs::create_dir_all(out_dir)?;
    let path = out_dir.join("_complete.json");
    let tmp = out_dir.join("_complete.json.tmp");
    let data = serde_json::to_vec_pretty(&marker)
        .map_err(|e| Error::state(format!("serialize block marker: {e}")))?;
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Validates engine options without requiring real paths (used by the API).
pub fn validate_options(options: &EngineOptions) -> Result<()> {
    let mut cfg = JobConfig::default();
    cfg.data_dir = "/".into();
    cfg.output_dir = "/tmp".into();
    cfg.options = options.clone();
    cfg.validate()
}
