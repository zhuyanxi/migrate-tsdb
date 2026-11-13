//! Grafana Mimir object-storage source.
//!
//! Mimir stores TSDB blocks under `<storage-prefix>/<tenant>/<ULID>/`:
//!
//! ```text
//! bucketA/
//!   tenant-a/
//!     01H.../meta.json,index,tombstones,chunks/000001...
//!   tenant-b/
//!     01H.../...
//! ```
//!
//! We discover tenants via a delimiter listing, parse each tenant's object
//! listing into block summaries, then download one block at a time into a
//! bounded local cache before handing it to the existing mmap reader. This
//! keeps the reader/pipeline unchanged and makes memory independent of object
//! size.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjPath;
use object_store::{ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, RetryConfig};
use tokio::io::AsyncWriteExt;

use tsdb_reader::{BlockInfo, BlockMeta};

use crate::config::{is_valid_tenant, JobConfig, MimirBackend, MimirConfig};
use crate::error::{Error, Result};
use crate::planner::{Plan, PlannedBlock};

const READY_MARKER: &str = ".tsdb-migrate-ready";
/// Suffix of an in-flight object download. Kept next to the final file so a
/// failed run can resume from the bytes already on disk.
const PART_SUFFIX: &str = ".tsdb-part";
const META_CONCURRENCY: usize = 16;
/// First retry delay for a failed part; doubles per consecutive failure.
const RETRY_BASE_MS: u64 = 250;
const RETRY_MAX_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub struct TenantInfo {
    pub id: String,
    pub blocks: u64,
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone)]
struct ObjectEntry {
    key: String,
    size: u64,
}

#[derive(Debug, Clone, Default)]
struct BlockSummary {
    ulid: String,
    has_meta: bool,
    has_index: bool,
    chunk_objects: u64,
    objects: u64,
    bytes: u64,
    index_bytes: u64,
    chunks_bytes: u64,
    tombstones_bytes: u64,
    deletion_mark: bool,
}

#[derive(Clone)]
enum Backend {
    S3(AmazonS3),
    Local(PathBuf),
}

pub struct MimirSource {
    cfg: MimirConfig,
    backend: Backend,
    runtime: Option<tokio::runtime::Runtime>,
    cache_root: PathBuf,
}

impl MimirSource {
    pub fn new(mimir: &MimirConfig, output_dir: &Path) -> Result<Self> {
        mimir.validate()?;
        let cache_root = mimir.cache_root(output_dir);
        let (backend, runtime) = match mimir.backend {
            MimirBackend::Local => {
                let root = PathBuf::from(&mimir.bucket);
                if !root.is_dir() {
                    return Err(Error::config(format!(
                        "mimir local bucket root is not a directory: {}",
                        root.display()
                    )));
                }
                (Backend::Local(root), None)
            }
            MimirBackend::S3 => {
                // object_store's defaults are hostile to multi-GB objects:
                //  * `ClientOptions::timeout` is 30 s and covers *the whole
                //    response body* (reqwest's `TotalTimeoutBody`), so any
                //    object that takes longer than 30 s to stream dies with
                //    "HTTP error: request or response body error";
                //  * `RetryConfig::retry_timeout` is 180 s from the first
                //    request, after which body-stream retries stop and the
                //    error is surfaced.
                // We disable the request deadline (and instead enforce a stall
                // timeout) and keep object_store's internal retry budget
                // generous, because range-level recovery is done by our own
                // part loop below.
                let mut client_options = if mimir.download_timeout_secs == 0 {
                    ClientOptions::default().with_timeout_disabled()
                } else {
                    ClientOptions::default()
                        .with_timeout(Duration::from_secs(mimir.download_timeout_secs))
                };
                client_options = if mimir.download_read_timeout_secs == 0 {
                    client_options.with_read_timeout_disabled()
                } else {
                    client_options
                        .with_read_timeout(Duration::from_secs(mimir.download_read_timeout_secs))
                };
                let retry = RetryConfig {
                    max_retries: mimir.download_retries.clamp(1, 10),
                    retry_timeout: Duration::from_secs(3600),
                    ..Default::default()
                };
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(&mimir.bucket)
                    .with_region(&mimir.region)
                    .with_client_options(client_options)
                    .with_retry(retry);
                if !mimir.endpoint.trim().is_empty() {
                    builder = builder
                        .with_endpoint(mimir.endpoint.trim())
                        .with_allow_http(mimir.allow_http)
                        .with_virtual_hosted_style_request(!mimir.force_path_style);
                }
                let s3 = builder
                    .build()
                    .map_err(|e| Error::Other(format!("mimir: build S3 client: {e}")))?;
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .map_err(|e| Error::Other(format!("mimir: create runtime: {e}")))?;
                (Backend::S3(s3), Some(rt))
            }
        };
        Ok(Self {
            cfg: mimir.clone(),
            backend,
            runtime,
            cache_root,
        })
    }

    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    fn prefix(&self) -> String {
        self.cfg.normalized_prefix()
    }

    fn tenant_prefix(&self, tenant: &str) -> String {
        format!("{}{tenant}/", self.prefix())
    }

    fn block_prefix(&self, tenant: &str, ulid: &str) -> String {
        format!("{}{}/", self.tenant_prefix(tenant), ulid)
    }

    pub fn cache_path(&self, tenant: &str, ulid: &str) -> PathBuf {
        self.cache_root.join(tenant).join(ulid)
    }

    /// Lists tenant directories under the storage prefix.
    pub fn list_tenants(&self) -> Result<Vec<String>> {
        let mut tenants = match &self.backend {
            Backend::S3(store) => {
                let rt = self.runtime.as_ref().expect("s3 runtime");
                let store = store.clone();
                let root = self.prefix();
                rt.block_on(async move {
                    let prefix = if root.is_empty() {
                        None
                    } else {
                        Some(ObjPath::from(root.trim_end_matches('/')))
                    };
                    let result = store
                        .list_with_delimiter(prefix.as_ref())
                        .await
                        .map_err(|e| Error::Other(format!("mimir: list tenants: {e}")))?;
                    let mut out = Vec::new();
                    for p in result.common_prefixes {
                        if let Some(t) = p.to_string().trim_end_matches('/').rsplit('/').next() {
                            if is_valid_tenant(t) {
                                out.push(t.to_string());
                            }
                        }
                    }
                    Ok::<Vec<String>, Error>(out)
                })?
            }
            Backend::Local(root) => {
                let dir = root.join(self.prefix().trim_end_matches('/'));
                let mut out = Vec::new();
                if dir.is_dir() {
                    for entry in std::fs::read_dir(&dir)? {
                        let entry = entry?;
                        if entry.file_type()?.is_dir() {
                            if let Some(name) = entry.file_name().to_str() {
                                if is_valid_tenant(name) {
                                    out.push(name.to_string());
                                }
                            }
                        }
                    }
                }
                out
            }
        };
        tenants.sort();
        tenants.dedup();
        if !self.cfg.tenants.is_empty() {
            tenants.retain(|t| self.cfg.tenants.iter().any(|x| x == t));
        }
        Ok(tenants)
    }

    /// Fast tenant inventory used by the API discovery endpoint.
    pub fn list_tenant_infos(&self) -> Result<Vec<TenantInfo>> {
        let mut out = Vec::new();
        for tenant in self.list_tenants()? {
            let blocks = self.scan_tenant(&tenant)?;
            out.push(TenantInfo {
                id: tenant,
                blocks: blocks.len() as u64,
                objects: blocks.iter().map(|b| b.objects).sum(),
                bytes: blocks.iter().map(|b| b.bytes).sum(),
            });
        }
        Ok(out)
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        match &self.backend {
            Backend::S3(store) => {
                let rt = self.runtime.as_ref().expect("s3 runtime");
                let store = store.clone();
                let prefix = prefix.trim_end_matches('/').to_string();
                rt.block_on(async move {
                    let mut stream = store.list(Some(&ObjPath::from(prefix)));
                    let mut out = Vec::new();
                    while let Some(item) = stream.next().await {
                        let meta =
                            item.map_err(|e| Error::Other(format!("mimir: list objects: {e}")))?;
                        out.push(ObjectEntry {
                            key: meta.location.to_string(),
                            size: meta.size,
                        });
                    }
                    Ok(out)
                })
            }
            Backend::Local(root) => {
                let dir = root.join(prefix.trim_end_matches('/'));
                let mut out = Vec::new();
                if dir.is_dir() {
                    walk_files(root, &dir, &mut out)?;
                }
                Ok(out)
            }
        }
    }

    fn scan_tenant(&self, tenant: &str) -> Result<Vec<BlockSummary>> {
        let prefix = self.tenant_prefix(tenant);
        let blocks = match &self.backend {
            Backend::S3(store) => {
                let rt = self.runtime.as_ref().expect("s3 runtime");
                let store = store.clone();
                let prefix2 = prefix.clone();
                rt.block_on(async move {
                    let mut blocks: BTreeMap<String, BlockSummary> = BTreeMap::new();
                    let mut stream = store.list(Some(&ObjPath::from(
                        prefix2.trim_end_matches('/').to_string(),
                    )));
                    while let Some(item) = stream.next().await {
                        let meta =
                            item.map_err(|e| Error::Other(format!("mimir: list objects: {e}")))?;
                        group_object(&prefix2, &mut blocks, &meta.location.to_string(), meta.size);
                    }
                    Ok::<BTreeMap<String, BlockSummary>, Error>(blocks)
                })?
            }
            Backend::Local(root) => {
                let mut blocks: BTreeMap<String, BlockSummary> = BTreeMap::new();
                let dir = root.join(prefix.trim_end_matches('/'));
                if dir.is_dir() {
                    walk_summaries(root, &dir, &prefix, &mut blocks)?;
                }
                blocks
            }
        };
        Ok(blocks
            .into_values()
            .filter(|b| b.has_meta && b.has_index && b.chunk_objects > 0 && !b.deletion_mark)
            .collect())
    }

    fn fetch_metas(
        &self,
        tenant: &str,
        blocks: &[BlockSummary],
    ) -> Result<Vec<(String, Option<BlockMeta>, Option<String>)>> {
        match &self.backend {
            Backend::S3(store) => {
                let rt = self.runtime.as_ref().expect("s3 runtime");
                let store = store.clone();
                let tenant = tenant.to_string();
                let prefix = self.tenant_prefix(tenant.as_str());
                let items: Vec<(String, String)> = blocks
                    .iter()
                    .map(|b| (b.ulid.clone(), format!("{}{}/meta.json", prefix, b.ulid)))
                    .collect();
                let results = rt.block_on(async move {
                    stream::iter(items.into_iter().map(|(ulid, key)| {
                        let store = store.clone();
                        async move {
                            let res = async {
                                let r = store.get(&ObjPath::from(key)).await.map_err(|e| {
                                    Error::Other(format!("mimir: get meta.json: {e}"))
                                })?;
                                let bytes = r.bytes().await.map_err(|e| {
                                    Error::Other(format!("mimir: read meta.json: {e}"))
                                })?;
                                serde_json::from_slice::<BlockMeta>(&bytes).map_err(|e| {
                                    Error::Other(format!("mimir: parse meta.json: {e}"))
                                })
                            }
                            .await;
                            (ulid, res)
                        }
                    }))
                    .buffer_unordered(META_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await
                });
                Ok(results
                    .into_iter()
                    .map(|(ulid, res)| match res {
                        Ok(meta) => (ulid, Some(meta), None),
                        Err(e) => (ulid, None, Some(e.to_string())),
                    })
                    .collect())
            }
            Backend::Local(root) => {
                let prefix = self.tenant_prefix(tenant);
                Ok(blocks
                    .iter()
                    .map(|b| {
                        let path = root.join(format!("{}{}/meta.json", prefix, b.ulid));
                        match std::fs::read(&path)
                            .map_err(|e| e.to_string())
                            .and_then(|bytes| {
                                serde_json::from_slice::<BlockMeta>(&bytes)
                                    .map_err(|e| e.to_string())
                            }) {
                            Ok(meta) => (b.ulid.clone(), Some(meta), None),
                            Err(e) => (b.ulid.clone(), None, Some(e)),
                        }
                    })
                    .collect())
            }
        }
    }

    /// Builds a migration plan from the object-store layout.
    pub fn plan(&self, cfg: &JobConfig) -> Result<Plan> {
        let tenants = self.list_tenants()?;
        let mut warnings = Vec::new();
        if tenants.is_empty() {
            return Err(Error::config(format!(
                "no Mimir tenants found under s3://{}/{}",
                self.cfg.bucket,
                self.cfg.normalized_prefix()
            )));
        }
        if !self.cfg.tenants.is_empty() {
            let found: Vec<&String> = tenants.iter().collect();
            for requested in &self.cfg.tenants {
                if !found.iter().any(|t| t.as_str() == requested) {
                    warnings.push(format!("requested tenant {requested:?} not found; ignored"));
                }
            }
        }

        let mut blocks = Vec::new();
        let mut skipped_by_time = 0usize;
        for tenant in &tenants {
            let summaries = self.scan_tenant(tenant)?;
            if summaries.is_empty() {
                warnings.push(format!("tenant {tenant:?}: no valid TSDB blocks"));
                continue;
            }
            let summary_by_ulid: BTreeMap<String, BlockSummary> = summaries
                .iter()
                .map(|b| (b.ulid.clone(), b.clone()))
                .collect();
            let metas = self.fetch_metas(tenant, &summaries)?;
            for (ulid, meta, err) in metas {
                let Some(mut meta) = meta else {
                    warnings.push(format!(
                        "tenant {tenant:?} block {ulid}: {}",
                        err.unwrap_or_else(|| "meta.json unreadable".into())
                    ));
                    continue;
                };
                // The directory name is the object-store source of truth; keep
                // it consistent with meta.ulid (some writers leave it empty).
                meta.ulid = ulid.clone();
                let Some(summary) = summary_by_ulid.get(&ulid) else {
                    continue;
                };
                if !cfg.blocks.is_empty()
                    && !cfg
                        .blocks
                        .iter()
                        .any(|x| x == &ulid || x == &format!("{tenant}/{ulid}"))
                {
                    continue;
                }
                if let Some(min) = cfg.filter.min_time {
                    if meta.max_time <= min {
                        skipped_by_time += 1;
                        continue;
                    }
                }
                if let Some(max) = cfg.filter.max_time {
                    if meta.min_time > max {
                        skipped_by_time += 1;
                        continue;
                    }
                }
                let stats_series = meta.stats.num_series;
                let stats_chunks = meta.stats.num_chunks;
                if stats_series == 0 || stats_chunks == 0 {
                    warnings.push(format!(
                        "tenant {tenant:?} block {ulid}: meta.json has no stats; progress totals will be derived from decoded samples"
                    ));
                }
                let info = BlockInfo {
                    path: self.cache_path(tenant, &ulid),
                    tenant: tenant.clone(),
                    remote: true,
                    index_bytes: summary.index_bytes,
                    chunks_bytes: summary.chunks_bytes,
                    tombstones_bytes: summary.tombstones_bytes,
                    warnings: Vec::new(),
                    meta: meta.clone(),
                };
                blocks.push(PlannedBlock {
                    info,
                    stats: tsdb_reader::IndexStats {
                        series: stats_series,
                        chunks: stats_chunks,
                        min_series_offset: 0,
                        end_series_offset: 0,
                    },
                    samples_expected: meta.samples_hint(),
                });
            }
        }

        blocks.sort_by(|a, b| {
            a.info
                .tenant
                .cmp(&b.info.tenant)
                .then_with(|| a.info.meta.min_time.cmp(&b.info.meta.min_time))
                .then_with(|| a.info.meta.ulid.cmp(&b.info.meta.ulid))
        });
        if blocks.is_empty() {
            return Err(Error::config(
                "no Mimir blocks matched the requested tenants / block list / time range",
            ));
        }
        Ok(Plan {
            blocks,
            skipped_by_time,
            warnings,
        })
    }

    /// Downloads a block into the local cache, streaming each object to disk.
    pub fn materialize(&self, tenant: &str, ulid: &str) -> Result<PathBuf> {
        if !is_valid_tenant(tenant) || !is_block_ulid(ulid) {
            return Err(Error::Other(format!(
                "mimir: invalid tenant/block ({tenant:?}, {ulid:?})"
            )));
        }
        let dir = self.cache_path(tenant, ulid);
        if dir.join(READY_MARKER).is_file() {
            return Ok(dir);
        }
        // A previous run may have been interrupted. Never wipe the cache dir:
        // completed objects are skipped by size and in-flight ones resume from
        // their `.tsdb-part` file (see `download_object`).
        std::fs::create_dir_all(dir.join("chunks"))?;
        let prefix = self.block_prefix(tenant, ulid);
        let objects = self.list_objects(&prefix)?;
        if objects.is_empty() {
            return Err(Error::Other(format!(
                "mimir: block {tenant}/{ulid} has no objects (prefix {prefix})"
            )));
        }
        let has = |name: &str| objects.iter().any(|o| o.key.ends_with(name));
        if !has("/meta.json")
            || !has("/index")
            || !objects.iter().any(|o| o.key.contains("/chunks/"))
        {
            return Err(Error::Other(format!(
                "mimir: block {tenant}/{ulid} is incomplete (meta/index/chunks required)"
            )));
        }

        let object_count = objects.len() as u64;
        let object_bytes = objects.iter().map(|o| o.size).sum::<u64>();
        let result = match &self.backend {
            Backend::Local(root) => {
                let mut out = Ok(());
                for obj in &objects {
                    let Some(rel) = obj.key.strip_prefix(&prefix) else {
                        continue;
                    };
                    let Some(target) = safe_join(&dir, rel) else {
                        out = Err(Error::Other(format!(
                            "mimir: unsafe object key {}",
                            obj.key
                        )));
                        break;
                    };
                    if let Some(parent) = target.parent() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            out = Err(e.into());
                            break;
                        }
                    }
                    // Skip objects this (or an earlier) run already copied.
                    if let Ok(md) = std::fs::metadata(&target) {
                        if md.is_file() && md.len() == obj.size {
                            continue;
                        }
                    }
                    if let Err(e) = std::fs::copy(root.join(&obj.key), &target) {
                        out = Err(e.into());
                        break;
                    }
                }
                out
            }
            Backend::S3(store) => {
                let rt = self.runtime.as_ref().expect("s3 runtime");
                let store = store.clone();
                let concurrency = self.cfg.max_concurrent_downloads.clamp(1, 64);
                let dir2 = dir.clone();
                let prefix2 = prefix.clone();
                let policy = DownloadPolicy {
                    part_bytes: self.cfg.download_part_bytes(),
                    retries: self.cfg.download_retries,
                };
                rt.block_on(async move {
                    let results = stream::iter(objects.into_iter().map(|obj| {
                        let store = store.clone();
                        let dir = dir2.clone();
                        let prefix = prefix2.clone();
                        async move {
                            let key = obj.key.clone();
                            download_object(&store, &obj, &prefix, &dir, policy)
                                .await
                                .map(|_| key)
                        }
                    }))
                    .buffer_unordered(concurrency)
                    .collect::<Vec<_>>()
                    .await;
                    let mut first = None;
                    for r in results {
                        if let Err(e) = r {
                            first.get_or_insert(e);
                        }
                    }
                    match first {
                        Some(e) => Err(e),
                        None => Ok::<(), Error>(()),
                    }
                })
            }
        };
        if let Err(e) = result {
            // Keep whatever made it to disk: the next run resumes from here.
            return Err(Error::Other(format!(
                "{e} (partial download kept in {}; rerun to resume)",
                dir.display()
            )));
        }
        let marker = serde_json::json!({
            "tenant": tenant,
            "ulid": ulid,
            "objects": object_count,
            "bytes": object_bytes,
            "downloaded_at": crate::stats::ms_to_rfc3339(chrono::Utc::now().timestamp_millis()),
        });
        std::fs::write(
            dir.join(READY_MARKER),
            serde_json::to_vec_pretty(&marker).map_err(|e| Error::Other(e.to_string()))?,
        )?;
        Ok(dir)
    }

    /// Removes a downloaded block from the cache (unless `keep_cache`).
    pub fn cleanup(&self, tenant: &str, ulid: &str) -> Result<()> {
        if self.cfg.keep_cache {
            return Ok(());
        }
        let dir = self.cache_path(tenant, ulid);
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for MimirSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MimirSource")
            .field("bucket", &self.cfg.bucket)
            .field("prefix", &self.cfg.prefix)
            .field("backend", &self.cfg.backend)
            .field("cache", &self.cache_root)
            .finish()
    }
}

fn is_block_ulid(s: &str) -> bool {
    s.len() == 26
        && s.chars()
            .all(|c| c.is_ascii_digit() || ('A'..='Z').contains(&c))
}

fn safe_join(base: &Path, rel: &str) -> Option<PathBuf> {
    let mut out = base.to_path_buf();
    for part in rel.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return None;
        }
        out.push(part);
    }
    if out.components().any(|c| matches!(c, Component::ParentDir)) {
        return None;
    }
    Some(out)
}

/// How an object is pulled out of the bucket.
#[derive(Debug, Clone, Copy)]
struct DownloadPolicy {
    /// Ranged-GET part size in bytes.
    part_bytes: u64,
    /// Consecutive failures without progress before giving up on the object.
    retries: usize,
}

/// Path of the in-flight download of `target`.
fn part_path(target: &Path) -> PathBuf {
    let name = match target.file_name() {
        Some(n) => format!("{}{}", n.to_string_lossy(), PART_SUFFIX),
        None => format!("object{}", PART_SUFFIX),
    };
    target.with_file_name(name)
}

/// Downloads one object into the block cache.
///
/// The object is fetched with ranged GETs of `policy.part_bytes` and appended to
/// `<name>.tsdb-part`. Any interruption (network reset, request stall, short
/// body, ...) is resumed from the byte offset already on disk, so a 20 GiB
/// `index` makes progress across attempts instead of restarting. Only when an
/// attempt makes no progress at all does the retry counter move; on success the
/// part file is renamed into place, so a complete file is never partially
/// visible.
async fn download_object(
    store: &AmazonS3,
    obj: &ObjectEntry,
    prefix: &str,
    dir: &Path,
    policy: DownloadPolicy,
) -> Result<()> {
    let key = obj.key.as_str();
    let size = obj.size;
    let Some(rel) = key.strip_prefix(prefix) else {
        return Ok(());
    };
    let Some(target) = safe_join(dir, rel) else {
        return Err(Error::Other(format!("mimir: unsafe object key {key}")));
    };
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::Other(format!("mimir: create cache dir: {e}")))?;
    }
    // Already complete (earlier run, or a previous run that finished after the
    // ready marker was checked).
    if let Ok(md) = tokio::fs::metadata(&target).await {
        if md.is_file() && md.len() == size {
            return Ok(());
        }
    }
    let part = part_path(&target);
    if size == 0 {
        tokio::fs::write(&target, b"")
            .await
            .map_err(|e| Error::Other(format!("mimir: create {}: {e}", target.display())))?;
        let _ = tokio::fs::remove_file(&part).await;
        return Ok(());
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&part)
        .await
        .map_err(|e| Error::Other(format!("mimir: open {}: {e}", part.display())))?;
    let mut done = file
        .metadata()
        .await
        .map_err(|e| Error::Other(format!("mimir: stat {}: {e}", part.display())))?
        .len();
    if done > size {
        // Stale part from a different object version: start over.
        file.set_len(0)
            .await
            .map_err(|e| Error::Other(format!("mimir: truncate {}: {e}", part.display())))?;
        done = 0;
    }

    let mut failures = 0usize;
    while done < size {
        let end = done.saturating_add(policy.part_bytes).min(size);
        let window = end - done;
        let outcome = fetch_range(store, key, &mut file, done, end).await;
        let (progress, err) = match outcome {
            Ok(n) => (n, None),
            Err((n, msg)) => (n, Some(msg)),
        };
        done += progress;
        if progress > 0 {
            // The transfer is making progress; only stalls count against us.
            failures = 0;
            // A peer that keeps truncating to a few bytes per request makes
            // progress too, so it must not be failed — but it also must not
            // spin at full request rate.
            if progress * 8 < window {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } else if let Some(msg) = err {
            // A store that answers a ranged GET with the whole object cannot be
            // salvaged by retrying: fail with the actual cause instead of
            // backing off for minutes.
            if msg.contains("non-partial response") {
                return Err(Error::Other(format!(
                    "mimir: download {key}: {msg} (the S3 endpoint ignores Range requests, \
                     which the resumable downloader requires)"
                )));
            }
            failures += 1;
            if failures > policy.retries {
                return Err(Error::Other(format!(
                    "mimir: download {key} failed after {failures} attempts: {msg} \
                     ({} of {} bytes kept)",
                    done, size
                )));
            }
            tokio::time::sleep(retry_delay(failures)).await;
        }
    }
    file.flush()
        .await
        .map_err(|e| Error::Other(format!("mimir: flush {}: {e}", part.display())))?;
    // Close before renaming: Windows refuses to move a file with open handles,
    // and the final name must never expose a half-written object.
    drop(file);
    let _ = tokio::fs::remove_file(&target).await;
    tokio::fs::rename(&part, &target)
        .await
        .map_err(|e| Error::Other(format!("mimir: finalize {}: {e}", target.display())))?;
    Ok(())
}

/// Fetches `start..end` of `key` and appends it to `file`.
///
/// Returns the number of bytes appended. On failure the bytes written before the
/// error are returned too (the file is truncated back to that length first), so
/// the caller can resume from there rather than restarting the object.
async fn fetch_range(
    store: &AmazonS3,
    key: &str,
    file: &mut tokio::fs::File,
    start: u64,
    end: u64,
) -> std::result::Result<u64, (u64, String)> {
    let want = end - start;
    let mut written = 0u64;
    let result = store
        .get_opts(
            &ObjPath::from(key.to_string()),
            GetOptions {
                range: Some(GetRange::Bounded(start..end)),
                ..Default::default()
            },
        )
        .await;
    let result = match result {
        Ok(r) => r,
        Err(e) => return Err((0, format!("get bytes {start}..{end}: {e}"))),
    };
    let mut stream = result.into_stream();
    while written < want {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                truncate_to(file, start + written).await;
                return Err((
                    written,
                    format!("body stream failed at byte {}: {e}", start + written),
                ));
            }
        };
        let take = (want - written).min(chunk.len() as u64) as usize;
        if let Err(e) = file.write_all(&chunk[..take]).await {
            let msg = format!("write at byte {}: {e}", start + written);
            truncate_to(file, start + written).await;
            return Err((written, msg));
        }
        written += take as u64;
    }
    if written < want {
        truncate_to(file, start + written).await;
        return Err((
            written,
            format!("short body: got {} of {} bytes", written, want),
        ));
    }
    if let Err(e) = file.flush().await {
        truncate_to(file, start + written).await;
        return Err((written, format!("flush: {e}")));
    }
    Ok(written)
}

/// Cuts the part file back to the last confirmed byte offset, so a failed write
/// cannot leave stray bytes that would corrupt the resumed object.
async fn truncate_to(file: &mut tokio::fs::File, len: u64) {
    let _ = file.flush().await;
    if let Ok(md) = file.metadata().await {
        if md.len() > len {
            let _ = file.set_len(len).await;
        }
    }
}

fn retry_delay(failures: usize) -> Duration {
    let shift = failures.min(7) as u32;
    Duration::from_millis((RETRY_BASE_MS << shift).min(RETRY_MAX_MS))
}

fn group_object(prefix: &str, blocks: &mut BTreeMap<String, BlockSummary>, key: &str, size: u64) {
    let Some(rel) = key.strip_prefix(prefix) else {
        return;
    };
    let parts: Vec<&str> = rel.split('/').collect();
    if parts.len() < 2 {
        return;
    }
    let ulid = parts[0];
    if !is_block_ulid(ulid) {
        return;
    }
    let entry = blocks.entry(ulid.to_string()).or_default();
    entry.ulid = ulid.to_string();
    entry.objects += 1;
    entry.bytes += size;
    match parts[1] {
        "meta.json" => entry.has_meta = true,
        "index" => {
            entry.has_index = true;
            entry.index_bytes += size;
        }
        "tombstones" => entry.tombstones_bytes += size,
        "chunks" => {
            entry.chunk_objects += 1;
            entry.chunks_bytes += size;
        }
        "deletion-mark.json" => entry.deletion_mark = true,
        _ => {}
    }
}

fn walk_summaries(
    bucket_root: &Path,
    dir: &Path,
    prefix: &str,
    blocks: &mut BTreeMap<String, BlockSummary>,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            continue;
        }
        let path = entry.path();
        if ft.is_dir() {
            walk_summaries(bucket_root, &path, prefix, blocks)?;
        } else if ft.is_file() {
            let rel = path
                .strip_prefix(bucket_root)
                .map_err(|e| Error::Other(e.to_string()))?;
            let key = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect::<Vec<_>>()
                .join("/");
            group_object(prefix, blocks, &key, entry.metadata()?.len());
        }
    }
    Ok(())
}

fn walk_files(bucket_root: &Path, dir: &Path, out: &mut Vec<ObjectEntry>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            continue;
        }
        let path = entry.path();
        if ft.is_dir() {
            walk_files(bucket_root, &path, out)?;
        } else if ft.is_file() {
            let rel = path
                .strip_prefix(bucket_root)
                .map_err(|e| Error::Other(e.to_string()))?;
            let key = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect::<Vec<_>>()
                .join("/");
            out.push(ObjectEntry {
                key,
                size: entry.metadata()?.len(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineOptions, FilterConfig, SinkConfig};

    fn fixture_block() -> PathBuf {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gen/data/block");
        let gt = std::fs::read(root.join("ground_truth.json")).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&gt).unwrap();
        root.join(v["ulid"].as_str().unwrap())
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for e in std::fs::read_dir(src).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            if p.is_dir() {
                copy_dir(&p, &dst.join(e.file_name()));
            } else {
                std::fs::copy(&p, dst.join(e.file_name())).unwrap();
            }
        }
    }

    fn temp_root(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "tsdb-mimir-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn discovers_tenants_and_materialises_blocks() {
        let bucket = temp_root("bucket");
        let src = fixture_block();
        let ulid = src.file_name().unwrap().to_str().unwrap().to_string();
        for tenant in ["tenant-a", "tenant-b"] {
            copy_dir(&src, &bucket.join(tenant).join(&ulid));
        }
        let out = temp_root("out");
        let cfg = MimirConfig {
            backend: MimirBackend::Local,
            bucket: bucket.display().to_string(),
            tenants: vec![],
            ..Default::default()
        };
        let source = MimirSource::new(&cfg, &out).unwrap();
        let tenants = source.list_tenants().unwrap();
        assert_eq!(tenants, vec!["tenant-a", "tenant-b"]);
        let infos = source.list_tenant_infos().unwrap();
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].blocks, 1);

        let job = JobConfig {
            name: "t".into(),
            data_dir: PathBuf::new(),
            output_dir: out.clone(),
            mimir: Some(cfg.clone()),
            blocks: vec![],
            filter: FilterConfig::default(),
            sink: SinkConfig::default(),
            options: EngineOptions::default(),
        };
        let plan = source.plan(&job).unwrap();
        assert_eq!(plan.blocks.len(), 2);
        assert_eq!(plan.blocks[0].info.tenant, "tenant-a");
        assert_eq!(plan.blocks[1].info.tenant, "tenant-b");
        assert_ne!(plan.blocks[0].info.key(), plan.blocks[1].info.key());

        let local = source.materialize("tenant-a", &ulid).unwrap();
        assert!(local.join("meta.json").is_file());
        assert!(local.join("index").is_file());
        assert!(local.join("chunks").read_dir().unwrap().next().is_some());
        let reader =
            tsdb_reader::BlockReader::open(&local, tsdb_reader::ReaderOptions::default()).unwrap();
        assert_eq!(reader.scan_stats().unwrap().series, 42);
        source.cleanup("tenant-a", &ulid).unwrap();
        assert!(!local.exists());
        std::fs::remove_dir_all(&bucket).unwrap();
        std::fs::remove_dir_all(&out).unwrap();
    }

    fn local_bucket(tag: &str) -> (PathBuf, String, MimirSource, PathBuf) {
        let bucket = temp_root(&format!("bucket-{tag}"));
        let src = fixture_block();
        let ulid = src.file_name().unwrap().to_str().unwrap().to_string();
        copy_dir(&src, &bucket.join("tenant-a").join(&ulid));
        let out = temp_root(&format!("out-{tag}"));
        let cfg = MimirConfig {
            backend: MimirBackend::Local,
            bucket: bucket.display().to_string(),
            tenants: vec![],
            ..Default::default()
        };
        let source = MimirSource::new(&cfg, &out).unwrap();
        (bucket, ulid, source, out)
    }

    #[test]
    fn part_file_lives_next_to_its_object() {
        let target = Path::new("/cache/t/blk/index");
        assert_eq!(
            part_path(target),
            PathBuf::from("/cache/t/blk/index.tsdb-part")
        );
        let chunk = Path::new("/cache/t/blk/chunks/000001");
        let part = part_path(chunk);
        assert_eq!(part, PathBuf::from("/cache/t/blk/chunks/000001.tsdb-part"));
        // The reader only maps files whose name is a decimal sequence number, so
        // an in-flight part can never be mistaken for a chunk segment.
        let name = part.file_name().unwrap().to_str().unwrap();
        assert!(name.parse::<u64>().is_err());
    }

    #[test]
    fn resumes_a_truncated_cache_without_wiping_it() {
        let (bucket, ulid, source, out) = local_bucket("resume");
        let dir = source.materialize("tenant-a", &ulid).unwrap();
        let index = dir.join("index");
        let full = std::fs::metadata(&index).unwrap().len();
        assert!(full > 8);

        // Simulate an interrupted run: half-written object, no ready marker,
        // and the data of other objects already in place.
        let kept = std::fs::metadata(dir.join("meta.json")).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&index)
            .unwrap()
            .set_len(full / 2)
            .unwrap();
        std::fs::remove_file(dir.join(READY_MARKER)).unwrap();
        std::fs::write(part_path(&index), vec![0u8; 32]).unwrap();

        let again = source.materialize("tenant-a", &ulid).unwrap();
        assert_eq!(again, dir);
        assert_eq!(std::fs::metadata(&index).unwrap().len(), full);
        assert_eq!(
            std::fs::metadata(dir.join("meta.json")).unwrap().len(),
            kept
        );
        assert!(dir.join(READY_MARKER).is_file());
        let reader =
            tsdb_reader::BlockReader::open(&again, tsdb_reader::ReaderOptions::default()).unwrap();
        assert_eq!(reader.scan_stats().unwrap().series, 42);

        std::fs::remove_dir_all(&bucket).unwrap();
        std::fs::remove_dir_all(&out).unwrap();
    }

    #[test]
    fn keeps_the_cache_dir_when_a_block_fails() {
        let (bucket, ulid, source, out) = local_bucket("keep");
        let dir = source.materialize("tenant-a", &ulid).unwrap();
        std::fs::remove_file(dir.join(READY_MARKER)).unwrap();
        // Make one object impossible to write: the destination is a directory,
        // so the copy fails half-way through the block.
        let chunk = dir.join("chunks").join("000001");
        std::fs::remove_file(&chunk).unwrap();
        std::fs::create_dir(&chunk).unwrap();

        let err = source.materialize("tenant-a", &ulid).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("partial download kept"), "{msg}");

        // Everything already transferred is still there for the next attempt.
        assert!(dir.join("index").is_file());
        assert!(dir.join("meta.json").is_file());
        assert!(!dir.join(READY_MARKER).exists());

        std::fs::remove_dir_all(&bucket).unwrap();
        std::fs::remove_dir_all(&out).unwrap();
    }
}

/// End-to-end tests of the part-and-resume downloader against a tiny mock S3.
///
/// The mock answers `GET` with `206 Partial Content` for ranged requests and can
/// misbehave on demand: cut a response mid-body, announce fewer bytes than the
/// requested range, or drop the connection without answering.
#[cfg(test)]
mod s3_download_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Mode {
        /// Serve the whole requested range.
        Ok,
        /// Announce the full range, then answer with 40% of it (cut connection).
        Cut,
        /// Announce only 40% of the range (clean but short body).
        Short,
        /// Accept and close without a response.
        Dead,
        /// Trickle the range out in small pieces with pauses between them.
        Slow,
    }

    #[derive(Default)]
    struct MockState {
        modes: VecDeque<Mode>,
        hits: Vec<(u64, u64)>,
    }

    fn serve(mut sock: TcpStream, payload: &[u8], state: &Arc<Mutex<MockState>>) {
        sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            match sock.read(&mut chunk) {
                Ok(0) => return,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
            if buf.len() > 64 * 1024 {
                return;
            }
        }
        let head = String::from_utf8_lossy(&buf).to_string();
        let mut range = None;
        for line in head.split("\r\n") {
            if let Some(rest) = line
                .to_ascii_lowercase()
                .strip_prefix("range:")
                .map(str::to_owned)
            {
                if let Some(spec) = rest.trim().strip_prefix("bytes=") {
                    let mut it = spec.split('-');
                    let a = it.next().and_then(|s| s.trim().parse::<u64>().ok());
                    let b = it.next().and_then(|s| s.trim().parse::<u64>().ok());
                    if let (Some(a), Some(b)) = (a, b) {
                        range = Some((a, b));
                    }
                }
            }
        }
        let mode = {
            let mut st = state.lock().unwrap();
            st.modes.pop_front().unwrap_or(Mode::Ok)
        };
        if mode == Mode::Dead {
            let _ = sock.shutdown(Shutdown::Both);
            return;
        }
        let total = payload.len() as u64;
        let (start, end) = match range {
            Some((a, b)) => (a, b.min(total - 1)),
            None => (0, total - 1),
        };
        state.lock().unwrap().hits.push((start, end));
        let slice = &payload[start as usize..=end as usize];
        let announced = slice.len();
        let sent = match mode {
            Mode::Cut | Mode::Short => announced * 2 / 5,
            _ => announced,
        };
        // "Cut" promises the full range but sends less (peer vanished). "Short"
        // honestly announces the truncated length, so the body ends cleanly.
        let content_length = match mode {
            Mode::Cut => announced,
            _ => sent,
        };
        // No ETag on purpose: without one object_store does not retry the body
        // stream itself, so the assertions below exercise *our* resume loop.
        let head = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {content_length}\r\n\
             Content-Range: bytes {start}-{end}/{total}\r\nConnection: close\r\n\r\n"
        );
        if sock.write_all(head.as_bytes()).is_err() {
            return;
        }
        let body = &slice[..sent];
        match mode {
            Mode::Slow => {
                for piece in body.chunks(32 * 1024) {
                    if sock.write_all(piece).is_err() {
                        return;
                    }
                    let _ = sock.flush();
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            _ => {
                let _ = sock.write_all(body);
            }
        }
        let _ = sock.flush();
        let _ = sock.shutdown(Shutdown::Both);
    }

    fn mock_server(payload: Vec<u8>, state: Arc<Mutex<MockState>>) -> (String, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let payload = Arc::new(payload);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        std::thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((sock, _)) => {
                        let payload = payload.clone();
                        let state = state.clone();
                        std::thread::spawn(move || serve(sock, &payload, &state));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        (format!("http://{addr}"), stop)
    }

    fn client(endpoint: &str) -> AmazonS3 {
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_skip_signature(true)
            .with_retry(RetryConfig {
                max_retries: 1,
                retry_timeout: Duration::from_secs(10),
                ..Default::default()
            })
            .build()
            .unwrap()
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    fn scratch(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "tsdb-s3-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn object(size: u64) -> ObjectEntry {
        ObjectEntry {
            key: "tenant/blk/index".to_string(),
            size,
        }
    }

    #[test]
    fn resumes_truncated_parts_until_the_object_is_complete() {
        const PART: u64 = 1024 * 1024;
        let payload: Vec<u8> = (0..3 * PART).map(|i| (i % 251) as u8).collect();
        let state = Arc::new(Mutex::new(MockState::default()));
        state.lock().unwrap().modes = VecDeque::from([Mode::Cut, Mode::Short]);
        let (endpoint, stop) = mock_server(payload.clone(), state.clone());
        let store = client(&endpoint);
        let dir = scratch("resume");
        let obj = object(payload.len() as u64);
        let policy = DownloadPolicy {
            part_bytes: PART,
            retries: 5,
        };
        rt().block_on(download_object(&store, &obj, "tenant/blk/", &dir, policy))
            .unwrap();

        assert_eq!(
            std::fs::read(dir.join("index")).unwrap(),
            payload.as_slice()
        );
        assert!(
            !dir.join("index.tsdb-part").exists(),
            "part must be renamed"
        );

        let hits = state.lock().unwrap().hits.clone();
        // 3 parts + one resume for the cut body + one for the short body.
        assert_eq!(hits.len(), 5, "requests: {hits:?}");
        assert_eq!(hits[0], (0, PART - 1));
        assert!(hits[1].0 > 0 && hits[1].0 < PART, "resumed at: {hits:?}");
        assert!(hits[2].0 > hits[1].0, "kept moving: {hits:?}");

        // A finished object is never re-fetched.
        rt().block_on(download_object(&store, &obj, "tenant/blk/", &dir, policy))
            .unwrap();
        assert_eq!(state.lock().unwrap().hits.len(), 5);

        stop.store(true, Ordering::Relaxed);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn client_with_timeout(endpoint: &str, timeout: Option<Duration>) -> AmazonS3 {
        // `with_client_options` replaces the whole options struct, so the
        // plain-HTTP switch has to live inside it.
        let options = match timeout {
            Some(t) => ClientOptions::default().with_timeout(t),
            None => ClientOptions::default().with_timeout_disabled(),
        }
        .with_allow_http(true);
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_skip_signature(true)
            .with_client_options(options)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap()
    }

    /// Reproduces the production failure and shows the fix.
    ///
    /// object_store's default `ClientOptions::timeout` is 30 s and covers the
    /// whole response body, so a slow body read fails with
    /// "Generic S3 error: HTTP error: request or response body error" — exactly
    /// the error seen on the 20 GiB `index`. With the deadline disabled the same
    /// server completes.
    #[test]
    fn a_request_deadline_is_what_breaks_long_body_reads() {
        const SIZE: usize = 512 * 1024;
        let payload: Vec<u8> = (0..SIZE).map(|i| (i % 13) as u8).collect();
        let state = Arc::new(Mutex::new(MockState::default()));
        // 32 KiB every 100 ms: ~1.6 s for the whole body.
        state.lock().unwrap().modes = VecDeque::from([Mode::Slow; 4]);
        let (endpoint, stop) = mock_server(payload.clone(), state.clone());
        let dir = scratch("deadline");
        let obj = object(SIZE as u64);
        let policy = DownloadPolicy {
            part_bytes: SIZE as u64,
            retries: 5,
        };

        // A 1 s deadline (the 30 s default, shrunk) kills the body mid-stream.
        let timed = client_with_timeout(&endpoint, Some(Duration::from_secs(1)));
        let msg = rt()
            .block_on(async {
                let result = timed.get(&ObjPath::from("tenant/blk/index")).await?;
                let mut stream = result.into_stream();
                while let Some(chunk) = stream.next().await {
                    chunk?;
                }
                Ok::<(), object_store::Error>(())
            })
            .unwrap_err()
            .to_string();
        assert!(msg.contains("request or response body error"), "{msg}");

        // Deadline disabled (our default): the same server completes.
        let fixed = client_with_timeout(&endpoint, None);
        rt().block_on(download_object(&fixed, &obj, "tenant/blk/", &dir, policy))
            .unwrap();
        assert_eq!(
            std::fs::read(dir.join("index")).unwrap(),
            payload.as_slice()
        );

        // And even with a deadline, the part loop grinds through it: the bytes
        // already received are kept and the next request resumes from there.
        let dir2 = scratch("deadline-resume");
        rt().block_on(download_object(&timed, &obj, "tenant/blk/", &dir2, policy))
            .unwrap();
        assert_eq!(
            std::fs::read(dir2.join("index")).unwrap(),
            payload.as_slice()
        );

        stop.store(true, Ordering::Relaxed);
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dir2).ok();
    }

    #[test]
    fn a_failed_object_keeps_its_part_and_resumes_on_the_next_run() {
        const PART: u64 = 512 * 1024;
        let payload: Vec<u8> = (0..(4 * PART + 1234)).map(|i| (i % 97) as u8).collect();
        let state = Arc::new(Mutex::new(MockState::default()));
        // Two good parts land on disk, then the connection dies for good.
        state.lock().unwrap().modes = VecDeque::from([
            Mode::Ok,
            Mode::Ok,
            Mode::Dead,
            Mode::Dead,
            Mode::Dead,
            Mode::Dead,
        ]);
        let (endpoint, stop) = mock_server(payload.clone(), state.clone());
        let store = client(&endpoint);
        let dir = scratch("partial");
        let obj = object(payload.len() as u64);
        let policy = DownloadPolicy {
            part_bytes: PART,
            retries: 1,
        };

        // Two good parts, then the connection dies twice in a row.
        let err = rt()
            .block_on(download_object(&store, &obj, "tenant/blk/", &dir, policy))
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed after"), "{msg}");
        assert!(
            !dir.join("index").exists(),
            "must not expose a partial object"
        );
        let kept = std::fs::metadata(dir.join("index.tsdb-part"))
            .unwrap()
            .len();
        assert_eq!(kept, 2 * PART);

        // Next run continues exactly where the failed one stopped.
        state.lock().unwrap().modes.clear();
        let before = state.lock().unwrap().hits.len();
        rt().block_on(download_object(&store, &obj, "tenant/blk/", &dir, policy))
            .unwrap();
        let hits = state.lock().unwrap().hits.clone();
        assert_eq!(hits[before].0, kept, "resume offset: {hits:?}");
        assert_eq!(
            std::fs::read(dir.join("index")).unwrap(),
            payload.as_slice()
        );
        assert!(!dir.join("index.tsdb-part").exists());

        stop.store(true, Ordering::Relaxed);
        std::fs::remove_dir_all(&dir).ok();
    }
}
