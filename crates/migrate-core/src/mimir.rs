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

use futures::stream::{self, StreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::io::AsyncWriteExt;

use tsdb_reader::{BlockInfo, BlockMeta};

use crate::config::{is_valid_tenant, JobConfig, MimirBackend, MimirConfig};
use crate::error::{Error, Result};
use crate::planner::{Plan, PlannedBlock};

const READY_MARKER: &str = ".tsdb-migrate-ready";
const META_CONCURRENCY: usize = 16;

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
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(&mimir.bucket)
                    .with_region(&mimir.region);
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
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(dir.join("chunks"))?;
        let prefix = self.block_prefix(tenant, ulid);
        let objects = self.list_objects(&prefix)?;
        if objects.is_empty() {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(Error::Other(format!(
                "mimir: block {tenant}/{ulid} has no objects (prefix {prefix})"
            )));
        }
        let has = |name: &str| objects.iter().any(|o| o.key.ends_with(name));
        if !has("/meta.json")
            || !has("/index")
            || !objects.iter().any(|o| o.key.contains("/chunks/"))
        {
            let _ = std::fs::remove_dir_all(&dir);
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
                rt.block_on(async move {
                    let results = stream::iter(objects.into_iter().map(|obj| {
                        let store = store.clone();
                        let dir = dir2.clone();
                        let prefix = prefix2.clone();
                        async move { download_object(&store, &obj.key, &prefix, &dir).await }
                    }))
                    .buffer_unordered(concurrency)
                    .collect::<Vec<_>>()
                    .await;
                    for r in results {
                        r?;
                    }
                    Ok::<(), Error>(())
                })
            }
        };
        if let Err(e) = result {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(e);
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

async fn download_object(store: &AmazonS3, key: &str, prefix: &str, dir: &Path) -> Result<()> {
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
    let result = store
        .get(&ObjPath::from(key.to_string()))
        .await
        .map_err(|e| Error::Other(format!("mimir: get {key}: {e}")))?;
    let mut file = tokio::fs::File::create(&target)
        .await
        .map_err(|e| Error::Other(format!("mimir: create {}: {e}", target.display())))?;
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| Error::Other(format!("mimir: download {key}: {e}")))?;
        file.write_all(&chunk)
            .await
            .map_err(|e| Error::Other(format!("mimir: write {}: {e}", target.display())))?;
    }
    file.flush()
        .await
        .map_err(|e| Error::Other(format!("mimir: flush {}: {e}", target.display())))?;
    Ok(())
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
}
