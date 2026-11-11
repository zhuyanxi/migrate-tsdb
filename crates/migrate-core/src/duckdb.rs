//! DuckDB integration.
//!
//! The migration itself still writes Parquet; this module is the final DuckDB
//! shaping step. It scans the exported `series`/`histograms` Parquet files to
//! learn the label set of every metric, then emits one DuckDB table per metric:
//!
//! ```sql
//! CREATE TABLE "http_requests_total" AS
//! SELECT epoch_ms(s.ts) AS "timestamp",
//!        s.value        AS "value",
//!        x.labels['job'] AS "job"
//! FROM read_parquet('.../samples/*.parquet', hive_partitioning=true) s
//! JOIN read_parquet('.../series/*.parquet', hive_partitioning=true) x
//!   ON s.series_uid=x.series_uid AND s.block=x.block
//! WHERE x.labels['__name__'] = 'http_requests_total';
//! ```
//!
//! A Prometheus export gets one DuckDB file; a Mimir export gets one file per
//! tenant (`<out>/duckdb/<tenant>.duckdb`).
//!
//! The Parquet layout, reader, pipeline and checkpoint logic are untouched.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use arrow_array::{Array, MapArray, StringArray, UInt64Array};
use md5::{Digest, Md5};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::config::is_valid_tenant;
use crate::error::{Error, Result};

pub const LOAD_SQL_FILE: &str = "load_duckdb.sql";
const RESERVED_COLUMNS: &[&str] = &[
    "timestamp",
    "value",
    "count",
    "zero_count",
    "sum",
    "schema",
    "zero_threshold",
    "pos_spans",
    "neg_spans",
    "pos_buckets",
    "neg_buckets",
    "custom_values",
    "reset_hint",
    "st",
];
const HIST_COLUMNS: &[&str] = &[
    "count",
    "zero_count",
    "sum",
    "schema",
    "zero_threshold",
    "pos_spans",
    "neg_spans",
    "pos_buckets",
    "neg_buckets",
    "custom_values",
    "reset_hint",
    "st",
];

#[derive(Debug, Clone)]
pub struct DuckDbPlan {
    /// `None` for a Prometheus/local export, `Some(tenant)` for Mimir.
    pub tenant: Option<String>,
    pub sql_path: PathBuf,
    pub db_path: PathBuf,
    /// Intermediate metric-partitioned Parquet used to build the wide tables.
    pub stage_dir: PathBuf,
    pub metrics: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Default)]
struct MetricSchema {
    labels: BTreeSet<String>,
    has_float: bool,
    has_hist: bool,
}

struct LabelColumn {
    key: String,
    column: String,
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Builds the final DuckDB plans (one per tenant/database) and writes SQL files.
/// `configured_db` is empty for defaults, a file for a Prometheus export, or a
/// directory for a multi-tenant Mimir export.
pub fn plan(out_dir: &Path, configured_db: &str) -> Result<Vec<DuckDbPlan>> {
    let tenants = output_tenants(out_dir);
    let mut plans = Vec::new();
    if tenants.is_empty() {
        let db_path = resolve_db_paths(out_dir, configured_db, None)?;
        let stage_dir = stage_dir(out_dir, None);
        let (sql_path, metrics, warnings) = build_sql(out_dir, None, &stage_dir)?;
        plans.push(DuckDbPlan {
            tenant: None,
            sql_path,
            db_path,
            stage_dir,
            metrics,
            warnings,
        });
    } else {
        for tenant in tenants {
            let db_path = resolve_db_paths(out_dir, configured_db, Some(&tenant))?;
            let stage_dir = stage_dir(out_dir, Some(&tenant));
            let (sql_path, metrics, warnings) = build_sql(out_dir, Some(&tenant), &stage_dir)?;
            plans.push(DuckDbPlan {
                tenant: Some(tenant),
                sql_path,
                db_path,
                stage_dir,
                metrics,
                warnings,
            });
        }
    }
    Ok(plans)
}

/// Convenience used after a completed run when no custom DB path is needed.
pub fn write_sqls(out_dir: &Path) -> Result<Vec<DuckDbPlan>> {
    plan(out_dir, "")
}

fn output_tenants(out_dir: &Path) -> Vec<String> {
    let dir = out_dir.join("blocks");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for e in rd.flatten() {
        if !e.path().is_dir() {
            continue;
        }
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(t) = name.strip_prefix("tenant=") {
            if is_valid_tenant(t) {
                out.push(t.to_string());
            }
        }
    }
    out.sort();
    out
}

fn resolve_db_paths(out_dir: &Path, configured: &str, tenant: Option<&str>) -> Result<PathBuf> {
    let configured = configured.trim();
    match tenant {
        None => {
            if configured.is_empty() {
                Ok(out_dir.join("migration.duckdb"))
            } else {
                let p = PathBuf::from(configured);
                if p.extension().map(|e| e == "duckdb").unwrap_or(false) {
                    Ok(p)
                } else {
                    Ok(p.join("migration.duckdb"))
                }
            }
        }
        Some(t) => {
            let dir = if configured.is_empty() {
                out_dir.join("duckdb")
            } else {
                let p = PathBuf::from(configured);
                if p.extension().map(|e| e == "duckdb").unwrap_or(false) {
                    // A single file only makes sense for a single tenant.
                    if output_tenants(out_dir).len() > 1 {
                        return Err(Error::sink(
                            "sink.duckdb_path must be a directory when migrating multiple Mimir tenants",
                        ));
                    }
                    return Ok(p);
                } else {
                    p
                }
            };
            Ok(dir.join(format!("{t}.duckdb")))
        }
    }
}

fn sql_path(out_dir: &Path, tenant: Option<&str>) -> PathBuf {
    match tenant {
        None => out_dir.join(LOAD_SQL_FILE),
        Some(t) => out_dir.join(format!("load_duckdb_{t}.sql")),
    }
}

fn stage_dir(out_dir: &Path, tenant: Option<&str>) -> PathBuf {
    out_dir
        .join(".duckdb_stage")
        .join(tenant.unwrap_or("local"))
}

fn metric_key(metric: &str) -> String {
    let digest = Md5::digest(metric.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn build_sql(
    out_dir: &Path,
    tenant: Option<&str>,
    stage: &Path,
) -> Result<(PathBuf, usize, Vec<String>)> {
    let metrics = scan_metrics(out_dir, tenant)?;
    // DuckDB's COPY ... PARTITION_BY does not create the final parent
    // directory, so prepare an empty staging directory up front.
    if stage.exists() {
        std::fs::remove_dir_all(stage)?;
    }
    std::fs::create_dir_all(stage)?;
    let stage = stage.canonicalize().unwrap_or_else(|_| stage.to_path_buf());
    let root = out_dir
        .canonicalize()
        .unwrap_or_else(|_| out_dir.to_path_buf());
    let root = sql_escape(&root.display().to_string());
    let stage_s = sql_escape(&stage.display().to_string());
    let mut sql = String::new();
    sql.push_str("-- Generated by tsdb-migrate. Requires DuckDB >= 0.10.\n");
    match tenant {
        Some(t) => sql.push_str(&format!(
            "-- Source: Mimir tenant {t} - one table per metric, label columns + timestamp.\n\n"
        )),
        None => sql.push_str(
            "-- Source: Prometheus/local - one table per metric, label columns + timestamp.\n\n",
        ),
    }
    let mut warnings = Vec::new();
    if metrics.is_empty() {
        sql.push_str("-- No series were found; no metric tables created.\n");
    } else {
        let any_float = metrics.values().any(|m| m.has_float);
        let any_hist = metrics.values().any(|m| m.has_hist);
        let glob = |dataset: &str| match tenant {
            Some(t) => format!("{root}/blocks/tenant={t}/**/{dataset}/*.parquet"),
            None => format!("{root}/blocks/**/{dataset}/*.parquet"),
        };
        let float_select = any_float.then(|| {
            let mut cols = vec![
                "md5(x.labels['__name__']) AS metric_key".to_string(),
                "s.ts AS ts".to_string(),
                "s.value AS value".to_string(),
            ];
            if any_hist {
                for c in HIST_COLUMNS {
                    cols.push(format!("NULL AS {}", quote_ident(c)));
                }
            }
            cols.push("x.labels AS labels".to_string());
            format!(
                "SELECT\n    {}\nFROM read_parquet('{}', hive_partitioning = true) s\nJOIN read_parquet('{}', hive_partitioning = true) x\n  ON s.series_uid = x.series_uid AND s.block = x.block",
                cols.join(",\n    "),
                sql_escape(&glob("samples")),
                sql_escape(&glob("series"))
            )
        });
        let hist_select = any_hist.then(|| {
            let mut cols = vec![
                "md5(x.labels['__name__']) AS metric_key".to_string(),
                "h.ts AS ts".to_string(),
            ];
            if any_float {
                cols.push("NULL AS value".to_string());
            }
            for c in HIST_COLUMNS {
                cols.push(format!("h.{} AS {}", c, quote_ident(c)));
            }
            cols.push("x.labels AS labels".to_string());
            format!(
                "SELECT\n    {}\nFROM read_parquet('{}', hive_partitioning = true) h\nJOIN read_parquet('{}', hive_partitioning = true) x\n  ON h.series_uid = x.series_uid AND h.block = x.block",
                cols.join(",\n    "),
                sql_escape(&glob("histograms")),
                sql_escape(&glob("series"))
            )
        });
        let base = match (float_select, hist_select) {
            (Some(f), Some(h)) => format!("{f}\nUNION ALL\n{h}"),
            (Some(f), None) => f,
            (None, Some(h)) => h,
            (None, None) => String::new(),
        };
        sql.push_str("CREATE TEMP TABLE _tsdb_metric_long AS\n");
        sql.push_str(&base);
        sql.push_str(";\n\n");
        sql.push_str(&format!(
            "COPY _tsdb_metric_long TO '{}' (FORMAT PARQUET, PARTITION_BY (metric_key), OVERWRITE_OR_IGNORE);\n\n",
            stage_s
        ));
        sql.push_str("DROP TABLE _tsdb_metric_long;\n\n");
        for (metric, schema) in &metrics {
            let (labels, mut w) = label_columns(&schema.labels);
            warnings.append(&mut w);
            sql.push_str(&metric_sql(
                metric,
                schema,
                &labels,
                &format!("{stage_s}/metric_key={}/*.parquet", metric_key(metric)),
            ));
            sql.push('\n');
        }
    }
    if !warnings.is_empty() {
        let mut header = String::new();
        for w in &warnings {
            header.push_str(&format!("-- WARNING: {w}\n"));
        }
        header.push('\n');
        let pos = sql.find('\n').map(|i| i + 1).unwrap_or(0);
        sql.insert_str(pos, &header);
    }
    let path = sql_path(out_dir, tenant);
    std::fs::write(&path, sql)?;
    Ok((path, metrics.len(), warnings))
}

fn metric_sql(
    metric: &str,
    schema: &MetricSchema,
    labels: &[LabelColumn],
    stage_glob: &str,
) -> String {
    let table = quote_ident(metric);
    let mut cols = vec!["epoch_ms(ts) AS \"timestamp\"".to_string()];
    if schema.has_float {
        cols.push("value AS \"value\"".to_string());
    }
    if schema.has_hist {
        for c in HIST_COLUMNS {
            cols.push(format!("{} AS {}", c, quote_ident(c)));
        }
    }
    for l in labels {
        cols.push(format!(
            "labels['{}'] AS {}",
            sql_escape(&l.key),
            quote_ident(&l.column)
        ));
    }
    format!(
        "CREATE OR REPLACE TABLE {table} AS\nSELECT\n    {}\nFROM read_parquet('{}');\n",
        cols.join(",\n    "),
        sql_escape(stage_glob)
    )
}

fn label_columns(labels: &BTreeSet<String>) -> (Vec<LabelColumn>, Vec<String>) {
    let mut used: BTreeSet<String> = RESERVED_COLUMNS.iter().map(|s| s.to_string()).collect();
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    for key in labels {
        let mut column = key.clone();
        if used.contains(&column) {
            column = format!("label_{key}");
            warnings.push(format!(
                "label {key:?} conflicts with a reserved DuckDB column; exported as {column:?}"
            ));
        }
        while used.contains(&column) {
            column.push('_');
        }
        used.insert(column.clone());
        out.push(LabelColumn {
            key: key.clone(),
            column,
        });
    }
    (out, warnings)
}

fn scan_metrics(out_dir: &Path, tenant: Option<&str>) -> Result<BTreeMap<String, MetricSchema>> {
    let root = match tenant {
        Some(t) => out_dir.join("blocks").join(format!("tenant={t}")),
        None => out_dir.join("blocks"),
    };
    let hist_files = dataset_files(&root, "histograms")?;
    let series_files = dataset_files(&root, "series")?;
    if series_files.is_empty() {
        return Ok(BTreeMap::new());
    }
    let hist_uids = read_uids(&hist_files)?;
    let mut metrics: BTreeMap<String, MetricSchema> = BTreeMap::new();
    for path in series_files {
        let file = std::fs::File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::sink(format!("open {}: {e}", path.display())))?;
        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
        for batch in reader {
            let batch = batch.map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
            let Some(map) = batch
                .column_by_name("labels")
                .and_then(|c| c.as_any().downcast_ref::<MapArray>())
            else {
                continue;
            };
            let Some(uids) = batch
                .column_by_name("series_uid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            else {
                continue;
            };
            let keys = map
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::sink("labels map has non-string keys"))?;
            let values = map
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::sink("labels map has non-string values"))?;
            let offsets = map.value_offsets();
            for row in 0..map.len() {
                if map.is_null(row) || uids.is_null(row) {
                    continue;
                }
                let uid = uids.value(row);
                let mut metric: Option<String> = None;
                let mut labels = Vec::new();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                for idx in start..end {
                    let k = keys.value(idx);
                    let v = values.value(idx);
                    if k == "__name__" {
                        metric = Some(v.to_string());
                    } else {
                        labels.push(k.to_string());
                    }
                }
                let Some(metric) = metric else { continue };
                let entry = metrics.entry(metric).or_default();
                entry.labels.extend(labels);
                if hist_uids.contains(&uid) {
                    entry.has_hist = true;
                } else {
                    entry.has_float = true;
                }
            }
        }
    }
    Ok(metrics)
}

fn read_uids(files: &[PathBuf]) -> Result<HashSet<u64>> {
    let mut out = HashSet::new();
    for path in files {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::sink(format!("open {}: {e}", path.display())))?;
        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
        for batch in reader {
            let batch = batch.map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
            let Some(uid) = batch
                .column_by_name("series_uid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            else {
                continue;
            };
            for i in 0..uid.len() {
                if !uid.is_null(i) {
                    out.insert(uid.value(i));
                }
            }
        }
    }
    Ok(out)
}

fn dataset_files(root: &Path, dataset: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if root.is_dir() {
        collect_dataset(root, dataset, &mut out)?;
    }
    out.sort();
    Ok(out)
}

fn collect_dataset(dir: &Path, dataset: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            continue;
        }
        let path = entry.path();
        if ft.is_dir() {
            if entry.file_name().to_str() == Some(dataset) {
                for f in std::fs::read_dir(&path)? {
                    let f = f?;
                    if f.file_type()?.is_file()
                        && f.path()
                            .extension()
                            .map(|x| x == "parquet")
                            .unwrap_or(false)
                    {
                        out.push(f.path());
                    }
                }
            } else {
                collect_dataset(&path, dataset, out)?;
            }
        }
    }
    Ok(())
}

fn duckdb_available() -> bool {
    // `--version` must never wait on stdin: a wrapper script that reads stdin
    // would otherwise block the whole finalizer.
    std::process::Command::new("duckdb")
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Rebuilds `db_path` from `plan.sql_path` using the DuckDB CLI.
pub fn execute(plan: &DuckDbPlan) -> Result<()> {
    if !duckdb_available() {
        return Err(Error::sink(format!(
            "duckdb CLI not found in PATH; {} was generated, run it manually",
            plan.sql_path.display()
        )));
    }
    if let Some(parent) = plan.db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if plan.db_path.exists() {
        std::fs::remove_file(&plan.db_path)?;
    }
    let file = std::fs::File::open(&plan.sql_path)?;
    let status = std::process::Command::new("duckdb")
        .arg(&plan.db_path)
        .stdin(std::process::Stdio::from(file))
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()?;
    if !status.success() {
        return Err(Error::sink(format!(
            "duckdb exited with status {status} while loading {}",
            plan.sql_path.display()
        )));
    }
    // The metric-partitioned staging Parquet is only an intermediate artifact.
    // Keep the empty directory so the generated SQL can be re-run manually.
    if plan.stage_dir.exists() {
        let _ = std::fs::remove_dir_all(&plan.stage_dir);
        let _ = std::fs::create_dir_all(&plan.stage_dir);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_quotes() {
        assert_eq!(sql_escape("/tmp/a'b"), "/tmp/a''b");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn reserved_label_is_renamed() {
        let mut labels = BTreeSet::new();
        labels.insert("value".to_string());
        labels.insert("job".to_string());
        let (cols, warnings) = label_columns(&labels);
        assert_eq!(cols.len(), 2);
        assert!(cols
            .iter()
            .any(|c| c.key == "value" && c.column == "label_value"));
        assert!(!warnings.is_empty());
    }

    #[test]
    fn metric_key_matches_duckdb_md5() {
        assert_eq!(
            metric_key("fixture_metric_00"),
            "a243d64f533370f28c0625fdea762cdc"
        );
    }

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tsdb-duckdb-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn tenant_listing() {
        let dir = scratch_dir("tenants");
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-a")).unwrap();
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-b")).unwrap();
        assert_eq!(output_tenants(&dir), vec!["tenant-a", "tenant-b"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn prometheus_uses_one_database() {
        let dir = scratch_dir("single");
        assert_eq!(
            resolve_db_paths(&dir, "", None).unwrap(),
            dir.join("migration.duckdb")
        );
        // A configured directory gets the default file name appended.
        assert_eq!(
            resolve_db_paths(&dir, "/tmp/some-dir", None).unwrap(),
            PathBuf::from("/tmp/some-dir/migration.duckdb")
        );
        // A configured file is used verbatim.
        assert_eq!(
            resolve_db_paths(&dir, "/tmp/a.duckdb", None).unwrap(),
            PathBuf::from("/tmp/a.duckdb")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn mimir_gets_one_database_per_tenant() {
        let dir = scratch_dir("multi");
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-a")).unwrap();
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-b")).unwrap();
        assert_eq!(
            resolve_db_paths(&dir, "", Some("tenant-a")).unwrap(),
            dir.join("duckdb/tenant-a.duckdb")
        );
        assert_eq!(
            resolve_db_paths(&dir, "", Some("tenant-b")).unwrap(),
            dir.join("duckdb/tenant-b.duckdb")
        );
        // A single-file target cannot hold more than one tenant.
        assert!(resolve_db_paths(&dir, "/tmp/f.duckdb", Some("tenant-a")).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn single_tenant_mimir_accepts_a_file() {
        let dir = scratch_dir("one");
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-a")).unwrap();
        assert_eq!(
            resolve_db_paths(&dir, "/tmp/only.duckdb", Some("tenant-a")).unwrap(),
            PathBuf::from("/tmp/only.duckdb")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
