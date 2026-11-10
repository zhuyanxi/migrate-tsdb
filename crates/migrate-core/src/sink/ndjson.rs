//! NDJSON sink for debugging and small exports.
//!
//! Non-finite floats are written as strings (`"NaN"`, `"StaleNaN"`, `"Inf"`)
//! because JSON has no representation for them.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use serde_json::json;

use crate::batch::{Batch, HistRow, SeriesRow};
use crate::error::{Error, Result};
use crate::sink::{ensure_parent, part_path, BatchSink, SinkOutcome};

fn num(v: f64) -> serde_json::Value {
    if v.is_finite() {
        json!(v)
    } else if v.is_nan() {
        if v.to_bits() == 0x7ff0_0000_0000_0002 {
            json!("StaleNaN")
        } else {
            json!("NaN")
        }
    } else if v.is_sign_positive() {
        json!("+Inf")
    } else {
        json!("-Inf")
    }
}

pub struct NdjsonSink {
    dir: PathBuf,
    worker: usize,
    unit: Option<u64>,
    series: Option<BufWriter<File>>,
    samples: Option<BufWriter<File>>,
    histograms: Option<BufWriter<File>>,
    files: Vec<PathBuf>,
}

impl NdjsonSink {
    pub fn new(dir: PathBuf, worker: usize) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            worker,
            unit: None,
            series: None,
            samples: None,
            histograms: None,
            files: Vec::new(),
        })
    }

    fn writer(&mut self, dataset: &str, unit: u64) -> Result<&mut BufWriter<File>> {
        let slot = match dataset {
            "series" => &mut self.series,
            "samples" => &mut self.samples,
            _ => &mut self.histograms,
        };
        if slot.is_none() {
            let path = part_path(&self.dir, dataset, unit, self.worker, "ndjson");
            ensure_parent(&path)?;
            self.files.push(path.clone());
            *slot = Some(BufWriter::new(File::create(path)?));
        }
        Ok(slot.as_mut().unwrap())
    }

    fn finish_one(slot: &mut Option<BufWriter<File>>) -> Result<()> {
        if let Some(mut w) = slot.take() {
            w.flush()?;
            let f = w.into_inner().map_err(|e| Error::sink(e.to_string()))?;
            f.sync_all()?;
        }
        Ok(())
    }
}

impl BatchSink for NdjsonSink {
    fn write(&mut self, unit: u64, batch: &mut Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        if self.unit != Some(unit) {
            if self.unit.is_some() {
                return Err(Error::sink(
                    "attempted to write a new unit before finishing the previous one",
                ));
            }
            self.unit = Some(unit);
        }
        if !batch.series.is_empty() {
            let w = self.writer("series", unit)?;
            for r in &batch.series {
                let line = series_line(r);
                serde_json::to_writer(&mut *w, &line).map_err(|e| Error::sink(e.to_string()))?;
                w.write_all(b"\n")?;
            }
        }
        if !batch.floats.is_empty() {
            let w = self.writer("samples", unit)?;
            for r in &batch.floats {
                let line = json!({"uid": r.uid, "ts": r.ts, "value": num(r.value)});
                serde_json::to_writer(&mut *w, &line).map_err(|e| Error::sink(e.to_string()))?;
                w.write_all(b"\n")?;
            }
        }
        if !batch.hists.is_empty() {
            let w = self.writer("histograms", unit)?;
            for r in &batch.hists {
                let line = hist_line(r);
                serde_json::to_writer(&mut *w, &line).map_err(|e| Error::sink(e.to_string()))?;
                w.write_all(b"\n")?;
            }
        }
        batch.clear();
        Ok(())
    }

    fn finish_unit(&mut self, unit: u64) -> Result<()> {
        if self.unit != Some(unit) {
            return Ok(());
        }
        Self::finish_one(&mut self.series)?;
        Self::finish_one(&mut self.samples)?;
        Self::finish_one(&mut self.histograms)?;
        self.unit = None;
        Ok(())
    }

    fn end_block(&mut self) -> Result<SinkOutcome> {
        if let Some(u) = self.unit {
            self.finish_unit(u)?;
        }
        Ok(SinkOutcome {
            files: self.files.clone(),
            bytes: super::dir_size(&self.dir),
        })
    }
}

fn series_line(r: &SeriesRow) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (k, v) in &r.labels {
        map.insert(k.to_string(), json!(v.as_ref()));
    }
    json!({"uid": r.uid, "series_ref": r.series_ref, "labels": map})
}

fn hist_line(r: &HistRow) -> serde_json::Value {
    json!({
        "uid": r.uid,
        "ts": r.ts,
        "st": r.st,
        "count": num(r.count),
        "zero_count": num(r.zero_count),
        "sum": num(r.sum),
        "schema": r.schema,
        "zero_threshold": num(r.zero_threshold),
        "pos_spans": r.pos_spans.iter().map(|s| json!({"offset": s.offset, "length": s.length})).collect::<Vec<_>>(),
        "neg_spans": r.neg_spans.iter().map(|s| json!({"offset": s.offset, "length": s.length})).collect::<Vec<_>>(),
        "pos_buckets": r.pos_buckets.iter().map(|v| num(*v)).collect::<Vec<_>>(),
        "neg_buckets": r.neg_buckets.iter().map(|v| num(*v)).collect::<Vec<_>>(),
        "custom_values": r.custom_values.iter().map(|v| num(*v)).collect::<Vec<_>>(),
        "reset_hint": r.reset_hint,
    })
}
