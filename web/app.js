/* tsdb-migrate web console — no framework, no external requests. */

'use strict';

const S = {
  token: localStorage.getItem('tsdb_token') || '',
  jobs: [],
  currentId: null,
  detail: null,
  blocks: [],
  blockTotal: 0,
  blockOffset: 0,
  blockLimit: 50,
  blockStatus: '',
  blockSort: 'min_time',
  logs: [],
  logSeq: 0,
  showDebug: false,
  health: null,
  scanning: false,
  scanBlocks: [],
  selectedBlocks: new Set(),
  tenantInfos: [],
  selectedTenants: new Set(),
};

const $ = (id) => document.getElementById(id);

const STATE_LABEL = {
  created: '未开始',
  running: '运行中',
  pausing: '暂停中',
  paused: '已暂停',
  stopping: '停止中',
  stopped: '已停止',
  completed: '已完成',
  failed: '失败',
};

const LEVEL_LABEL = { info: 'INFO', warn: 'WARN', error: 'ERROR', debug: 'DEBUG' };

/* ---------------- api ---------------- */

async function api(path, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  if (S.token) headers['Authorization'] = 'Bearer ' + S.token;
  let body = opts.body;
  if (body && typeof body !== 'string') {
    headers['content-type'] = 'application/json';
    body = JSON.stringify(body);
  }
  const res = await fetch(path, Object.assign({}, opts, { headers, body }));
  const text = await res.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch (_) { data = text; }
  }
  if (res.status === 401) {
    openModal('tokenModal');
    throw new Error('未授权：请设置 API Token');
  }
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : (typeof data === 'string' ? data : res.statusText);
    throw new Error(msg || ('HTTP ' + res.status));
  }
  return data;
}

/* ---------------- format ---------------- */

const nf = new Intl.NumberFormat('en-US');
const fmtInt = (n) => nf.format(Math.round(Number(n || 0)));

function fmtCompact(n) {
  n = Number(n || 0);
  if (n >= 1e9) return (n / 1e9).toFixed(n >= 1e10 ? 1 : 2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 1 : 2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 1 : 2) + 'K';
  return String(Math.round(n));
}

function fmtBytes(n) {
  n = Number(n || 0);
  if (n >= 1024 ** 3) return (n / 1024 ** 3).toFixed(2) + ' GiB';
  if (n >= 1024 ** 2) return (n / 1024 ** 2).toFixed(1) + ' MiB';
  if (n >= 1024) return (n / 1024).toFixed(1) + ' KiB';
  return n + ' B';
}

function fmtDuration(sec) {
  if (sec == null || !isFinite(sec) || sec < 0) return '--';
  sec = Math.round(sec);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return h + 'h' + String(m).padStart(2, '0') + 'm';
  if (m > 0) return m + 'm' + String(s).padStart(2, '0') + 's';
  return s + 's';
}

function fmtMs(ms) {
  if (!ms) return '--';
  const d = new Date(ms);
  const p = (x) => String(x).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

function fmtIso(iso) {
  if (!iso) return '--';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  const p = (x) => String(x).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function ratio(done, total) {
  const d = Number(done || 0);
  const t = Number(total || 0);
  if (t <= 0) return null;
  return Math.min(1, Math.max(0, d / t));
}

function pctText(r) {
  if (r == null) return '--';
  return (r * 100).toFixed(r > 0.999 ? 0 : 1) + '%';
}

/* ---------------- toast / modal ---------------- */

let toastTimer = null;
function toast(msg, isError) {
  const el = $('toast');
  el.textContent = msg;
  el.className = 'toast show' + (isError ? ' err' : '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.className = 'toast'; }, 3200);
}

function openModal(id) { $(id).classList.remove('hidden'); }
function closeModal(id) { $(id).classList.add('hidden'); }

document.querySelectorAll('[data-close]').forEach((b) => {
  b.addEventListener('click', () => closeModal(b.getAttribute('data-close')));
});

/* ---------------- health / jobs ---------------- */

async function refreshHealth() {
  try {
    const h = await api('/api/health');
    S.health = h;
    $('healthDot').className = 'dot ok';
    $('healthText').textContent = `API 正常 · ${h.jobs ? h.jobs.total : 0} 任务`;
    $('apiVersion').textContent = 'v' + (h.version || '?');
  } catch (e) {
    S.health = null;
    $('healthDot').className = 'dot bad';
    $('healthText').textContent = 'API 不可达';
  }
}

async function refreshJobs() {
  try {
    const jobs = await api('/api/jobs');
    S.jobs = jobs || [];
    renderJobs();
  } catch (_) {
    /* health indicator already reflects connectivity */
  }
}

function renderJobs() {
  const box = $('jobList');
  box.innerHTML = '';
  $('jobCount').textContent = S.jobs.length + ' 个任务';
  if (!S.jobs.length) {
    const d = document.createElement('div');
    d.className = 'faint';
    d.style.padding = '12px 10px';
    d.style.fontSize = '11.5px';
    d.textContent = '暂无任务。点击「＋ 新建」创建。';
    box.appendChild(d);
    return;
  }
  for (const j of S.jobs) {
    const t = j.totals || {};
    const r = ratio(t.chunks_done, t.chunks_total) ?? ratio(t.series_done, t.series_total);
    const el = document.createElement('div');
    el.className = 'job-item' + (j.job_id === S.currentId ? ' active' : '');
    el.innerHTML = `
      <div class="row1">
        <span class="jname" title="${escapeHtml(j.name || j.job_id)}">${escapeHtml(j.name || j.job_id)}</span>
        <span class="badge ${escapeHtml(j.state)}">${STATE_LABEL[j.state] || j.state}</span>
      </div>
      <div class="jmeta">
        <span>${fmtCompact(t.samples)} samples</span>
        <span>${pctText(r)}</span>
      </div>
      <div class="minibar"><span style="width:${r == null ? 0 : (r * 100).toFixed(1)}%"></span></div>`;
    el.addEventListener('click', () => selectJob(j.job_id));
    box.appendChild(el);
  }
}

function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/* ---------------- job detail ---------------- */

async function selectJob(id) {
  if (S.currentId !== id) {
    S.logs = [];
    S.logSeq = 0;
    S.blockOffset = 0;
  }
  S.currentId = id;
  renderJobs();
  await refreshDetail();
  await refreshBlocks();
  await refreshLogs();
}

async function refreshDetail() {
  if (!S.currentId) return;
  try {
    const d = await api('/api/jobs/' + encodeURIComponent(S.currentId));
    S.detail = d;
    $('emptyState').classList.add('hidden');
    $('jobView').classList.remove('hidden');
    renderDetail(d);
  } catch (e) {
    if (!S.detail) toast(e.message, true);
  }
}

function setBar(id, r) {
  const bar = $(id);
  if (!bar) return;
  const span = bar.querySelector('span');
  if (!span) return;
  span.style.width = (r == null ? 0 : r * 100).toFixed(2) + '%';
  bar.className = 'bar' + (r == null ? '' : (r >= 1 ? ' green' : (S.detail && S.detail.job.state === 'paused' ? ' amber' : '')));
}

function renderDetail(d) {
  const j = d.job;
  const t = j.totals || {};
  $('jobName').textContent = j.name || j.job_id;
  $('jobId').textContent = j.job_id;
  $('jobSink').textContent = (j.sink_kind || 'parquet') + ' · ' + (d.config && d.config.sink ? d.config.sink.compression : '');
  $('jobElapsed').textContent = '已运行 ' + fmtDuration(j.elapsed_ms / 1000);

  const st = $('jobState');
  st.className = 'badge ' + j.state;
  st.textContent = STATE_LABEL[j.state] || j.state;

  const errBox = $('errorBanner');
  if (j.error) {
    errBox.textContent = j.error;
    errBox.classList.remove('hidden');
  } else {
    errBox.classList.add('hidden');
  }

  const rChunks = ratio(t.chunks_done, t.chunks_total);
  const rSeries = ratio(t.series_done, t.series_total);
  const r = rChunks ?? rSeries;
  $('kpiPct').textContent = pctText(r);
  $('kpiPctSub').textContent = `chunks ${fmtCompact(t.chunks_done)} / ${fmtCompact(t.chunks_total)}`;
  $('kpiSamples').textContent = fmtCompact(t.samples);
  $('kpiSamplesSub').textContent = `float ${fmtCompact(t.floats)} · hist ${fmtCompact(t.histograms)}`;
  $('kpiRate').textContent = fmtCompact(j.rate.samples_per_sec) + '/s';
  $('kpiRateSub').textContent = `${fmtBytes(j.rate.bytes_per_sec)}/s · 窗口 ${j.rate.window_secs ? j.rate.window_secs.toFixed(1) : 0}s`;
  $('kpiEta').textContent = (j.eta_seconds == null || ['completed', 'failed', 'stopped'].includes(j.state))
    ? '--'
    : fmtDuration(j.eta_seconds);
  $('kpiEtaSub').textContent = '已运行 ' + fmtDuration(j.elapsed_ms / 1000);
  $('kpiBlocks').textContent = `${j.blocks_completed + j.blocks_skipped} / ${j.blocks_total}`;
  $('kpiBlocksSub').textContent = `完成 ${j.blocks_completed} · 失败 ${j.blocks_failed} · 跳过 ${j.blocks_skipped}` + (t.tenants ? ` · ${t.tenants} tenants` : '');
  const errs = Number(t.crc_errors || 0) + Number(t.decode_errors || 0);
  $('kpiErrors').textContent = fmtInt(errs);
  $('kpiErrorsSub').textContent = `tombstone ${fmtCompact(t.samples_skipped_tombstone)} · 范围 ${fmtCompact(t.samples_skipped_range)}`;
  $('kpiErrorBox').className = 'kpi' + (errs > 0 || j.blocks_failed > 0 ? ' bad' : '');

  setBar('barChunks', rChunks);
  setBar('barSeries', rSeries);
  $('numChunks').textContent = `${fmtInt(t.chunks_done)} / ${fmtInt(t.chunks_total)}`;
  $('numSeries').textContent = `${fmtInt(t.series_done)} / ${fmtInt(t.series_total)}`;

  const mcfg = d.config.mimir;
  $('cfgSource').textContent = mcfg
    ? `Mimir ${mcfg.backend === 'local' ? 'local' : 'S3'}://${mcfg.bucket}/${mcfg.prefix || ''}`
    : `Local ${j.data_dir}`;
  $('cfgTenants').textContent = mcfg
    ? `${t.tenants || 0} 个${mcfg.tenants && mcfg.tenants.length ? '（指定：' + mcfg.tenants.join(', ') + '）' : '（全部）'}`
    : '—';
  $('cfgDataDir').textContent = mcfg ? (mcfg.cache_dir || '默认 <out>/.cache') : j.data_dir;
  $('cfgOutDir').textContent = j.output_dir;
  $('cfgSink').textContent = `${j.sink_kind} / ${d.config.sink.compression}`;
  $('cfgParallel').textContent = `块 ${j.options.block_parallel} · 解码 ${j.options.decode_workers}`;
  $('cfgBatch').textContent = `${fmtInt(j.options.batch_samples)} samples · ${fmtInt(j.options.unit_series)} series`;
  $('cfgFilter').textContent = filterText(d.config.filter);
  $('cfgBytes').textContent = `${fmtBytes(t.bytes_read)} / ${fmtBytes(t.bytes_written)}`;
  $('cfgErrors').textContent = `${fmtInt(t.crc_errors)} / ${fmtInt(t.decode_errors)}`;
  $('cfgStarted').textContent = fmtIso(j.started_at);

  const state = j.state;
  $('btnStart').disabled = !(state === 'created' || state === 'stopped');
  $('btnPause').disabled = state !== 'running';
  $('btnResume').disabled = !(state === 'paused' || state === 'pausing' || state === 'stopped');
  $('btnStop').disabled = !(state === 'running' || state === 'pausing' || state === 'paused');
  $('btnDelete').disabled = !!(d.engine_alive || ['running', 'pausing', 'stopping'].includes(state));
}

function filterText(f) {
  if (!f) return '无';
  const parts = [];
  if (f.min_time != null) parts.push('min=' + fmtIso(new Date(f.min_time).toISOString()));
  if (f.max_time != null) parts.push('max=' + fmtIso(new Date(f.max_time).toISOString()));
  if (f.matchers && f.matchers.length) {
    parts.push(f.matchers.map((m) => m.name + '=' + m.value).join(','));
  }
  return parts.length ? parts.join(' · ') : '无';
}

/* ---------------- blocks ---------------- */

async function refreshBlocks() {
  if (!S.currentId) return;
  try {
    const q = new URLSearchParams({
      offset: String(S.blockOffset),
      limit: String(S.blockLimit),
      sort: S.blockSort,
    });
    if (S.blockStatus) q.set('status', S.blockStatus);
    const data = await api(`/api/jobs/${encodeURIComponent(S.currentId)}/blocks?${q}`);
    S.blocks = data.blocks || [];
    S.blockTotal = data.total || 0;
    renderBlocks();
  } catch (_) { /* detail poll shows errors */ }
}

function statusBadge(s) {
  return `<span class="badge ${escapeHtml(s)}">${escapeHtml(STATE_LABEL[s] || s)}</span>`;
}

function renderBlocks() {
  const body = $('blockBody');
  body.innerHTML = '';
  if (!S.blocks.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="10" class="faint">暂无数据</td>';
    body.appendChild(tr);
  }
  for (const b of S.blocks) {
    const r = ratio(b.chunks_done, b.chunks_total) ?? ratio(b.series_done, b.series_total);
    const skipped = Number(b.samples_skipped_tombstone || 0) + Number(b.samples_skipped_range || 0);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="faint">${b.tenant ? escapeHtml(b.tenant) : '—'}</td>
      <td title="${escapeHtml(b.path)}">${escapeHtml(b.ulid)}</td>
      <td class="faint">${fmtMs(b.min_time)} → ${fmtMs(b.max_time)}</td>
      <td>${statusBadge(b.status)}</td>
      <td class="right">${fmtInt(b.series_done)} / ${fmtInt(b.series_total)}</td>
      <td class="right">${fmtInt(b.chunks_done)} / ${fmtInt(b.chunks_total)}</td>
      <td class="right">${fmtCompact(b.floats + b.histograms)}</td>
      <td>
        <span class="cell-bar"><span style="width:${r == null ? 0 : (r * 100).toFixed(1)}%"></span></span>
        <span class="faint">${pctText(r)}</span>
      </td>
      <td class="right">${skipped ? fmtInt(skipped) : '—'}</td>
      <td class="right">${b.duration_ms != null ? fmtDuration(b.duration_ms / 1000) : '—'}</td>`;
    if (b.error) tr.title = b.error;
    body.appendChild(tr);
  }
  const from = S.blockTotal === 0 ? 0 : S.blockOffset + 1;
  const to = Math.min(S.blockOffset + S.blockLimit, S.blockTotal);
  $('blockPageInfo').textContent = `${from}–${to} / ${fmtInt(S.blockTotal)} blocks`;
  $('blockPrev').disabled = S.blockOffset <= 0;
  $('blockNext').disabled = S.blockOffset + S.blockLimit >= S.blockTotal;
}

/* ---------------- logs ---------------- */

async function refreshLogs() {
  if (!S.currentId) return;
  try {
    const data = await api(`/api/jobs/${encodeURIComponent(S.currentId)}/logs?after=${S.logSeq}&limit=300`);
    if (data.entries && data.entries.length) {
      S.logs = S.logs.concat(data.entries).slice(-1200);
      S.logSeq = data.last_seq || S.logSeq;
      renderLogs();
    } else if (data.last_seq != null) {
      S.logSeq = Math.max(S.logSeq, data.last_seq);
    }
  } catch (_) {}
}

function renderLogs() {
  const box = $('logBody');
  if (!box) return;
  const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 60;
  box.innerHTML = '';
  for (const e of S.logs) {
    if (e.level === 'debug' && !S.showDebug) continue;
    const div = document.createElement('div');
    div.className = 'log-line';
    const time = (e.ts || '').slice(11, 19);
    div.innerHTML = `<span class="t">${escapeHtml(time)}</span>` +
      `<span class="l lvl-${escapeHtml(e.level)}">${escapeHtml(LEVEL_LABEL[e.level] || e.level)}</span>` +
      `<span class="m">${escapeHtml(e.msg)}</span>`;
    box.appendChild(div);
  }
  if (nearBottom) box.scrollTop = box.scrollHeight;
}

/* ---------------- actions ---------------- */

async function doAction(action) {
  if (!S.currentId) return;
  const labels = { start: '启动', pause: '暂停', resume: '恢复', stop: '停止' };
  try {
    await api(`/api/jobs/${encodeURIComponent(S.currentId)}/actions`, {
      method: 'POST',
      body: { action },
    });
    toast(labels[action] + '指令已发送');
    await refreshDetail();
    await refreshJobs();
  } catch (e) {
    toast(e.message, true);
  }
}

async function doDelete() {
  if (!S.currentId) return;
  if (!confirm('仅删除任务记录（保留已导出的 Parquet 数据），确定继续？')) return;
  try {
    await api('/api/jobs/' + encodeURIComponent(S.currentId), { method: 'DELETE' });
    S.currentId = null;
    S.detail = null;
    S.logs = [];
    S.logSeq = 0;
    $('jobView').classList.add('hidden');
    $('emptyState').classList.remove('hidden');
    await refreshJobs();
    toast('任务记录已删除');
  } catch (e) {
    toast(e.message, true);
  }
}

/* ---------------- create job ---------------- */

async function scanBlocks() {
  const dir = $('fDataDir').value.trim();
  if (!dir) { toast('请输入数据目录', true); return; }
  const box = $('scanBox');
  $('btnScan').disabled = true;
  $('btnScan').textContent = '扫描中…';
  try {
    const data = await api('/api/blocks?scan=true&data_dir=' + encodeURIComponent(dir));
    S.scanBlocks = data.blocks || [];
    S.selectedBlocks = new Set(S.scanBlocks.map((b) => b.ulid));
    renderScan();
    if (!S.scanBlocks.length) toast('未发现 Block', true);
    else toast(`发现 ${S.scanBlocks.length} 个 Block`);
  } catch (e) {
    S.scanBlocks = [];
    S.selectedBlocks = new Set();
    renderScan();
    toast(e.message, true);
  } finally {
    $('btnScan').disabled = false;
    $('btnScan').textContent = '扫描 Blocks';
  }
}

function renderScan() {
  const box = $('scanBox');
  box.innerHTML = `
    <div class="scan-row head">
      <span><input type="checkbox" class="checkbox" id="scanAll"></span>
      <span>Block</span><span>时间范围</span><span class="n">Series</span><span class="n">Chunks</span>
    </div>`;
  if (!S.scanBlocks.length) {
    const d = document.createElement('div');
    d.className = 'scan-row';
    d.innerHTML = '<span></span><span class="u faint">扫描后显示，可全不选表示迁移全部</span><span></span><span></span><span></span>';
    box.appendChild(d);
    return;
  }
  for (const b of S.scanBlocks) {
    const row = document.createElement('div');
    row.className = 'scan-row';
    row.innerHTML = `
      <input type="checkbox" class="checkbox" data-ulid="${escapeHtml(b.ulid)}" ${S.selectedBlocks.has(b.ulid) ? 'checked' : ''}>
      <span class="u" title="${escapeHtml(b.path)}">${escapeHtml(b.ulid)}</span>
      <span class="time">${fmtMs(b.min_time)} → ${fmtMs(b.max_time)}</span>
      <span class="n">${fmtCompact(b.series)}</span>
      <span class="n">${fmtCompact(b.chunks)}</span>`;
    box.appendChild(row);
  }
  box.querySelectorAll('input[data-ulid]').forEach((cb) => {
    cb.addEventListener('change', () => {
      if (cb.checked) S.selectedBlocks.add(cb.dataset.ulid);
      else S.selectedBlocks.delete(cb.dataset.ulid);
      syncScanAll();
    });
  });
  const all = $('scanAll');
  if (all) {
    all.checked = S.selectedBlocks.size === S.scanBlocks.length && S.scanBlocks.length > 0;
    all.addEventListener('change', () => {
      if (all.checked) S.selectedBlocks = new Set(S.scanBlocks.map((b) => b.ulid));
      else S.selectedBlocks.clear();
      renderScan();
    });
  }
}

function syncScanAll() {
  const all = $('scanAll');
  if (all) all.checked = S.scanBlocks.length > 0 && S.selectedBlocks.size === S.scanBlocks.length;
}

function mimirMode() {
  return $('fSource').value === 'mimir';
}

function mimirConfig() {
  const endpoint = $('fMimirEndpoint').value.trim();
  return {
    backend: $('fMimirBackend').value,
    bucket: $('fMimirBucket').value.trim(),
    prefix: $('fMimirPrefix').value.trim(),
    region: $('fMimirRegion').value.trim() || 'us-east-1',
    endpoint,
    tenants: [],
    cache_dir: $('fMimirCache').value.trim(),
    keep_cache: $('fMimirKeep').checked,
    max_concurrent_downloads: 8,
    allow_http: endpoint.startsWith('http://'),
    force_path_style: endpoint.length > 0,
  };
}

function toggleSource() {
  const m = mimirMode();
  $('localSourceFields').classList.toggle('hidden', m);
  $('mimirSourceFields').classList.toggle('hidden', !m);
}

async function discoverTenants() {
  const cfg = mimirConfig();
  if (!cfg.bucket) { toast('请输入 bucket 或本地目录', true); return; }
  $('btnDiscoverTenants').disabled = true;
  $('btnDiscoverTenants').textContent = '扫描中…';
  try {
    const data = await api('/api/mimir/tenants', { method: 'POST', body: cfg });
    S.tenantInfos = data.tenants || [];
    S.selectedTenants = new Set(S.tenantInfos.map((t) => t.id));
    renderTenants();
    toast(`发现 ${S.tenantInfos.length} 个 tenant`);
  } catch (e) {
    S.tenantInfos = [];
    S.selectedTenants = new Set();
    renderTenants();
    toast(e.message, true);
  } finally {
    $('btnDiscoverTenants').disabled = false;
    $('btnDiscoverTenants').textContent = '扫描 Tenant';
  }
}

function renderTenants() {
  const box = $('tenantBox');
  box.innerHTML = `
    <div class="tenant-row head">
      <span><input type="checkbox" class="checkbox" id="tenantAll"></span>
      <span>Tenant</span><span class="n">Blocks</span><span class="n">Objects</span><span class="n">Bytes</span>
    </div>`;
  if (!S.tenantInfos.length) {
    const d = document.createElement('div');
    d.className = 'tenant-row';
    d.innerHTML = '<span></span><span class="u faint">点击「扫描 Tenant」发现</span><span></span><span></span><span></span>';
    box.appendChild(d);
    return;
  }
  for (const t of S.tenantInfos) {
    const row = document.createElement('div');
    row.className = 'tenant-row';
    row.innerHTML = `
      <input type="checkbox" class="checkbox" data-tenant="${escapeHtml(t.id)}" ${S.selectedTenants.has(t.id) ? 'checked' : ''}>
      <span class="u" title="${escapeHtml(t.id)}">${escapeHtml(t.id)}</span>
      <span class="n">${fmtCompact(t.blocks)}</span>
      <span class="n">${fmtCompact(t.objects)}</span>
      <span class="n">${fmtBytes(t.bytes)}</span>`;
    box.appendChild(row);
  }
  box.querySelectorAll('input[data-tenant]').forEach((cb) => {
    cb.addEventListener('change', () => {
      if (cb.checked) S.selectedTenants.add(cb.dataset.tenant);
      else S.selectedTenants.delete(cb.dataset.tenant);
      const all = $('tenantAll');
      if (all) all.checked = S.selectedTenants.size === S.tenantInfos.length;
    });
  });
  const all = $('tenantAll');
  if (all) {
    all.checked = S.selectedTenants.size === S.tenantInfos.length && S.tenantInfos.length > 0;
    all.addEventListener('change', () => {
      if (all.checked) S.selectedTenants = new Set(S.tenantInfos.map((t) => t.id));
      else S.selectedTenants.clear();
      renderTenants();
    });
  }
}

function parseMatchers(text) {
  const out = [];
  for (const part of (text || '').split(',')) {
    const s = part.trim();
    if (!s) continue;
    const i = s.indexOf('=');
    if (i <= 0) throw new Error('标签过滤格式应为 name=value：' + s);
    out.push({ name: s.slice(0, i).trim(), value: s.slice(i + 1).trim() });
  }
  return out;
}

async function createJob() {
  const m = mimirMode();
  const dataDir = $('fDataDir').value.trim();
  const outDir = $('fOutDir').value.trim();
  if (!m && !dataDir) { toast('请输入数据目录', true); return; }
  if (m && !$('fMimirBucket').value.trim()) { toast('请输入 bucket 或本地目录', true); return; }
  if (!outDir) { toast('请输入输出目录', true); return; }
  const minText = $('fMinTime').value.trim();
  const maxText = $('fMaxTime').value.trim();
  let filter;
  try {
    filter = {
      min_time: minText ? parseTimeInput(minText) : null,
      max_time: maxText ? parseTimeInput(maxText) : null,
      matchers: parseMatchers($('fMatchers').value),
    };
  } catch (e) {
    toast(e.message, true);
    return;
  }
  const allSelected = S.scanBlocks.length > 0 && S.selectedBlocks.size === S.scanBlocks.length;
  const mimir = m ? mimirConfig() : null;
  if (mimir) {
    mimir.tenants = S.selectedTenants.size === S.tenantInfos.length ? [] : Array.from(S.selectedTenants);
    if (S.tenantInfos.length > 0 && S.selectedTenants.size === 0) mimir.tenants = [];
  }
  const body = {
    name: $('fName').value.trim() || null,
    data_dir: m ? null : dataDir,
    output_dir: outDir,
    mimir,
    blocks: m || allSelected || S.selectedBlocks.size === 0 ? [] : Array.from(S.selectedBlocks),
    filter,
    sink: {
      kind: $('fSink').value,
      compression: $('fCompression').value,
      row_group_size: 65536,
      duckdb_path: '',
      duckdb_exec: true,
    },
    options: {
      block_parallel: Number($('fBlockParallel').value) || 2,
      decode_workers: Number($('fDecodeWorkers').value) || 4,
      batch_samples: Number($('fBatchSamples').value) || 65536,
      unit_series: Number($('fUnitSeries').value) || 50000,
      channel_capacity: 2048,
      verify_crc: true,
      mmap_advise: true,
      skip_bad_chunks: false,
      resume: true,
      force: false,
      continue_on_error: true,
    },
  };
  $('btnCreate').disabled = true;
  $('btnCreate').textContent = '创建中…';
  try {
    const d = await api('/api/jobs', { method: 'POST', body });
    closeModal('newModal');
    await refreshJobs();
    await selectJob(d.job.job_id);
    toast('任务已创建');
  } catch (e) {
    const n = $('newNotice');
    n.textContent = e.message;
    n.classList.remove('hidden');
  } finally {
    $('btnCreate').disabled = false;
    $('btnCreate').textContent = '创建任务';
  }
}

function parseTimeInput(s) {
  if (/^\d+$/.test(s)) return Number(s);
  const d = new Date(s);
  if (isNaN(d.getTime())) throw new Error('时间格式无效：' + s);
  return d.getTime();
}

/* ---------------- token ---------------- */

function openToken() {
  $('tokenInput').value = S.token;
  openModal('tokenModal');
}

function saveToken() {
  S.token = $('tokenInput').value.trim();
  localStorage.setItem('tsdb_token', S.token);
  closeModal('tokenModal');
  refreshHealth();
  refreshJobs();
  if (S.currentId) selectJob(S.currentId);
  toast(S.token ? 'Token 已保存' : 'Token 已清除');
}

/* ---------------- polling ---------------- */

let detailTimer = null;
let blocksTimer = null;

function startPolling() {
  clearInterval(detailTimer);
  clearInterval(blocksTimer);
  detailTimer = setInterval(async () => {
    if (document.hidden) return;
    await refreshHealth();
    await refreshJobs();
    if (S.currentId) {
      await refreshDetail();
      await refreshLogs();
    }
  }, 1500);
  blocksTimer = setInterval(() => {
    if (!document.hidden && S.currentId) refreshBlocks();
  }, 6000);
}

/* ---------------- wiring ---------------- */

function bind() {
  $('newJobBtn').addEventListener('click', () => {
    $('newNotice').classList.add('hidden');
    openModal('newModal');
  });
  $('fSource').addEventListener('change', toggleSource);
  $('btnScan').addEventListener('click', scanBlocks);
  $('btnDiscoverTenants').addEventListener('click', discoverTenants);
  $('btnCreate').addEventListener('click', createJob);
  $('tokenBtn').addEventListener('click', openToken);
  $('tokenSave').addEventListener('click', saveToken);

  $('btnStart').addEventListener('click', () => doAction('start'));
  $('btnPause').addEventListener('click', () => doAction('pause'));
  $('btnResume').addEventListener('click', () => doAction('resume'));
  $('btnStop').addEventListener('click', () => doAction('stop'));
  $('btnDelete').addEventListener('click', doDelete);

  $('blockStatusFilter').addEventListener('change', (e) => {
    S.blockStatus = e.target.value;
    S.blockOffset = 0;
    refreshBlocks();
  });
  $('blockSort').addEventListener('change', (e) => {
    S.blockSort = e.target.value;
    S.blockOffset = 0;
    refreshBlocks();
  });
  $('blockPrev').addEventListener('click', () => {
    S.blockOffset = Math.max(0, S.blockOffset - S.blockLimit);
    refreshBlocks();
  });
  $('blockNext').addEventListener('click', () => {
    if (S.blockOffset + S.blockLimit < S.blockTotal) {
      S.blockOffset += S.blockLimit;
      refreshBlocks();
    }
  });
  $('logClear').addEventListener('click', () => { S.logs = []; $('logBody').innerHTML = ''; });
  $('logDebug').addEventListener('change', (e) => {
    S.showDebug = e.target.checked;
    renderLogs();
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      document.querySelectorAll('.modal-mask').forEach((m) => m.classList.add('hidden'));
    }
  });
}

async function boot() {
  bind();
  await refreshHealth();
  await refreshJobs();
  startPolling();
  const params = new URLSearchParams(location.search);
  const job = params.get('job');
  if (job) await selectJob(job);
}

boot();
