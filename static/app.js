/* ── Greeting ─────────────────────────────────────────── */

function setGreeting() {
  const h = new Date().getHours();
  let msg;
  if (h < 6)       msg = "Burning the midnight oil?";
  else if (h < 12) msg = "Good morning. Let\u2019s see what\u2019s running.";
  else if (h < 17) msg = "Good afternoon. Here\u2019s the latest.";
  else if (h < 21) msg = "Good evening. Checking in on jobs.";
  else              msg = "Late session. Here\u2019s the status.";
  document.getElementById("greeting").textContent = msg;
}

/* ── Loading messages ────────────────────────────────── */

const LOADING_MSGS = [
  "SSHing into the cluster\u2026",
  "Poking the scheduler\u2026",
  "Asking squeue nicely\u2026",
  "Checking on your jobs\u2026",
  "Rounding up job statuses\u2026",
  "Querying the queue\u2026",
];
let loadMsgIdx = 0;

function nextLoadMsg() {
  return LOADING_MSGS[loadMsgIdx++ % LOADING_MSGS.length];
}

/* ── Name filter ─────────────────────────────────────── */

let nameFilter = "";

function applyNameFilter() {
  nameFilter = document.getElementById("name-filter").value.toLowerCase();
  const rows = document.querySelectorAll("tr[data-jobname]");
  rows.forEach((row) => {
    const jobName = row.dataset.jobname;
    row.style.display = (nameFilter === "" || jobName.includes(nameFilter)) ? "" : "none";
  });
}

/* ── Constants ───────────────────────────────────────── */

const STATE_CLASSES = {
  RUNNING:    "badge-running",
  PENDING:    "badge-pending",
  COMPLETING: "badge-completing",
  FAILED:     "badge-failed",
  CANCELLED:  "badge-cancelled",
  COMPLETED:  "badge-completed",
  TIMEOUT:    "badge-timeout",
};

const RECENT_COLUMNS = [
  { key: "JobID",    label: "Job ID" },
  { key: "JobName",  label: "Name" },
  { key: "State",    label: "State" },
  { key: "Elapsed",  label: "Elapsed" },
  { key: "Start",    label: "Start" },
  { key: "End",      label: "End" },
  { key: "ExitCode", label: "Exit Code" },
];

// Per-server open/closed state for the recent-jobs disclosure (survives DOM diffing)
const recentOpen = {};

// Per-job GPU expansion state (survives DOM diffing)
const jobGpuOpen = {};

const CLICKABLE_STATES = new Set(["RUNNING", "COMPLETING"]);

const COLUMNS = [
  { key: "JOBID",            label: "Job ID" },
  { key: "PARTITION",        label: "Partition" },
  { key: "NAME",             label: "Name" },
  { key: "STATE",            label: "State" },
  { key: "TIME",             label: "Elapsed / Start" },
  { key: "TIME_LIMIT",       label: "Time Limit" },
  { key: "TRES_PER_NODE",    label: "GPUs" },
  { key: "SUBMIT_TIME",      label: "Submitted" },
  { key: "NODES",            label: "Nodes" },
  { key: "NODELIST(REASON)", label: "Nodelist / Reason" },
];

/* ── Helpers ─────────────────────────────────────────── */

function badgeClass(state) {
  if (STATE_CLASSES[state]) return STATE_CLASSES[state];
  // sacct may return e.g. "CANCELLED by 12345" or "CANCELLED+"
  for (const key of Object.keys(STATE_CLASSES)) {
    if (state.startsWith(key)) return STATE_CLASSES[key];
  }
  return "badge-other";
}

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

const COPY_ICON = `<svg class="copy-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>`;
const CHECK_ICON = `<svg class="copy-icon copied" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>`;
const GPU_ICON = `<svg class="gpu-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M18 20V10M12 20V4M6 20v-6"/></svg>`;

async function copyJobId(jobId, btn) {
  try {
    await navigator.clipboard.writeText(jobId);
    if (btn) {
      const icon = btn.querySelector(".copy-icon");
      if (icon) {
        icon.outerHTML = CHECK_ICON;
        const restore = () => {
          const cur = btn.querySelector(".copy-icon");
          if (cur) cur.outerHTML = COPY_ICON;
          btn.removeEventListener("mouseleave", restore);
        };
        btn.addEventListener("mouseleave", restore);
      }
    }
  } catch (err) {
    console.error('Failed to copy job ID:', err);
  }
}

/** Format a SLURM timestamp as a relative time string (e.g. "5m ago", "2h30m ago"). */
function formatRelativeTime(timestamp) {
  if (!timestamp || timestamp === "Unknown" || timestamp === "N/A") return timestamp || "-";
  // SLURM timestamps: "2025-02-15T14:30:00" or similar
  const date = new Date(timestamp.replace("T", " "));
  if (isNaN(date.getTime())) return timestamp;
  const now = new Date();
  const diffMs = now - date;
  if (diffMs < 0) return timestamp; // future timestamps shown as-is

  const totalMin = Math.floor(diffMs / 60000);
  if (totalMin < 1) return "just now";
  if (totalMin < 60) return `${totalMin}m ago`;

  const hours = Math.floor(totalMin / 60);
  const mins = totalMin % 60;
  if (hours < 24) {
    return mins > 0 ? `${hours}h${mins}m ago` : `${hours}h ago`;
  }

  const days = Math.floor(hours / 24);
  const remHours = hours % 24;
  if (remHours > 0) return `${days}d${remHours}h ago`;
  return `${days}d ago`;
}

/** Format a timestamp value according to user preference. */
function formatTimestamp(val) {
  if ((currentConfig.timestamp_format || "absolute") === "relative") {
    return formatRelativeTime(val);
  }
  return escapeHtml(val);
}

function formatGres(val) {
  const m = val.match(/gpu[^:]*:(\d+)/i);
  return m ? m[1] + " GPU" : (val && val !== "N/A" ? val : "-");
}

/** Return the display HTML for a single cell value. */
function cellHtml(col, job) {
  const val = job[col.key] || "";
  if (col.key === "STATE") {
    return `<span class="badge ${badgeClass(val)}">${val}</span>`;
  }
  if (col.key === "JOBID") {
    return `<button class="job-id-copy" onclick="event.stopPropagation(); copyJobId('${escapeHtml(val)}', this)" title="Click to copy">${escapeHtml(val)}<svg class="copy-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg></button>`;
  }
  if (col.key === "TRES_PER_NODE") {
    return formatGres(val);
  }
  if (col.key === "SUBMIT_TIME") {
    return formatTimestamp(val);
  }
  // For PENDING jobs, show START_TIME instead of elapsed time
  if (col.key === "TIME" && job.STATE === "PENDING") {
    return escapeHtml(job.START_TIME || "-");
  }
  return escapeHtml(val);
}

/* ── Recently finished disclosure ────────────────────── */

function buildRecentSection(serverName, recentJobs, isOpen) {
  const count = recentJobs.length;
  const openCls = isOpen ? " open" : "";

  let html = `<div class="recent-section">`;
  html += `<button class="recent-toggle" onclick="toggleRecent('${serverName}')">`;
  html += `<span class="recent-chevron${openCls}">&#9654;</span> `;
  html += `Recently finished (${count})`;
  html += `</button>`;
  html += `<div class="recent-body${openCls}">`;
  html += buildRecentTableHtml(serverName, recentJobs);
  html += `</div></div>`;
  return html;
}

function buildRecentTableHtml(serverName, recentJobs) {
  let html = `<div class="table-wrap"><table class="recent-table"><thead><tr>`;
  RECENT_COLUMNS.forEach((c) => { html += `<th data-col="${c.key}">${c.label}</th>`; });
  html += `</tr></thead><tbody>`;
  recentJobs.forEach((job) => {
    const jobName = (job.JobName || "").replace(/'/g, "\\'");
    const onclick = `onclick="showJobOutput('${serverName}','${job.JobID}','${jobName}')"`;
    html += `<tr class="clickable" ${onclick} title="Click to view output" data-jobname="${(job.JobName || '').toLowerCase()}">`;
    RECENT_COLUMNS.forEach((c) => {
      const val = job[c.key] || "";
      if (c.key === "State") {
        html += `<td data-col="${c.key}"><span class="badge ${badgeClass(val)}">${escapeHtml(val)}</span></td>`;
      } else if (c.key === "JobID") {
        html += `<td data-col="${c.key}"><button class="job-id-copy" onclick="event.stopPropagation(); copyJobId('${escapeHtml(val)}', this)" title="Click to copy">${escapeHtml(val)}<svg class="copy-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg></button></td>`;
      } else if (c.key === "Start" || c.key === "End") {
        html += `<td data-col="${c.key}">${formatTimestamp(val)}</td>`;
      } else {
        html += `<td data-col="${c.key}">${escapeHtml(val)}</td>`;
      }
    });
    html += `</tr>`;
  });
  html += `</tbody></table></div>`;
  return html;
}

function toggleRecent(serverName) {
  recentOpen[serverName] = !recentOpen[serverName];
  const section = document.getElementById(`server-${serverName}`);
  if (!section) return;
  const chevron = section.querySelector(".recent-chevron");
  const body = section.querySelector(".recent-body");
  if (chevron) chevron.classList.toggle("open");
  if (body) body.classList.toggle("open");
}

/* ── Per-Job GPU Status ──────────────────────────────── */

async function toggleJobGpu(event, serverName, jobId) {
  event.stopPropagation(); // Prevent row click

  const jobKey = `${serverName}-${jobId}`;
  const wasOpen = !!jobGpuOpen[jobKey];
  jobGpuOpen[jobKey] = !wasOpen;

  const section = document.getElementById(`server-${serverName}`);
  if (!section) return;

  const tbody = section.querySelector("tbody");
  if (!tbody) return;

  const jobRow = tbody.querySelector(`tr[data-jobid="${jobId}"]`);
  if (!jobRow) return;

  // Toggle GPU active state on row
  if (jobGpuOpen[jobKey]) {
    jobRow.classList.add("gpu-active");
  } else {
    jobRow.classList.remove("gpu-active");
  }

  // Find or create GPU details row
  let gpuRow = jobRow.nextElementSibling;
  if (!gpuRow || !gpuRow.classList.contains("gpu-details-row")) {
    gpuRow = document.createElement("tr");
    gpuRow.className = "gpu-details-row";
    gpuRow.innerHTML = `<td colspan="100%"><div class="gpu-details-content"></div></td>`;
    jobRow.after(gpuRow);
  }

  if (jobGpuOpen[jobKey]) {
    gpuRow.classList.add("open");
    await fetchJobGpuStatus(serverName, jobId);
  } else {
    gpuRow.classList.remove("open");
  }
}

async function fetchJobGpuStatus(serverName, jobId) {
  const section = document.getElementById(`server-${serverName}`);
  if (!section) return;

  const tbody = section.querySelector("tbody");
  if (!tbody) return;

  const jobRow = tbody.querySelector(`tr[data-jobid="${jobId}"]`);
  if (!jobRow) return;

  const gpuRow = jobRow.nextElementSibling;
  if (!gpuRow || !gpuRow.classList.contains("gpu-details-row")) return;

  const content = gpuRow.querySelector(".gpu-details-content");
  if (!content) return;

  content.innerHTML = '<div class="loading-text"><span class="spinner"></span>Fetching GPU status…</div>';

  try {
    const resp = await fetch(
      `/api/job-gpu-status?server=${encodeURIComponent(serverName)}&jobid=${encodeURIComponent(jobId)}`
    );
    const data = await resp.json();

    if (data.error) {
      content.innerHTML = `<div class="error-msg">${escapeHtml(data.error)}</div>`;
      return;
    }

    if (!data.nodes || data.nodes.length === 0) {
      content.innerHTML = '<div class="no-jobs">No GPU data available for this job.</div>';
      return;
    }

    content.innerHTML = buildJobGpuHtml(data.nodes);
  } catch (err) {
    content.innerHTML = `<div class="error-msg">Failed to fetch GPU status: ${escapeHtml(err.message)}</div>`;
  }
}

function buildJobGpuHtml(nodeData) {
  const multiNode = nodeData.length > 1;

  // Collect all GPUs for aggregate summary
  const allGpus = [];
  nodeData.forEach((nodeInfo) => {
    if (!nodeInfo.error && nodeInfo.gpus) {
      nodeInfo.gpus.forEach((gpu) => allGpus.push(gpu));
    }
  });

  let html = '<div class="gpu-compact">';

  // Aggregate summary bar
  if (allGpus.length > 0) {
    const avgUtil = Math.round(allGpus.reduce((s, g) => s + (parseInt(g.gpu_util) || 0), 0) / allGpus.length);
    const totalMemUsed = allGpus.reduce((s, g) => s + (parseInt(g.mem_used) || 0), 0);
    const totalMemTotal = allGpus.reduce((s, g) => s + (parseInt(g.mem_total) || 0), 0);
    const peakTemp = Math.max(...allGpus.map((g) => parseInt(g.temperature) || 0));

    const memUsedGB = (totalMemUsed / 1024).toFixed(1);
    const memTotalGB = (totalMemTotal / 1024).toFixed(1);

    let summaryUtilClass = "util-low";
    if (avgUtil > 80) summaryUtilClass = "util-high";
    else if (avgUtil > 40) summaryUtilClass = "util-medium";

    let summaryTempClass = "temp-normal";
    if (peakTemp > 80) summaryTempClass = "temp-high";
    else if (peakTemp > 70) summaryTempClass = "temp-warm";

    html += '<div class="gpu-summary">';
    html += `<div class="gpu-summary-item"><span class="gpu-summary-label">GPUs</span><span class="gpu-summary-value">${allGpus.length}</span></div>`;
    html += `<div class="gpu-summary-item"><span class="gpu-summary-label">Avg Util</span><div class="gpu-summary-bar"><div class="gpu-bar"><div class="gpu-bar-fill ${summaryUtilClass}" style="width:${avgUtil}%"></div><span class="gpu-bar-text">${avgUtil}%</span></div></div></div>`;
    html += `<div class="gpu-summary-item"><span class="gpu-summary-label">Memory</span><span class="gpu-summary-value">${memUsedGB} / ${memTotalGB} GB</span></div>`;
    html += `<div class="gpu-summary-item"><span class="gpu-summary-label">Peak Temp</span><span class="gpu-summary-value"><span class="gpu-temp-dot ${summaryTempClass}"></span>${peakTemp}&deg;C</span></div>`;
    html += '</div>';
  }

  html += '<table class="gpu-table"><thead><tr>';
  html += '<th>GPU</th><th>Utilization</th><th>Memory</th><th>Temp</th>';
  html += '</tr></thead><tbody>';

  nodeData.forEach((nodeInfo) => {
    if (nodeInfo.error) {
      const label = nodeInfo.node ? escapeHtml(nodeInfo.node) + ': ' : '';
      html += `<tr><td colspan="4" class="gpu-table-msg error-msg">${label}${escapeHtml(nodeInfo.error)}</td></tr>`;
      return;
    }

    if (multiNode && nodeInfo.node) {
      html += `<tr class="gpu-node-header"><td colspan="4">${escapeHtml(nodeInfo.node)}</td></tr>`;
    }

    if (!nodeInfo.gpus || nodeInfo.gpus.length === 0) {
      html += '<tr><td colspan="4" class="gpu-table-msg">No GPUs allocated</td></tr>';
      return;
    }

    nodeInfo.gpus.forEach((gpu) => {
      const gpuUtil = parseInt(gpu.gpu_util) || 0;
      const memUsed = parseInt(gpu.mem_used) || 0;
      const memTotal = parseInt(gpu.mem_total) || 1;
      const memPct = Math.round((memUsed / memTotal) * 100);
      const temp = parseInt(gpu.temperature) || 0;

      let utilClass = "util-low";
      if (gpuUtil > 80) utilClass = "util-high";
      else if (gpuUtil > 40) utilClass = "util-medium";

      let memClass = "mem-low";
      if (memPct > 80) memClass = "mem-high";
      else if (memPct > 40) memClass = "mem-medium";

      let tempClass = "temp-normal";
      if (temp > 80) tempClass = "temp-high";
      else if (temp > 70) tempClass = "temp-warm";

      html += '<tr>';
      html += `<td class="gpu-cell-id"><span class="gpu-idx">${escapeHtml(gpu.index)}</span> <span class="gpu-model">${escapeHtml(gpu.name)}</span></td>`;
      html += `<td class="gpu-cell-util"><div class="gpu-bar"><div class="gpu-bar-fill ${utilClass}" style="width:${gpuUtil}%"></div><span class="gpu-bar-text">${escapeHtml(gpu.gpu_util)}%</span></div></td>`;
      html += `<td class="gpu-cell-mem"><div class="gpu-bar"><div class="gpu-bar-fill ${memClass}" style="width:${memPct}%"></div><span class="gpu-bar-text">${escapeHtml(gpu.mem_used)} / ${escapeHtml(gpu.mem_total)} MB</span></div></td>`;
      html += `<td class="gpu-cell-temp ${tempClass}"><span class="gpu-temp-dot ${tempClass}"></span>${escapeHtml(gpu.temperature)}&deg;C</td>`;
      html += '</tr>';
    });
  });

  html += '</tbody></table></div>';
  return html;
}

function patchRecentSection(section, serverData) {
  const name = serverData.server;
  const recentJobs = serverData.recent_jobs || [];
  const existing = section.querySelector(".recent-section");

  if (recentJobs.length === 0) {
    if (existing) existing.remove();
    return;
  }

  if (!existing) {
    section.insertAdjacentHTML("beforeend",
      buildRecentSection(name, recentJobs, !!recentOpen[name]));
    return;
  }

  // Update toggle label count
  const toggle = existing.querySelector(".recent-toggle");
  if (toggle) {
    const isOpen = !!recentOpen[name];
    const openCls = isOpen ? " open" : "";
    toggle.innerHTML =
      `<span class="recent-chevron${openCls}">&#9654;</span> Recently finished (${recentJobs.length})`;
  }

  // Update table contents
  const body = existing.querySelector(".recent-body");
  if (body) {
    body.innerHTML = buildRecentTableHtml(name, recentJobs);
  }
}

/* ── Config Modal ────────────────────────────────────── */

let currentConfig = { recent_jobs_count: 5 };

async function loadConfig() {
  try {
    const resp = await fetch("/api/config");
    const config = await resp.json();
    currentConfig = config;
  } catch (err) {
    console.error("Failed to load config:", err);
  }
}

async function showConfigModal() {
  const overlay = document.getElementById("config-overlay");
  document.getElementById("config-error").textContent = "";
  document.getElementById("config-recent-count").value = currentConfig.recent_jobs_count || 5;
  document.getElementById("config-refresh-interval").value = currentConfig.refresh_interval ?? 10;
  document.getElementById("config-timestamp-format").value = currentConfig.timestamp_format || "absolute";
  overlay.classList.add("active");
}

function closeConfigModal(event) {
  if (event && event.target !== document.getElementById("config-overlay")) return;
  document.getElementById("config-overlay").classList.remove("active");
}

async function saveConfig() {
  const count = parseInt(document.getElementById("config-recent-count").value);
  const interval = parseInt(document.getElementById("config-refresh-interval").value);
  const timestampFormat = document.getElementById("config-timestamp-format").value;

  const errorEl = document.getElementById("config-error");

  if (isNaN(count) || count < 1 || count > 50) {
    errorEl.textContent = "Please enter a number between 1 and 50";
    return;
  }

  try {
    const resp = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ recent_jobs_count: count, refresh_interval: interval, timestamp_format: timestampFormat })
    });

    const result = await resp.json();

    if (result.success) {
      currentConfig = result.config;
      updateInterval();
      closeConfigModal();
      fetchJobs();
    } else {
      errorEl.textContent = "Failed to save: " + (result.error || "Unknown error");
    }
  } catch (err) {
    errorEl.textContent = "Failed to save: " + err.message;
  }
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    closeModal();
    closeConfigModal();
  }
});

/* ── Modal ───────────────────────────────────────────── */

function closeModal(event) {
  if (event && event.target !== document.getElementById("modal-overlay")) return;
  document.getElementById("modal-overlay").classList.remove("active");
}

async function showJobOutput(server, jobid, jobname) {
  const overlay = document.getElementById("modal-overlay");
  const title   = document.getElementById("modal-title");
  const body    = document.getElementById("modal-body");

  title.textContent = `${jobname} (${jobid}) on ${server}`;
  body.innerHTML = '<div class="loading-text"><span class="spinner"></span>Fetching output\u2026</div>';
  overlay.classList.add("active");

  try {
    const resp = await fetch(
      `/api/job-output?server=${encodeURIComponent(server)}&jobid=${encodeURIComponent(jobid)}`
    );
    const data = await resp.json();
    if (data.error) {
      body.innerHTML = `<div class="error-msg">${escapeHtml(data.error)}</div>`;
      return;
    }

    const hasStdout = data.stdout || data.stdout_error;
    const hasStderr = data.stderr || data.stderr_error;

    if (!hasStdout && !hasStderr) {
      body.innerHTML = '<div class="no-jobs">No output files found. The job may still be starting up.</div>';
      return;
    }

    // Build tabbed interface
    let html = '<div class="log-tabs">';
    if (hasStdout) {
      html += '<button class="log-tab active" onclick="switchLogTab(event, \'stdout\')">stdout</button>';
    }
    if (hasStderr) {
      html += `<button class="log-tab${!hasStdout ? ' active' : ''}" onclick="switchLogTab(event, 'stderr')">stderr</button>`;
    }
    html += '</div>';

    // stdout content
    if (hasStdout) {
      html += '<div class="log-content active" data-log="stdout">';
      if (data.stdout_path) {
        html += `<div class="log-path">Path: <code>${escapeHtml(data.stdout_path)}</code></div>`;
      }
      if (data.stdout_error) {
        html += `<div class="error-msg">${escapeHtml(data.stdout_error)}</div>`;
      } else if (data.stdout && data.stdout.trim()) {
        html += `<pre>${escapeHtml(data.stdout)}</pre>`;
      } else {
        html += '<div class="no-jobs">No stdout output yet.</div>';
      }
      html += '</div>';
    }

    // stderr content
    if (hasStderr) {
      html += `<div class="log-content${!hasStdout ? ' active' : ''}" data-log="stderr">`;
      if (data.stderr_path) {
        html += `<div class="log-path">Path: <code>${escapeHtml(data.stderr_path)}</code></div>`;
      }
      if (data.stderr_error) {
        html += `<div class="error-msg">${escapeHtml(data.stderr_error)}</div>`;
      } else if (data.stderr && data.stderr.trim()) {
        html += `<pre>${escapeHtml(data.stderr)}</pre>`;
      } else {
        html += '<div class="no-jobs">No stderr output yet.</div>';
      }
      html += '</div>';
    }

    body.innerHTML = html;
  } catch (err) {
    body.innerHTML = `<div class="error-msg">Failed to fetch output: ${escapeHtml(err.message)}</div>`;
  }
}

function switchLogTab(event, logType) {
  const tabs = document.querySelectorAll('.log-tab');
  const contents = document.querySelectorAll('.log-content');

  tabs.forEach(tab => tab.classList.remove('active'));
  contents.forEach(content => content.classList.remove('active'));

  event.target.classList.add('active');
  document.querySelector(`.log-content[data-log="${logType}"]`).classList.add('active');
}


/* ── DOM diffing ─────────────────────────────────────── */

// Previous API response, keyed by server name.
let prevData = {};

/** Build (or update) a single server card in-place. */
function patchServer(container, serverData, isFirstRender) {
  const name     = serverData.server;
  const hasError = !!serverData.error;
  const sectionId = `server-${name}`;

  let section = document.getElementById(sectionId);

  // ── First time: create the whole card ──
  if (!section) {
    section = document.createElement("div");
    section.id = sectionId;
    section.className = "server-section" + (isFirstRender ? " entering" : "");
    container.appendChild(section);
    renderServerFull(section, serverData, isFirstRender);
    return;
  }

  // ── Subsequent updates: patch in-place ──

  // Update status dot
  const dot = section.querySelector(".status-dot");
  if (dot) {
    dot.className = "status-dot " + (hasError ? "error" : "ok");
  }

  // Update job count
  const countEl = section.querySelector(".job-count");
  if (!hasError && serverData.jobs.length >= 0) {
    const countText = `${serverData.jobs.length} job${serverData.jobs.length !== 1 ? "s" : ""}`;
    if (countEl) {
      countEl.textContent = countText;
    } else {
      // Add count span if it didn't exist (e.g. was error before)
      const h2 = section.querySelector("h2");
      if (h2 && !h2.querySelector(".job-count")) {
        const span = document.createElement("span");
        span.className = "job-count";
        span.textContent = countText;
        h2.appendChild(span);
      }
    }
  }

  const prev = prevData[name];
  const prevHadError = prev && !!prev.error;
  const prevHadJobs  = prev && !prev.error && prev.jobs.length > 0;
  const nowHasJobs   = !hasError && serverData.jobs.length > 0;

  // If the shape changed (error<->table<->empty), just re-render the body
  const shapeChanged =
    hasError !== prevHadError ||
    nowHasJobs !== prevHadJobs;

  if (shapeChanged) {
    renderServerFull(section, serverData, false);
    return;
  }

  // If error, update error message
  if (hasError) {
    const errEl = section.querySelector(".error-msg");
    if (errEl) {
      errEl.innerHTML = `Could not reach ${escapeHtml(name)}.<br><small>${escapeHtml(serverData.error)}</small>`;
    }
    return;
  }

  // If no active jobs, still patch the recent section
  if (!nowHasJobs) {
    patchRecentSection(section, serverData);
    return;
  }

  // ── Patch table rows ──
  patchTable(section, serverData);

  // ── Patch recent-jobs disclosure ──
  patchRecentSection(section, serverData);
}

/** Full render of a server card's inner content. */
function renderServerFull(section, serverData, animate) {
  const name     = serverData.server;
  const hasError = !!serverData.error;

  let html = `<h2><span class="status-dot ${hasError ? "error" : "ok"}"></span>${escapeHtml(name)}`;
  if (!hasError) {
    html += ` <span class="job-count">${serverData.jobs.length} job${serverData.jobs.length !== 1 ? "s" : ""}</span>`;
  }
  html += `</h2>`;

  if (hasError) {
    html += `<div class="error-msg">Could not reach ${escapeHtml(name)}.<br><small>${escapeHtml(serverData.error)}</small></div>`;
  } else if (serverData.jobs.length === 0) {
    html += `<div class="no-jobs">All quiet on ${escapeHtml(name)}. No jobs in queue.</div>`;
  } else {
    html += `<div class="table-wrap"><table><thead><tr>`;
    COLUMNS.forEach((c) => { html += `<th data-col="${c.key}">${c.label}</th>`; });
    html += `</tr></thead><tbody>`;
    serverData.jobs.forEach((job, idx) => {
      html += buildRowHtml(name, job, animate ? idx * 0.03 : -1);
    });
    html += `</tbody></table></div>`;
  }

  // Append recent-jobs disclosure if data is available
  const recentJobs = serverData.recent_jobs || [];
  if (recentJobs.length > 0) {
    html += buildRecentSection(name, recentJobs, !!recentOpen[name]);
  }

  section.innerHTML = html;
}

/** Build HTML string for a single <tr>. animDelay < 0 means no animation. */
function buildRowHtml(serverName, job, animDelay) {
  const state    = job["STATE"] || "";
  const clickable = CLICKABLE_STATES.has(state);
  const onclick  = clickable
    ? ` onclick="showJobOutput('${serverName}','${job.JOBID}','${(job.NAME || "").replace(/'/g, "\\'")}')" `
    : "";
  const animStyle = animDelay >= 0 ? ` style="animation: row-in 0.25s ${animDelay}s both;"` : "";
  const cls = (clickable ? "clickable" : "") + (animDelay >= 0 ? " entering" : "");

  // Check if job has GPUs
  const hasGpu = job["TRES_PER_NODE"] && job["TRES_PER_NODE"].toLowerCase().includes("gpu");
  const showGpuTrigger = hasGpu && (state === "RUNNING" || state === "COMPLETING");
  const jobKey = `${serverName}-${job.JOBID}`;
  const gpuActive = jobGpuOpen[jobKey] ? " gpu-active" : "";

  let html = `<tr class="${cls}${gpuActive}" ${onclick} title="${clickable ? "Click to view output" : ""}" data-jobid="${job.JOBID}" data-jobname="${(job.NAME || '').toLowerCase()}"${animStyle}>`;
  COLUMNS.forEach((c) => {
    const cellContent = cellHtml(c, job);

    // Make GPUs column clickable for running GPU jobs
    if (c.key === "TRES_PER_NODE" && showGpuTrigger) {
      html += `<td data-col="${c.key}" class="gpu-trigger" onclick="event.stopPropagation(); toggleJobGpu(event, '${serverName}', '${job.JOBID}')" title="View GPU stats">`;
      html += `<span class="gpu-trigger-content">${cellContent} ${GPU_ICON}</span>`;
      html += `</td>`;
    } else {
      html += `<td data-col="${c.key}">${cellContent}</td>`;
    }
  });
  html += `</tr>`;
  return html;
}

/** Diff and patch table rows for a server that already has a <tbody>. */
function patchTable(section, serverData) {
  const tbody = section.querySelector("tbody");
  if (!tbody) return;

  const name    = serverData.server;
  const newJobs = serverData.jobs;
  const newIds  = new Set(newJobs.map((j) => j.JOBID));

  // Index existing rows by JOBID (skip GPU details rows)
  const existingRows = {};
  tbody.querySelectorAll("tr[data-jobid]").forEach((tr) => {
    existingRows[tr.dataset.jobid] = tr;
  });

  // Remove rows for jobs that no longer exist
  for (const [id, tr] of Object.entries(existingRows)) {
    if (!newIds.has(id)) {
      // Also remove GPU details row if it exists
      const nextRow = tr.nextElementSibling;
      if (nextRow && nextRow.classList.contains("gpu-details-row")) {
        nextRow.remove();
      }
      tr.remove();
    }
  }

  // Update or insert rows in order
  let prevRow = null;
  for (const job of newJobs) {
    const id = job.JOBID;
    let tr = existingRows[id];
    const hasGpu = job["TRES_PER_NODE"] && job["TRES_PER_NODE"].toLowerCase().includes("gpu");
    const jobKey = `${name}-${id}`;

    const state = job["STATE"] || "";
    const showGpuTrigger = hasGpu && (state === "RUNNING" || state === "COMPLETING");

    if (tr) {
      // Check if this row has a GPU details row after it
      const hasGpuRow = tr.nextElementSibling && tr.nextElementSibling.classList.contains("gpu-details-row");
      let gpuRow = hasGpuRow ? tr.nextElementSibling : null;

      // Update GPU active class on row
      const isGpuOpen = jobGpuOpen[jobKey];
      if (isGpuOpen) {
        tr.classList.add("gpu-active");
      } else {
        tr.classList.remove("gpu-active");
      }

      // Update cells that changed
      COLUMNS.forEach((col) => {
        const td = tr.querySelector(`td[data-col="${col.key}"]`);
        if (!td) return;

        const newHtml = cellHtml(col, job);

        // Special handling for GPU trigger column
        if (col.key === "TRES_PER_NODE" && showGpuTrigger) {
          const hasGpuClass = td.classList.contains("gpu-trigger");
          if (!hasGpuClass) {
            td.className = "gpu-trigger";
            td.setAttribute("onclick", `event.stopPropagation(); toggleJobGpu(event, '${name}', '${id}')`);
            td.setAttribute("title", "View GPU stats");
            td.innerHTML = `<span class="gpu-trigger-content">${newHtml} ${GPU_ICON}</span>`;
          } else {
            const link = td.querySelector(".gpu-trigger-content");
            if (link) {
              const newContent = `${newHtml} ${GPU_ICON}`;
              if (link.innerHTML !== newContent) {
                link.innerHTML = newContent;
              }
            }
          }
        } else if (col.key === "TRES_PER_NODE" && !showGpuTrigger) {
          td.className = "";
          td.removeAttribute("onclick");
          td.removeAttribute("title");
          if (td.innerHTML !== newHtml) {
            td.innerHTML = newHtml;
          }
        } else {
          // Normal cell update
          if (td.innerHTML !== newHtml) {
            td.innerHTML = newHtml;
            td.classList.remove("changed");
            void td.offsetWidth;
            td.classList.add("changed");
          }
        }
      });

      // Update clickable state
      const clickable = CLICKABLE_STATES.has(state);
      const baseClass = clickable ? "clickable" : "";
      const gpuClass = isGpuOpen ? " gpu-active" : "";
      tr.className = baseClass + gpuClass;

      if (clickable) {
        tr.setAttribute("onclick",
          `showJobOutput('${name}','${job.JOBID}','${(job.NAME || "").replace(/'/g, "\\'")}')`
        );
        tr.title = "Click to view output";
      } else {
        tr.removeAttribute("onclick");
        tr.title = "";
      }

      // Ensure correct order - move job row and GPU row together
      if (prevRow) {
        const targetNext = prevRow.nextElementSibling;
        const actualNext = targetNext && targetNext.classList.contains("gpu-details-row")
          ? targetNext.nextElementSibling
          : targetNext;
        if (actualNext !== tr) {
          prevRow.after(tr);
          if (gpuRow) {
            tr.after(gpuRow);
          }
        }
      } else {
        if (tbody.firstElementChild !== tr) {
          tbody.prepend(tr);
          if (gpuRow) {
            tr.after(gpuRow);
          }
        }
      }

      // Update prevRow to point to GPU row if exists, otherwise the job row
      prevRow = gpuRow || tr;
    } else {
      // New job — insert a row
      const temp = document.createElement("tbody");
      temp.innerHTML = buildRowHtml(name, job, 0);
      tr = temp.firstElementChild;

      if (prevRow) {
        prevRow.after(tr);
      } else {
        tbody.prepend(tr);
      }
      prevRow = tr;
    }
  }
}

/* ── Data fetching ───────────────────────────────────── */

let isFirstRender = true;

async function fetchJobs() {
  const btn = document.getElementById("refresh-btn");
  btn.disabled = true;
  btn.innerHTML = `<span class="spinner"></span>${nextLoadMsg()}`;

  try {
    const resp = await fetch("/api/jobs");
    const data = await resp.json();
    const content = document.getElementById("content");

    // On first render, clear the "Connecting..." placeholder
    if (isFirstRender) {
      content.innerHTML = "";
    }

    // Patch each server card
    data.forEach((serverData) => {
      patchServer(content, serverData, isFirstRender);
    });

    // Store for next diff
    const newPrev = {};
    data.forEach((s) => { newPrev[s.server] = s; });
    prevData = newPrev;

    // Reapply name filter after data update
    if (nameFilter !== "") {
      applyNameFilter();
    }

    isFirstRender = false;

    document.getElementById("last-updated").textContent =
      "Updated " + new Date().toLocaleTimeString();
  } catch (err) {
    document.getElementById("content").innerHTML =
      `<div class="error-msg">Could not reach the backend. Is job_monitor.py running?<br><small>${escapeHtml(err.message)}</small></div>`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = `<svg class="refresh-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"></polyline><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"></path></svg>`;
  }
}

/* ── Auto-refresh ────────────────────────────────────── */

let refreshTimer = null;

function updateInterval() {
  clearInterval(refreshTimer);
  refreshTimer = null;
  const dot  = document.getElementById("live-dot");
  const secs = currentConfig.refresh_interval ?? 10;
  if (secs > 0) {
    refreshTimer = setInterval(fetchJobs, secs * 1000);
    dot.classList.add("active");
  } else {
    dot.classList.remove("active");
  }
}

/* ── Column toggle ───────────────────────────────────── */

function toggleAllColumns() {
  document.body.classList.toggle("show-all-columns");
  const btn = document.getElementById("col-toggle-btn");
  const isActive = document.body.classList.contains("show-all-columns");
  btn.classList.toggle("active", isActive);
  sessionStorage.setItem("showAllColumns", isActive ? "1" : "");
}

function restoreColumnToggle() {
  if (sessionStorage.getItem("showAllColumns") === "1") {
    document.body.classList.add("show-all-columns");
    const btn = document.getElementById("col-toggle-btn");
    if (btn) btn.classList.add("active");
  }
}

/* ── Init ────────────────────────────────────────────── */

setGreeting();
restoreColumnToggle();
loadConfig().then(() => {
  fetchJobs();
  updateInterval();
});
