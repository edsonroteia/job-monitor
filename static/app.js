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
  const sections = document.querySelectorAll(".server-section");

  sections.forEach((section) => {
    const tbody = section.querySelector("tbody");
    if (!tbody) return;

    const rows = tbody.querySelectorAll("tr[data-jobid]");
    rows.forEach((row) => {
      const nameCell = row.querySelector("td[data-col='NAME']");
      if (!nameCell) return;

      const jobName = nameCell.textContent.toLowerCase();
      if (nameFilter === "" || jobName.includes(nameFilter)) {
        row.style.display = "";
      } else {
        row.style.display = "none";
      }
    });
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

const CLICKABLE_STATES = new Set(["RUNNING", "COMPLETING"]);

const COLUMNS = [
  { key: "JOBID",            label: "Job ID" },
  { key: "PARTITION",        label: "Partition" },
  { key: "NAME",             label: "Name" },
  { key: "STATE",            label: "State" },
  { key: "TIME",             label: "Elapsed" },
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
  if (col.key === "TRES_PER_NODE") {
    return formatGres(val);
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
  RECENT_COLUMNS.forEach((c) => { html += `<th>${c.label}</th>`; });
  html += `</tr></thead><tbody>`;
  recentJobs.forEach((job) => {
    const jobName = (job.JobName || "").replace(/'/g, "\\'");
    const onclick = `onclick="showJobOutput('${serverName}','${job.JobID}','${jobName}')"`;
    html += `<tr class="clickable" ${onclick} title="Click to view output">`;
    RECENT_COLUMNS.forEach((c) => {
      const val = job[c.key] || "";
      if (c.key === "State") {
        html += `<td><span class="badge ${badgeClass(val)}">${escapeHtml(val)}</span></td>`;
      } else {
        html += `<td>${escapeHtml(val)}</td>`;
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
  const input = document.getElementById("config-recent-count");

  input.value = currentConfig.recent_jobs_count || 5;
  overlay.classList.add("active");
}

function closeConfigModal(event) {
  if (event && event.target !== document.getElementById("config-overlay")) return;
  document.getElementById("config-overlay").classList.remove("active");
}

async function saveConfig() {
  const input = document.getElementById("config-recent-count");
  const count = parseInt(input.value);

  if (isNaN(count) || count < 1 || count > 50) {
    alert("Please enter a number between 1 and 50");
    return;
  }

  try {
    const resp = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ recent_jobs_count: count })
    });

    const result = await resp.json();

    if (result.success) {
      currentConfig = result.config;
      closeConfigModal();
      fetchJobs(); // Refresh to apply new settings
    } else {
      alert("Failed to save config: " + (result.error || "Unknown error"));
    }
  } catch (err) {
    alert("Failed to save config: " + err.message);
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
    COLUMNS.forEach((c) => { html += `<th>${c.label}</th>`; });
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

  let html = `<tr class="${cls}" ${onclick} title="${clickable ? "Click to view output" : ""}" data-jobid="${job.JOBID}"${animStyle}>`;
  COLUMNS.forEach((c) => {
    html += `<td data-col="${c.key}">${cellHtml(c, job)}</td>`;
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

  // Index existing rows by JOBID
  const existingRows = {};
  tbody.querySelectorAll("tr[data-jobid]").forEach((tr) => {
    existingRows[tr.dataset.jobid] = tr;
  });

  // Remove rows for jobs that no longer exist
  for (const [id, tr] of Object.entries(existingRows)) {
    if (!newIds.has(id)) {
      tr.remove();
    }
  }

  // Update or insert rows in order
  let prevRow = null;
  for (const job of newJobs) {
    const id = job.JOBID;
    let tr = existingRows[id];

    if (tr) {
      // Update cells that changed
      COLUMNS.forEach((col) => {
        const td = tr.querySelector(`td[data-col="${col.key}"]`);
        if (!td) return;
        const newHtml = cellHtml(col, job);
        if (td.innerHTML !== newHtml) {
          td.innerHTML = newHtml;
          td.classList.remove("changed");
          // Force reflow to restart animation
          void td.offsetWidth;
          td.classList.add("changed");
        }
      });

      // Update clickable state
      const state = job["STATE"] || "";
      const clickable = CLICKABLE_STATES.has(state);
      tr.className = clickable ? "clickable" : "";
      if (clickable) {
        tr.setAttribute("onclick",
          `showJobOutput('${name}','${job.JOBID}','${(job.NAME || "").replace(/'/g, "\\'")}')`
        );
        tr.title = "Click to view output";
      } else {
        tr.removeAttribute("onclick");
        tr.title = "";
      }
    } else {
      // New job — insert a row
      const temp = document.createElement("tbody");
      temp.innerHTML = buildRowHtml(name, job, 0);
      tr = temp.firstElementChild;
    }

    // Ensure correct order
    if (prevRow) {
      if (prevRow.nextElementSibling !== tr) {
        prevRow.after(tr);
      }
    } else {
      if (tbody.firstElementChild !== tr) {
        tbody.prepend(tr);
      }
    }
    prevRow = tr;
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
    btn.textContent = "Refresh";
  }
}

/* ── Auto-refresh ────────────────────────────────────── */

let refreshTimer = null;

function updateInterval() {
  clearInterval(refreshTimer);
  refreshTimer = null;
  const dot  = document.getElementById("live-dot");
  const secs = parseInt(document.getElementById("interval-select").value);
  if (secs > 0) {
    refreshTimer = setInterval(fetchJobs, secs * 1000);
    dot.classList.add("active");
  } else {
    dot.classList.remove("active");
  }
}

/* ── Init ────────────────────────────────────────────── */

setGreeting();
loadConfig().then(() => {
  fetchJobs();
  updateInterval();
});
