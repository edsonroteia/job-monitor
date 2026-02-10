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

/* ── Constants ───────────────────────────────────────── */

const STATE_CLASSES = {
  RUNNING:    "badge-running",
  PENDING:    "badge-pending",
  COMPLETING: "badge-completing",
  FAILED:     "badge-failed",
  CANCELLED:  "badge-cancelled",
};

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
  return STATE_CLASSES[state] || "badge-other";
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
    } else if (!data.output.trim()) {
      body.innerHTML = '<div class="no-jobs">No output yet. The job may still be starting up.</div>';
    } else {
      body.innerHTML = `<pre>${escapeHtml(data.output)}</pre>`;
    }
  } catch (err) {
    body.innerHTML = `<div class="error-msg">Failed to fetch output: ${escapeHtml(err.message)}</div>`;
  }
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeModal();
});

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

  // If no jobs, nothing to patch
  if (!nowHasJobs) return;

  // ── Patch table rows ──
  patchTable(section, serverData);
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
    html += `<table><thead><tr>`;
    COLUMNS.forEach((c) => { html += `<th>${c.label}</th>`; });
    html += `</tr></thead><tbody>`;
    serverData.jobs.forEach((job, idx) => {
      html += buildRowHtml(name, job, animate ? idx * 0.03 : -1);
    });
    html += `</tbody></table>`;
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
fetchJobs();
updateInterval();
