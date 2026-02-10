#!/usr/bin/env python3
"""Job Monitor — a single-file Flask dashboard for HPC job queues."""

import subprocess
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

SERVERS = ["juwels", "ferranti"]

SQUEUE_FORMAT = "%.18i %.12P %.30j %.8T %.10M %.12l %.20b %.20V %.6D %R"
SQUEUE_COLUMNS = ["JOBID", "PARTITION", "NAME", "STATE", "TIME", "TIME_LIMIT", "TRES_PER_NODE", "SUBMIT_TIME", "NODES", "NODELIST(REASON)"]

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Monitor</title>
<style>
  :root {
    --bg: #f5f7fa;
    --card-bg: #fff;
    --text: #1a1a2e;
    --muted: #6b7280;
    --border: #e5e7eb;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 2rem;
  }
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 2rem;
  }
  h1 { font-size: 1.5rem; }
  #refresh-btn {
    padding: 0.5rem 1.2rem;
    font-size: 0.9rem;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--card-bg);
    cursor: pointer;
    transition: background 0.15s;
  }
  #refresh-btn:hover { background: #e9ecef; }
  #refresh-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .last-updated { font-size: 0.8rem; color: var(--muted); margin-left: 1rem; }
  .server-section {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1.5rem;
    margin-bottom: 1.5rem;
  }
  .server-section h2 {
    font-size: 1.15rem;
    margin-bottom: 1rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .job-count {
    font-size: 0.8rem;
    color: var(--muted);
    font-weight: normal;
  }
  .error-msg {
    color: #dc2626;
    font-size: 0.9rem;
    padding: 0.75rem;
    background: #fef2f2;
    border-radius: 6px;
  }
  .no-jobs {
    color: var(--muted);
    font-size: 0.9rem;
    padding: 0.5rem 0;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
  }
  th {
    text-align: left;
    padding: 0.5rem 0.75rem;
    border-bottom: 2px solid var(--border);
    color: var(--muted);
    font-weight: 600;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  td {
    padding: 0.55rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f9fafb; }
  .badge {
    display: inline-block;
    padding: 0.15rem 0.55rem;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 600;
    text-transform: uppercase;
  }
  .badge-running    { background: #dcfce7; color: #166534; }
  .badge-pending    { background: #fef9c3; color: #854d0e; }
  .badge-completing { background: #dbeafe; color: #1e40af; }
  .badge-failed     { background: #fee2e2; color: #991b1b; }
  .badge-cancelled  { background: #fee2e2; color: #991b1b; }
  .badge-other      { background: #f3f4f6; color: #374151; }
  .spinner {
    display: inline-block;
    width: 14px; height: 14px;
    border: 2px solid var(--border);
    border-top-color: var(--text);
    border-radius: 50%;
    animation: spin 0.6s linear infinite;
    vertical-align: middle;
    margin-right: 0.4rem;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover td { background: #eef2ff; }

  /* Modal overlay */
  .modal-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.4);
    z-index: 100;
    justify-content: center;
    align-items: center;
  }
  .modal-overlay.active { display: flex; }
  .modal {
    background: var(--card-bg);
    border-radius: 12px;
    width: min(90vw, 800px);
    max-height: 80vh;
    display: flex;
    flex-direction: column;
    box-shadow: 0 20px 60px rgba(0,0,0,0.2);
  }
  .modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 1.25rem;
    border-bottom: 1px solid var(--border);
  }
  .modal-header h3 { font-size: 1rem; }
  .modal-close {
    background: none; border: none; font-size: 1.3rem;
    cursor: pointer; color: var(--muted); padding: 0.25rem 0.5rem;
    border-radius: 4px;
  }
  .modal-close:hover { background: #f3f4f6; }
  .modal-body {
    padding: 1.25rem;
    overflow-y: auto;
    flex: 1;
  }
  .modal-body pre {
    font-family: "SF Mono", "Fira Code", "Cascadia Code", Menlo, monospace;
    font-size: 0.82rem;
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-all;
    margin: 0;
  }
  .modal-body .loading-text {
    color: var(--muted);
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
</style>
</head>
<body>

<header>
  <div style="display:flex;align-items:center;">
    <h1>Job Monitor</h1>
    <span class="last-updated" id="last-updated"></span>
  </div>
  <div style="display:flex;align-items:center;gap:0.75rem;">
    <label style="font-size:0.82rem;color:var(--muted);">Auto-refresh:</label>
    <select id="interval-select" onchange="updateInterval()" style="padding:0.35rem 0.5rem;font-size:0.85rem;border:1px solid var(--border);border-radius:6px;background:var(--card-bg);">
      <option value="0">Off</option>
      <option value="5">5s</option>
      <option value="10" selected>10s</option>
      <option value="30">30s</option>
      <option value="60">60s</option>
    </select>
    <button id="refresh-btn" onclick="fetchJobs()">Refresh</button>
  </div>
</header>

<div id="content">
  <p style="color:var(--muted);">Loading...</p>
</div>

<div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modal-title">Job Output</h3>
      <button class="modal-close" onclick="closeModal()">&times;</button>
    </div>
    <div class="modal-body" id="modal-body">
    </div>
  </div>
</div>

<script>
const STATE_CLASSES = {
  RUNNING: "badge-running",
  PENDING: "badge-pending",
  COMPLETING: "badge-completing",
  FAILED: "badge-failed",
  CANCELLED: "badge-cancelled",
};

function badgeClass(state) {
  return STATE_CLASSES[state] || "badge-other";
}

function closeModal(event) {
  if (event && event.target !== document.getElementById("modal-overlay")) return;
  document.getElementById("modal-overlay").classList.remove("active");
}
document.addEventListener("keydown", e => { if (e.key === "Escape") closeModal(); });

async function showJobOutput(server, jobid, jobname) {
  const overlay = document.getElementById("modal-overlay");
  const title = document.getElementById("modal-title");
  const body = document.getElementById("modal-body");
  title.textContent = `Output: ${jobname} (${jobid}) on ${server}`;
  body.innerHTML = '<div class="loading-text"><span class="spinner"></span>Fetching output...</div>';
  overlay.classList.add("active");

  try {
    const resp = await fetch(`/api/job-output?server=${encodeURIComponent(server)}&jobid=${encodeURIComponent(jobid)}`);
    const data = await resp.json();
    if (data.error) {
      body.innerHTML = `<div class="error-msg">${data.error}</div>`;
    } else if (!data.output.trim()) {
      body.innerHTML = '<div class="no-jobs">No output yet.</div>';
    } else {
      body.innerHTML = `<pre>${escapeHtml(data.output)}</pre>`;
    }
  } catch (err) {
    body.innerHTML = `<div class="error-msg">Failed to fetch output: ${err.message}</div>`;
  }
}

function escapeHtml(s) {
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

const CLICKABLE_STATES = new Set(["RUNNING", "COMPLETING"]);

const COLUMNS = [
  {key: "JOBID",           label: "Job ID"},
  {key: "PARTITION",       label: "Partition"},
  {key: "NAME",            label: "Name"},
  {key: "STATE",           label: "State"},
  {key: "TIME",            label: "Elapsed"},
  {key: "TIME_LIMIT",      label: "Time Limit"},
  {key: "TRES_PER_NODE",   label: "GPUs"},
  {key: "SUBMIT_TIME",     label: "Submitted"},
  {key: "NODES",           label: "Nodes"},
  {key: "NODELIST(REASON)",label: "Nodelist / Reason"},
];

function renderServer(server) {
  let html = `<div class="server-section"><h2>${server.server}`;
  if (!server.error) {
    html += ` <span class="job-count">${server.jobs.length} job${server.jobs.length !== 1 ? "s" : ""}</span>`;
  }
  html += `</h2>`;

  if (server.error) {
    html += `<div class="error-msg">${server.error}</div>`;
  } else if (server.jobs.length === 0) {
    html += `<div class="no-jobs">No jobs in queue.</div>`;
  } else {
    html += `<table><thead><tr>`;
    COLUMNS.forEach(c => { html += `<th>${c.label}</th>`; });
    html += `</tr></thead><tbody>`;
    server.jobs.forEach(job => {
      const state = job["STATE"] || "";
      const clickable = CLICKABLE_STATES.has(state);
      const onclick = clickable
        ? ` onclick="showJobOutput('${server.server}','${job.JOBID}','${(job.NAME||'').replace(/'/g,"\\'")}')" `
        : "";
      html += `<tr class="${clickable ? 'clickable' : ''}" ${onclick} title="${clickable ? 'Click to view output' : ''}">`;
      COLUMNS.forEach(c => {
        let val = job[c.key] || "";
        if (c.key === "STATE") {
          val = `<span class="badge ${badgeClass(val)}">${val}</span>`;
        } else if (c.key === "TRES_PER_NODE") {
          // Clean up gres display: "gres/gpu:4" -> "4 GPU" or "N/A"
          const m = val.match(/gpu[^:]*:(\d+)/i);
          val = m ? m[1] + " GPU" : (val && val !== "N/A" ? val : "-");
        }
        html += `<td>${val}</td>`;
      });
      html += `</tr>`;
    });
    html += `</tbody></table>`;
  }
  html += `</div>`;
  return html;
}

async function fetchJobs() {
  const btn = document.getElementById("refresh-btn");
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Loading';

  try {
    const resp = await fetch("/api/jobs");
    const data = await resp.json();
    const content = document.getElementById("content");
    content.innerHTML = data.map(renderServer).join("");
    document.getElementById("last-updated").textContent =
      "Updated " + new Date().toLocaleTimeString();
  } catch (err) {
    document.getElementById("content").innerHTML =
      `<div class="error-msg">Failed to fetch jobs: ${err.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "Refresh";
  }
}

let refreshTimer = null;
function updateInterval() {
  clearInterval(refreshTimer);
  refreshTimer = null;
  const secs = parseInt(document.getElementById("interval-select").value);
  if (secs > 0) refreshTimer = setInterval(fetchJobs, secs * 1000);
}

fetchJobs();
updateInterval();
</script>
</body>
</html>
"""


def fetch_jobs(server):
    """SSH into *server* and return parsed squeue output."""
    try:
        result = subprocess.run(
            ["ssh", server, f"squeue --me --format '{SQUEUE_FORMAT}'"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            return {"server": server, "error": result.stderr.strip(), "jobs": []}

        lines = result.stdout.strip().splitlines()
        if len(lines) <= 1:
            return {"server": server, "error": None, "jobs": []}

        header = lines[0].split()
        jobs = []
        for line in lines[1:]:
            parts = line.split(None, len(header) - 1)
            if not parts:
                continue
            job = {}
            for i, col in enumerate(header):
                job[col] = parts[i] if i < len(parts) else ""
            jobs.append(job)

        return {"server": server, "error": None, "jobs": jobs}
    except subprocess.TimeoutExpired:
        return {"server": server, "error": "SSH connection timed out", "jobs": []}
    except Exception as exc:
        return {"server": server, "error": str(exc), "jobs": []}


def fetch_job_output(server, jobid):
    """SSH into *server*, find the SLURM stdout file for *jobid*, and tail it."""
    try:
        # Get the StdOut path from scontrol
        result = subprocess.run(
            ["ssh", server, "scontrol", "show", "job", jobid],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            return {"error": result.stderr.strip() or "Failed to query job info"}

        stdout_path = None
        for part in result.stdout.split():
            if part.startswith("StdOut="):
                stdout_path = part.split("=", 1)[1]
                break

        if not stdout_path:
            return {"error": "Could not find output file path for this job"}

        # Tail the last 25 lines
        tail_result = subprocess.run(
            ["ssh", server, "tail", "-n", "25", stdout_path],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if tail_result.returncode != 0:
            return {"error": tail_result.stderr.strip() or "Failed to read output file"}

        return {"output": tail_result.stdout, "path": stdout_path}

    except subprocess.TimeoutExpired:
        return {"error": "SSH connection timed out"}
    except Exception as exc:
        return {"error": str(exc)}


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/jobs")
def api_jobs():
    with ThreadPoolExecutor(max_workers=len(SERVERS)) as pool:
        results = list(pool.map(fetch_jobs, SERVERS))
    return jsonify(results)


@app.route("/api/job-output")
def api_job_output():
    server = request.args.get("server", "")
    jobid = request.args.get("jobid", "")
    if server not in SERVERS:
        return jsonify({"error": "Unknown server"}), 400
    if not jobid.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400
    result = fetch_job_output(server, jobid)
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
