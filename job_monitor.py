#!/usr/bin/env python3
"""Job Monitor — Flask backend for HPC job queue dashboard."""

import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

SERVERS = ["jz"]
SQUEUE_FORMAT = "%.18i %.12P %.30j %.8T %.10M %.12l %.20b %.20V %.6D %R"
CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "recent_jobs_count": 5
}


def load_config():
    """Load configuration from JSON file or return defaults."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(config):
    """Save configuration to JSON file."""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception:
        return False


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


def fetch_recent_jobs(server, count=5):
    """SSH into *server* and return recently finished jobs via sacct."""
    finished_prefixes = ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT")
    try:
        result = subprocess.run(
            [
                "ssh", server,
                "sacct --parsable2 --noheader --allocations"
                " --starttime=now-7days"
                " --format=JobID,JobName,State,Elapsed,Start,End,ExitCode",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            return []

        jobs = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("|")
            if len(parts) < 7:
                continue
            state = parts[2]
            if not state.startswith(finished_prefixes):
                continue
            jobs.append({
                "JobID": parts[0],
                "JobName": parts[1],
                "State": state,
                "Elapsed": parts[3],
                "Start": parts[4],
                "End": parts[5],
                "ExitCode": parts[6],
            })
        return jobs[-count:][::-1]
    except Exception:
        return []


def fetch_all_for_server(server, recent_count=5):
    """Fetch both active and recent jobs for *server*."""
    active = fetch_jobs(server)
    recent = fetch_recent_jobs(server, recent_count)
    active["recent_jobs"] = recent
    return active


def _find_output_paths_scontrol(server, jobid):
    """Try scontrol to get the StdOut and StdErr paths (works for active jobs)."""
    result = subprocess.run(
        ["ssh", server, "scontrol", "show", "job", jobid],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        return None, None

    stdout_path = None
    stderr_path = None
    for part in result.stdout.split():
        if part.startswith("StdOut="):
            stdout_path = part.split("=", 1)[1]
        elif part.startswith("StdErr="):
            stderr_path = part.split("=", 1)[1]
    return stdout_path, stderr_path


def _find_output_paths_sacct(server, jobid):
    """Fallback: use sacct WorkDir + glob to find the output files."""
    result = subprocess.run(
        ["ssh", server,
         f"sacct --parsable2 --noheader --allocations"
         f" --format=WorkDir --jobs={jobid}"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        return None, None
    workdir = result.stdout.strip().splitlines()[0].strip() if result.stdout.strip() else None
    if not workdir:
        return None, None

    # Search for slurm-<jobid>*.out and slurm-<jobid>*.err in the working directory
    find_result = subprocess.run(
        ["ssh", server,
         f"find {workdir} -maxdepth 1 -name 'slurm-{jobid}*' -type f"
         f" 2>/dev/null"],
        capture_output=True, text=True, timeout=15,
    )

    stdout_path = None
    stderr_path = None
    for path in find_result.stdout.strip().splitlines():
        if path.endswith(".out"):
            stdout_path = path
        elif path.endswith(".err"):
            stderr_path = path
        elif not stdout_path:  # Default to first file found if no .out extension
            stdout_path = path

    return stdout_path, stderr_path


def fetch_job_output(server, jobid):
    """SSH into *server*, find the SLURM stdout/stderr files for *jobid*, and tail them."""
    try:
        stdout_path, stderr_path = _find_output_paths_scontrol(server, jobid)
        if not stdout_path:
            stdout_path, stderr_path = _find_output_paths_sacct(server, jobid)

        if not stdout_path and not stderr_path:
            return {"error": "Could not find output file paths for this job"}

        result = {}

        # Fetch stdout
        if stdout_path:
            tail_result = subprocess.run(
                ["ssh", server, "tail", "-n", "50", stdout_path],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if tail_result.returncode == 0:
                result["stdout"] = tail_result.stdout
                result["stdout_path"] = stdout_path
            else:
                result["stdout_error"] = tail_result.stderr.strip() or "Failed to read stdout file"

        # Fetch stderr
        if stderr_path:
            tail_result = subprocess.run(
                ["ssh", server, "tail", "-n", "50", stderr_path],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if tail_result.returncode == 0:
                result["stderr"] = tail_result.stdout
                result["stderr_path"] = stderr_path
            else:
                result["stderr_error"] = tail_result.stderr.strip() or "Failed to read stderr file"

        return result

    except subprocess.TimeoutExpired:
        return {"error": "SSH connection timed out"}
    except Exception as exc:
        return {"error": str(exc)}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/jobs")
def api_jobs():
    config = load_config()
    recent_count = config.get("recent_jobs_count", 5)
    with ThreadPoolExecutor(max_workers=len(SERVERS)) as pool:
        results = list(pool.map(
            lambda s: fetch_all_for_server(s, recent_count),
            SERVERS
        ))
    return jsonify(results)


@app.route("/api/job-output")
def api_job_output():
    server = request.args.get("server", "")
    jobid = request.args.get("jobid", "")
    if server not in SERVERS:
        return jsonify({"error": "Unknown server"}), 400
    if not jobid.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400
    return jsonify(fetch_job_output(server, jobid))


@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(load_config())


@app.route("/api/config", methods=["POST"])
def api_update_config():
    try:
        new_config = request.json
        if not isinstance(new_config, dict):
            return jsonify({"error": "Invalid config format"}), 400

        # Validate recent_jobs_count
        if "recent_jobs_count" in new_config:
            count = new_config["recent_jobs_count"]
            if not isinstance(count, int) or count < 1 or count > 50:
                return jsonify({"error": "recent_jobs_count must be between 1 and 50"}), 400

        config = load_config()
        config.update(new_config)

        if save_config(config):
            return jsonify({"success": True, "config": config})
        else:
            return jsonify({"error": "Failed to save config"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
