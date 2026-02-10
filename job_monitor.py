#!/usr/bin/env python3
"""Job Monitor — Flask backend for HPC job queue dashboard."""

import subprocess
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

SERVERS = ["juwels", "ferranti"]
SQUEUE_FORMAT = "%.18i %.12P %.30j %.8T %.10M %.12l %.20b %.20V %.6D %R"


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
    return render_template("index.html")


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
    return jsonify(fetch_job_output(server, jobid))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
