#!/usr/bin/env python3
"""Job Monitor — Flask backend for HPC job queue dashboard."""

import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

SQUEUE_FORMAT = "%.18i %.12P %.30j %.8T %.10M %.12l %.20b %.20V %.20S %.6D %R"
CONFIG_FILE = "config.json"
JOB_LOG_FILE = "job_log.json"

DEFAULT_CONFIG = {
    "servers": ["juwels", "ferranti"],
    "recent_jobs_count": 5,
    "refresh_interval": 10
}


def load_job_log():
    """Load the job log from disk."""
    if not os.path.exists(JOB_LOG_FILE):
        return {}
    try:
        with open(JOB_LOG_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_job_log(log):
    """Save the job log to disk."""
    try:
        with open(JOB_LOG_FILE, "w") as f:
            json.dump(log, f, indent=2)
    except IOError:
        pass  # Fail silently if we can't write


def update_job_in_log(server, jobid, job_info):
    """Add or update a job entry in the log."""
    log = load_job_log()
    if server not in log:
        log[server] = {}

    # Keep existing data and update with new info
    if jobid in log[server]:
        log[server][jobid].update(job_info)
    else:
        log[server][jobid] = job_info

    save_job_log(log)


def get_job_from_log(server, jobid):
    """Retrieve a job entry from the log."""
    log = load_job_log()
    return log.get(server, {}).get(jobid)


def _find_output_paths_scontrol(server, jobid):
    """Try scontrol to get the StdOut and StdErr paths (works for active/recent jobs)."""
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
            path = part.split("=", 1)[1]
            # Skip if it's a placeholder like "(null)" or empty
            if path and path != "(null)":
                stdout_path = path
        elif part.startswith("StdErr="):
            path = part.split("=", 1)[1]
            if path and path != "(null)":
                stderr_path = path
    return stdout_path, stderr_path



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

            jobid = parts[0]
            job = {
                "JobID": jobid,
                "JobName": parts[1],
                "State": state,
                "Elapsed": parts[3],
                "Start": parts[4],
                "End": parts[5],
                "ExitCode": parts[6],
            }
            jobs.append(job)

        return jobs[-count:][::-1]
    except Exception:
        return []


def fetch_job_gpu_info(server, jobid):
    """Fetch GPU allocation and stats for a specific job."""
    try:
        # Get job info including node and GRES
        result = subprocess.run(
            ["ssh", server,
             f"squeue --me --jobs={jobid} --format='%N|%b' --noheader"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return {"error": "Job not found or not running"}

        parts = result.stdout.strip().split('|')
        if len(parts) < 2:
            return {"error": "Could not parse job information"}

        nodelist = parts[0].strip()
        gres = parts[1].strip()

        # Parse GPU indices from GRES
        gpu_indices = []
        if 'IDX:' in gres:
            idx_part = gres.split('IDX:')[1].rstrip(')')
            if '-' in idx_part:
                start, end = idx_part.split('-')
                gpu_indices = list(range(int(start), int(end) + 1))
            else:
                gpu_indices = [int(idx_part)]

        # Expand nodelist
        nodes = []
        if '[' in nodelist:
            base = nodelist.split('[')[0]
            range_part = nodelist.split('[')[1].rstrip(']')
            if '-' in range_part:
                start, end = range_part.split('-')
                for i in range(int(start), int(end) + 1):
                    nodes.append(f"{base}{str(i).zfill(len(start))}")
            else:
                nodes.append(f"{base}{range_part}")
        else:
            nodes.append(nodelist)

        if not nodes:
            return {"error": "No nodes found for this job"}

        # Fetch GPU stats for each node
        node_gpus = []
        for node in nodes:
            # Run nvidia-smi on the node
            result = subprocess.run(
                ["ssh", server,
                 f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {node} "
                 f"nvidia-smi --query-gpu=index,name,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu "
                 f"--format=csv,noheader,nounits"],
                capture_output=True,
                text=True,
                timeout=15,
            )

            if result.returncode != 0:
                node_gpus.append({
                    "node": node,
                    "error": result.stderr.strip() or "Failed to get GPU info",
                    "gpus": []
                })
                continue

            gpus = []
            for line in result.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 7:
                    gpu_idx = int(parts[0])
                    # Only include GPUs allocated to this job
                    if not gpu_indices or gpu_idx in gpu_indices:
                        gpus.append({
                            "index": parts[0],
                            "name": parts[1],
                            "gpu_util": parts[2],
                            "mem_util": parts[3],
                            "mem_used": parts[4],
                            "mem_total": parts[5],
                            "temperature": parts[6],
                        })

            node_gpus.append({
                "node": node,
                "error": None,
                "gpus": gpus
            })

        return {"nodes": node_gpus}

    except subprocess.TimeoutExpired:
        return {"error": "SSH connection timed out"}
    except Exception as exc:
        return {"error": str(exc)}




def fetch_all_for_server(server, recent_count=5):
    """Fetch both active and recent jobs for *server*."""
    active = fetch_jobs(server)
    recent = fetch_recent_jobs(server, recent_count)
    active["recent_jobs"] = recent
    return active


def fetch_job_output(server, jobid):
    """SSH into *server*, find the SLURM stdout/stderr files for *jobid*, and tail them."""
    try:
        # First, check if we have this job in our log
        logged_job = get_job_from_log(server, jobid)
        stdout_path = logged_job.get("stdout_path") if logged_job else None
        stderr_path = logged_job.get("stderr_path") if logged_job else None

        # If not in log or paths missing, try to find them
        if not stdout_path and not stderr_path:
            stdout_path, stderr_path = _find_output_paths_scontrol(server, jobid)

            # Save newly found paths to log
            if stdout_path or stderr_path:
                job_info = {}
                if stdout_path:
                    job_info["stdout_path"] = stdout_path
                if stderr_path:
                    job_info["stderr_path"] = stderr_path
                update_job_in_log(server, jobid, job_info)

        if not stdout_path and not stderr_path:
            return {
                "error": "Could not find output file paths for this job",
                "details": "Job may be too old, output files may have been deleted, or job may not have generated output files"
            }

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
    servers = config.get("servers", DEFAULT_CONFIG["servers"])
    recent_count = config.get("recent_jobs_count", 5)
    with ThreadPoolExecutor(max_workers=len(servers)) as pool:
        results = list(pool.map(
            lambda s: fetch_all_for_server(s, recent_count),
            servers
        ))
    return jsonify(results)


@app.route("/api/job-output")
def api_job_output():
    server = request.args.get("server", "")
    jobid = request.args.get("jobid", "")
    config = load_config()
    if server not in config.get("servers", DEFAULT_CONFIG["servers"]):
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

        # Validate refresh_interval
        if "refresh_interval" in new_config:
            interval = new_config["refresh_interval"]
            if not isinstance(interval, int) or interval not in (0, 5, 10, 30, 60):
                return jsonify({"error": "refresh_interval must be 0, 5, 10, 30, or 60"}), 400

        config = load_config()
        config.update(new_config)

        if save_config(config):
            return jsonify({"success": True, "config": config})
        else:
            return jsonify({"error": "Failed to save config"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/job-gpu-status")
def api_job_gpu_status():
    server = request.args.get("server", "")
    jobid = request.args.get("jobid", "")
    config = load_config()
    if server not in config.get("servers", DEFAULT_CONFIG["servers"]):
        return jsonify({"error": "Unknown server"}), 400
    if not jobid.isdigit():
        return jsonify({"error": "Invalid job ID"}), 400
    return jsonify(fetch_job_gpu_info(server, jobid))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
