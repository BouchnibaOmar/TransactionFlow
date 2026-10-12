"""Measure a launched Python CLI and its process tree without changing its code."""

import argparse
import csv
import importlib.metadata
import json
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

SAMPLE_INTERVAL_SECONDS = 0.2


def runtime_versions():
    java = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
    return {
        "python": platform.python_version(), "python_executable": sys.executable,
        "pyspark": importlib.metadata.version("pyspark"), "psutil": psutil.__version__,
        "java": (java.stderr or java.stdout).strip(), "platform": platform.platform(),
    }


def sample_tree(root, tracked):
    candidates = [root] + list(tracked.values())
    denied = 0
    for process in list(candidates):
        try:
            candidates.extend(process.children(recursive=True))
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            denied += 1
    unique = {}
    for process in candidates:
        try:
            if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                unique[(process.pid, process.create_time())] = process
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            denied += 1
    tracked.clear()
    tracked.update(unique)
    rss, java_observed, observed_processes = 0, False, 0
    for process in unique.values():
        try:
            rss += process.memory_info().rss
            observed_processes += 1
            java_observed = java_observed or process.name().lower().startswith("java")
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            denied += 1
    return rss, observed_processes, java_observed, denied


def logical_output_bytes(output):
    total = 0
    if output.exists():
        for path in output.rglob("*"):
            if path.is_symlink():
                raise ValueError(f"Output size cannot follow symlinks: {path}")
            if path.is_file():
                total += path.stat().st_size
    return total


def measure_command(command, output, log, cwd, versions):
    command = [str(value) for value in command]
    output, log, cwd = Path(output).resolve(), Path(log).resolve(), Path(cwd).resolve()
    log.parent.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc).isoformat()
    tracked, peak_rss, peak_processes, samples = {}, 0, 0, 0
    java_observed, denied_events, max_gap = False, 0, 0.0
    last_sample = None
    with log.open("x", encoding="utf-8") as stream:
        started = time.perf_counter()
        process = psutil.Popen(command, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT)
        next_sample = started
        while True:
            sampled_at = time.perf_counter()
            if last_sample is not None:
                max_gap = max(max_gap, sampled_at - last_sample)
            last_sample = sampled_at
            rss, process_count, saw_java, denied = sample_tree(process, tracked)
            samples += 1
            peak_rss = max(peak_rss, rss)
            peak_processes = max(peak_processes, process_count)
            java_observed = java_observed or saw_java
            denied_events += denied
            next_sample += SAMPLE_INTERVAL_SECONDS
            timeout = max(0.001, next_sample - time.perf_counter())
            try:
                exit_code = process.wait(timeout=timeout)
                break
            except (subprocess.TimeoutExpired, psutil.TimeoutExpired):
                # Slow sampling must not cause a burst of catch-up samples.
                next_sample = max(next_sample, time.perf_counter())
        elapsed = time.perf_counter() - started
    return {
        "started_at_utc": started_at, "elapsed_seconds": f"{elapsed:.6f}",
        "peak_sampled_tree_rss_bytes": peak_rss, "output_logical_bytes": logical_output_bytes(output),
        "sample_interval_ms": 200, "sample_count": samples, "max_sample_gap_seconds": f"{max_gap:.6f}",
        "max_observed_process_count": peak_processes, "java_observed": java_observed,
        "rss_access_denied_events": denied_events, "exit_code": exit_code,
        "command_json": json.dumps(command), "cwd": str(cwd), "output_path": str(output),
        "log_path": str(log), "versions_json": json.dumps(versions, sort_keys=True),
        "os_cache_policy": "not_flushed_not_controlled",
    }


def append_measurement(csv_path, row):
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    exists = csv_path.exists()
    if exists:
        with csv_path.open(encoding="utf-8", newline="") as stream:
            if next(csv.reader(stream)) != list(row):
                raise ValueError("Measurement CSV header does not match")
    with csv_path.open("a", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        if not exists:
            writer.writeheader()
        writer.writerow(row)
        stream.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="CLI output directory to size")
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Arguments after --, launched with this Python")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("Provide a Python script and its arguments after --")
    row = measure_command([sys.executable, *command], args.output, args.log, Path.cwd(), runtime_versions())
    append_measurement(args.csv, row)
    print(json.dumps(row, indent=2))
    raise SystemExit(row["exit_code"] or (1 if row["rss_access_denied_events"] else 0))


if __name__ == "__main__":
    main()
