#!/usr/bin/env python3
"""
local_java_process_sampler.py
=============================

Continuously sample CPU and memory usage for local Java processes on a Linux
server. The script reads from /proc directly, so it does not require psutil,
Jolokia, Docker, or changes to the Java process startup arguments.

Output CSV columns:
  timestamp,mode,run_id,target,pid,command,cpu_percent,rss_mib,
  virtual_memory_mib,memory_percent,threads

Examples:
  python3 local_java_process_sampler.py --list

  python3 local_java_process_sampler.py \
    --mode proposed \
    --run-id run1 \
    --match TaskManagerRunner \
    --target taskmanager \
    --interval-sec 1 \
    --duration 300 \
    --output output/local_java_metrics_proposed_run1.csv

  python3 local_java_process_sampler.py \
    --mode baseline \
    --pid 12345 \
    --target flink-taskmanager \
    --output output/local_java_metrics_baseline_run1.csv
"""

import argparse
import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PROC_ROOT = Path("/proc")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
CLK_TCK = os.sysconf("SC_CLK_TCK")
CPU_COUNT = os.cpu_count() or 1

CSV_FIELDS = [
    "timestamp",
    "mode",
    "run_id",
    "target",
    "pid",
    "command",
    "cpu_percent",
    "rss_mib",
    "virtual_memory_mib",
    "memory_percent",
    "threads",
]


class ProcessSample(object):
    def __init__(self, pid, command, cpu_ticks, rss_bytes, virtual_bytes, threads):
        self.pid = pid
        self.command = command
        self.cpu_ticks = cpu_ticks
        self.rss_bytes = rss_bytes
        self.virtual_bytes = virtual_bytes
        self.threads = threads


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sample local Java process CPU and memory metrics from Linux /proc",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["proposed", "baseline", "fast", "delayed", "compare"],
        help="Experiment mode label written to CSV; not required with --list",
    )
    parser.add_argument(
        "--run-id",
        default="run1",
        help="Experiment run identifier written to CSV",
    )
    parser.add_argument(
        "--target",
        default="java-process",
        help="Logical target label written to CSV",
    )
    parser.add_argument(
        "--pid",
        action="append",
        type=int,
        dest="pids",
        default=None,
        help="Java process PID to sample; pass multiple times for multiple processes",
    )
    parser.add_argument(
        "--match",
        action="append",
        dest="matches",
        default=None,
        help="Substring that must appear in the Java command line when auto-selecting a PID",
    )
    parser.add_argument(
        "--all-matches",
        action="store_true",
        help="Sample every Java process matching --match instead of requiring one match",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List local Java processes and exit",
    )
    parser.add_argument(
        "--interval-sec",
        type=float,
        default=1.0,
        help="Sampling interval in seconds",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Total sampling duration; 0 means run until Ctrl+C",
    )
    parser.add_argument(
        "--output",
        default="output/local_java_process_metrics.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--command-max-length",
        type=int,
        default=180,
        help="Maximum command length written to CSV; 0 keeps the full command",
    )
    args = parser.parse_args()

    if not args.list and not args.mode:
        parser.error("--mode is required unless --list is used")
    if args.interval_sec <= 0:
        parser.error("--interval-sec must be greater than 0")
    return args


def require_linux_proc():
    if not PROC_ROOT.exists() or not (PROC_ROOT / "stat").exists():
        raise SystemExit("This sampler requires Linux /proc and is intended for server-side Java processes.")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def bytes_to_mib(value):
    return value / (1024.0 * 1024.0)


def read_total_cpu_ticks():
    with open(PROC_ROOT / "stat", "r", encoding="utf-8") as fh:
        first_line = fh.readline().strip().split()
    if not first_line or first_line[0] != "cpu":
        raise RuntimeError("Unable to read aggregate CPU ticks from /proc/stat")
    return sum(int(value) for value in first_line[1:])


def read_total_memory_bytes():
    with open(PROC_ROOT / "meminfo", "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("Unable to read MemTotal from /proc/meminfo")


def read_cmdline(pid):
    path = PROC_ROOT / str(pid) / "cmdline"
    try:
        raw = path.read_bytes()
    except OSError:
        return ""

    command = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    if command:
        return command
    return read_comm(pid)


def read_comm(pid):
    try:
        return (PROC_ROOT / str(pid) / "comm").read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def is_java_process(pid):
    command = read_cmdline(pid)
    if not command:
        return False
    first_arg = command.split()[0]
    executable_name = Path(first_arg).name
    return executable_name.startswith("java") or read_comm(pid).startswith("java")


def iter_numeric_pids():
    for entry in PROC_ROOT.iterdir():
        if entry.name.isdigit():
            yield int(entry.name)


def list_java_processes():
    processes = []
    for pid in iter_numeric_pids():
        if not is_java_process(pid):
            continue
        try:
            sample = read_process_sample(pid)
        except OSError:
            continue
        processes.append(sample)
    return sorted(processes, key=lambda item: item.pid)


def find_matching_java_processes(matches):
    matches = matches or []
    selected = []
    for sample in list_java_processes():
        if all(match in sample.command for match in matches):
            selected.append(sample)
    return selected


def parse_proc_stat(pid):
    content = (PROC_ROOT / str(pid) / "stat").read_text(encoding="utf-8")
    close_paren = content.rfind(")")
    if close_paren < 0:
        raise RuntimeError("Unexpected /proc/{}/stat format".format(pid))

    fields_after_comm = content[close_paren + 2:].split()
    utime = int(fields_after_comm[11])
    stime = int(fields_after_comm[12])
    threads = int(fields_after_comm[17])
    virtual_bytes = int(fields_after_comm[20])
    rss_pages = int(fields_after_comm[21])
    return utime + stime, virtual_bytes, rss_pages * PAGE_SIZE, threads


def read_process_sample(pid):
    cpu_ticks, virtual_bytes, rss_bytes, threads = parse_proc_stat(pid)
    return ProcessSample(
        pid=pid,
        command=read_cmdline(pid),
        cpu_ticks=cpu_ticks,
        rss_bytes=rss_bytes,
        virtual_bytes=virtual_bytes,
        threads=threads,
    )


def ensure_output(path_str):
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists() or path.stat().st_size == 0
    fh = open(path, "a", newline="", buffering=1)
    writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
    if is_new:
        writer.writeheader()
    return path, fh, writer


def truncate_command(command, max_length):
    if max_length <= 0 or len(command) <= max_length:
        return command
    if max_length <= 3:
        return command[:max_length]
    return command[: max_length - 3] + "..."


def print_process_list(samples):
    if not samples:
        print("No local Java processes found.")
        return

    print("{:<8} {:>10}  {}".format("PID", "RSS_MIB", "COMMAND"))
    for sample in samples:
        print(
            "{:<8} {:>10.3f}  {}".format(
                sample.pid,
                bytes_to_mib(sample.rss_bytes),
                truncate_command(sample.command, 220),
            )
        )


def resolve_target_pids(args):
    if args.pids:
        return sorted(set(args.pids))

    matches = find_matching_java_processes(args.matches)
    if not matches:
        selector = " and ".join(args.matches or ["<any Java process>"])
        raise SystemExit("No local Java process matched: {}".format(selector))

    if len(matches) > 1 and not args.all_matches:
        print_process_list(matches)
        raise SystemExit(
            "Multiple Java processes matched. Use --pid, add more --match filters, or pass --all-matches."
        )

    return [sample.pid for sample in matches]


def collect_samples(pids):
    samples = {}
    for pid in pids:
        try:
            samples[pid] = read_process_sample(pid)
        except OSError:
            log.warning("PID %s is no longer readable; skipping.", pid)
    return samples


def cpu_percent(previous_sample, current_sample, previous_total_ticks, current_total_ticks):
    total_delta = current_total_ticks - previous_total_ticks
    process_delta = current_sample.cpu_ticks - previous_sample.cpu_ticks
    if total_delta <= 0 or process_delta < 0:
        return 0.0
    return (process_delta / total_delta) * CPU_COUNT * 100.0


def write_rows(args, writer, timestamp, previous_samples, current_samples, previous_total_ticks, current_total_ticks, total_memory_bytes):
    for pid, current_sample in sorted(current_samples.items()):
        previous_sample = previous_samples.get(pid)
        if previous_sample is None:
            process_cpu_percent = 0.0
        else:
            process_cpu_percent = cpu_percent(
                previous_sample,
                current_sample,
                previous_total_ticks,
                current_total_ticks,
            )

        memory_percent = (
            (current_sample.rss_bytes / total_memory_bytes) * 100.0
            if total_memory_bytes > 0
            else 0.0
        )

        writer.writerow({
            "timestamp": timestamp,
            "mode": args.mode,
            "run_id": args.run_id,
            "target": args.target,
            "pid": pid,
            "command": truncate_command(current_sample.command, args.command_max_length),
            "cpu_percent": "{:.3f}".format(process_cpu_percent),
            "rss_mib": "{:.3f}".format(bytes_to_mib(current_sample.rss_bytes)),
            "virtual_memory_mib": "{:.3f}".format(bytes_to_mib(current_sample.virtual_bytes)),
            "memory_percent": "{:.3f}".format(memory_percent),
            "threads": current_sample.threads,
        })


def main():
    args = parse_args()
    require_linux_proc()

    if args.list:
        print_process_list(list_java_processes())
        return

    pids = resolve_target_pids(args)
    output_path, fh, writer = ensure_output(args.output)
    total_memory_bytes = read_total_memory_bytes()

    log.info(
        "Sampling local Java PID(s): %s | mode=%s | run-id=%s | target=%s | interval=%.2fs | output=%s",
        ", ".join(str(pid) for pid in pids),
        args.mode,
        args.run_id,
        args.target,
        args.interval_sec,
        output_path,
    )
    log.info("CPU percent is normalized like top: 100%% means one fully used CPU core.")

    previous_total_ticks = read_total_cpu_ticks()
    previous_samples = collect_samples(pids)
    if not previous_samples:
        raise SystemExit("None of the selected Java process PIDs are readable.")

    deadline = time.monotonic() + args.duration if args.duration > 0 else None
    next_sample_at = time.monotonic() + args.interval_sec

    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                log.info("Reached requested duration, stopping.")
                break

            sleep_for = max(0.0, next_sample_at - time.monotonic())
            time.sleep(sleep_for)
            next_sample_at += args.interval_sec

            current_total_ticks = read_total_cpu_ticks()
            current_samples = collect_samples(pids)
            if not current_samples:
                log.info("All selected Java processes have exited, stopping sampler.")
                break

            write_rows(
                args,
                writer,
                now_iso(),
                previous_samples,
                current_samples,
                previous_total_ticks,
                current_total_ticks,
                total_memory_bytes,
            )

            previous_total_ticks = current_total_ticks
            previous_samples = current_samples

    except KeyboardInterrupt:
        log.info("Interrupted by user, stopping sampler.")
    finally:
        fh.flush()
        fh.close()
        log.info("Metrics written to %s", output_path)


if __name__ == "__main__":
    main()
