#!/usr/bin/env python3
import argparse
import os

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import re
from datetime import datetime

DEFAULT_CSV = "analysis/results_run12_baseline_726m/system_metrics_baseline_run12.csv"
DEFAULT_GC_LOG = "analysis/results_run12_baseline_726m/gc_baseline_run12.log"
DEFAULT_OUTPUT = (
    "analysis/single_resource_analysis_run12/baseline_combined_profiling_run12.png"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Combine System Metrics (CSV) and GC Logs into a diagnostic plot."
    )
    parser.add_argument(
        "--csv", default=DEFAULT_CSV, help="Path to system metrics CSV file."
    )
    parser.add_argument("--gc-log", default=DEFAULT_GC_LOG, help="Path to GC log file.")
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT, help="Path to output plot image."
    )
    return parser.parse_args()


def parse_gc_log(log_path):
    # Regex to capture GC log details. Typical Java 17 format:
    # [2026-05-01T15:00:00.123+0200]...[info]...[gc] GC(123) Pause Full (Ergonomics) 3999M->3998M(4096M) 5000.000ms
    # We will make it robust enough to match standard variations.
    pattern = re.compile(
        r"\[(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{4})\].*?"
        r"Pause (?P<type>Young|Full).*?"
        r"(?P<before>\d+)[A-Z]?->(?P<after>\d+)[A-Z]?\((?P<capacity>\d+)[A-Z]?\) "
        r"(?P<duration>\d+\.\d+)ms"
    )

    gc_events = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                # Java timestamps typically have 3 digits of ms, but strptime %f expects 6.
                ts_str = match.group("timestamp")
                # A robust way to parse ISO 8601 with timezone in pandas/python
                ts = pd.to_datetime(ts_str, utc=True)

                gc_events.append(
                    {
                        "timestamp": ts,
                        "type": match.group("type"),
                        "before_mb": float(match.group("before")),
                        "after_mb": float(match.group("after")),
                        "capacity_mb": float(match.group("capacity")),
                        "duration_ms": float(match.group("duration")),
                    }
                )

    df_gc = pd.DataFrame(gc_events)
    if not df_gc.empty:
        # Sort by timestamp just in case
        df_gc = df_gc.sort_values("timestamp")
        df_gc["recovered_mb"] = df_gc["before_mb"] - df_gc["after_mb"]
    return df_gc


def main():
    args = parse_args()

    # 1. Load and parse System Metrics CSV
    print(f"Loading system metrics from: {args.csv}")
    df_sys = pd.read_csv(args.csv)
    # Convert string timestamp to datetime, ensure UTC for alignment
    df_sys["timestamp"] = pd.to_datetime(df_sys["timestamp"], utc=True)
    df_sys = df_sys.sort_values("timestamp")

    # 2. Load and parse GC Log
    print(f"Parsing GC logs from: {args.gc_log}")
    df_gc = parse_gc_log(args.gc_log)

    if df_gc.empty:
        print("Warning: No GC events parsed from the log. Plot will be empty for GC.")

    # 3. Create the Three-Tier Plot
    print("Generating 3-tier combined diagnostic plot...")
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
    fig.suptitle(
        "Combined Resource & GC Diagnostic Profiling",
        fontsize=18,
        fontweight="bold",
        y=0.95,
    )

    # --- Tier 1: RSS Memory ---
    ax1.plot(
        df_sys["timestamp"],
        df_sys["rss_mib"],
        color="#1f77b4",
        linewidth=2,
        label="RSS Memory (MiB)",
    )
    ax1.set_ylabel("Memory (MiB)", fontsize=12)
    ax1.set_title("OS-Level Physical Memory (RSS) vs Time", fontsize=14, loc="left")
    ax1.legend(loc="upper left")

    # --- Tier 2: CPU Percentage ---
    ax2.plot(
        df_sys["timestamp"],
        df_sys["cpu_percent"],
        color="#ff7f0e",
        linewidth=1.5,
        alpha=0.8,
        label="CPU Usage (%)",
    )
    ax2.set_ylabel("CPU Percent (%)", fontsize=12)
    ax2.set_title("OS-Level CPU Usage (100% = 1 Core) vs Time", fontsize=14, loc="left")
    ax2.legend(loc="upper left")

    # --- Tier 3: GC Pause Events ---
    if not df_gc.empty:
        # Separate Full and Young GC
        full_gc = df_gc[df_gc["type"] == "Full"]
        young_gc = df_gc[df_gc["type"] == "Young"]

        # Plot Young GC (blue scatter)
        ax3.scatter(
            young_gc["timestamp"],
            young_gc["duration_ms"],
            color="lightblue",
            alpha=0.5,
            s=20,
            label="Pause Young",
            edgecolors="none",
        )
        # Plot Full GC (red scatter, larger)
        ax3.scatter(
            full_gc["timestamp"],
            full_gc["duration_ms"],
            color="red",
            alpha=0.8,
            s=40,
            marker="x",
            label="Pause Full",
        )

        # Calculate moving average of GC duration or highlight poor recovery
        # (Optional advanced plotting: a twin axis for memory recovered)
        ax3_twin = ax3.twinx()
        ax3_twin.scatter(
            full_gc["timestamp"],
            full_gc["recovered_mb"],
            color="purple",
            alpha=0.3,
            s=20,
            marker="o",
            label="MB Recovered (Full)",
        )
        ax3_twin.set_ylabel("Recovered Space (MB)", color="purple", fontsize=12)
        ax3_twin.grid(False)

    ax3.set_ylabel("GC Pause Duration (ms)", fontsize=12)
    ax3.set_xlabel("Time (UTC)", fontsize=12)
    ax3.set_title("JVM Garbage Collection Events & Recovery", fontsize=14, loc="left")

    # Combine legends for ax3
    lines1, labels1 = ax3.get_legend_handles_labels()
    if not df_gc.empty:
        lines2, labels2 = ax3_twin.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    # Format x-axis dates
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S\n%m-%d"))
    plt.xticks(rotation=0)

    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Create output directory if it doesn't exist
    import os

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    plt.savefig(args.output, dpi=150, bbox_inches="tight", pad_inches=0.03)
    print(f"Plot saved successfully to: {args.output}")


if __name__ == "__main__":
    main()
