#!/usr/bin/env python3
"""
Build abstract CPU and memory trend plots from per-second system metrics.

The default plots intentionally hide noisy per-second samples and compare
baseline against proposed using normalized runtime percentage:
  - CPU usage over 0-100% runtime
  - Memory usage over 0-100% runtime
"""

import argparse
import os
from typing import Optional, Tuple

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import matplotlib.pyplot as plt
import pandas as pd

from method_labels import method_label


DEFAULT_BASELINE_CSV = "analysis/results_baseline_465m/system_metrics_baseline_run1.csv"
DEFAULT_PROPOSED_CSV = "analysis/results_proposed_465m/system_metrics_proposed_run1.csv"
DEFAULT_OUTPUT_DIR = "analysis/resource_trends_465"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create abstract baseline/proposed CPU and memory trend plots.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--baseline-csv", default=DEFAULT_BASELINE_CSV, help="Baseline system metrics CSV.")
    parser.add_argument("--proposed-csv", default=DEFAULT_PROPOSED_CSV, help="Proposed system metrics CSV.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for generated plot and aggregated CSVs.")
    parser.add_argument("--cpu-bucket-sec", type=int, default=60, help="Bucket size for CPU trend aggregation.")
    parser.add_argument("--memory-bucket-sec", type=int, default=300, help="Bucket size for memory trend aggregation.")
    parser.add_argument("--runtime-bin-percent", type=float, default=5.0, help="Runtime percentage bin size for simplified plots.")
    parser.add_argument(
        "--cpu-runtime-stat",
        default="p95",
        choices=["median", "mean", "p95", "max"],
        help="CPU statistic used in the simplified runtime-percent plot.",
    )
    parser.add_argument(
        "--memory-runtime-stat",
        default="mean",
        choices=["median", "mean", "p95", "max"],
        help="Memory statistic used in the simplified runtime-percent plot.",
    )
    parser.add_argument("--drop-first-sec", type=float, default=0.0, help="Drop warm-up seconds from each run before aggregation.")
    return parser.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def pick_memory_column(df: pd.DataFrame) -> str:
    for column in ("rss_mib", "memory_used_mib", "heap_used_mib"):
        if column in df.columns:
            return column
    raise SystemExit("Metrics CSV must contain one of: rss_mib, memory_used_mib, heap_used_mib")


def load_metrics(path: str, mode: str, drop_first_sec: float) -> Tuple[pd.DataFrame, str]:
    df = pd.read_csv(path)
    missing = {"timestamp", "cpu_percent"} - set(df.columns)
    if missing:
        raise SystemExit("{} is missing required columns: {}".format(path, ", ".join(sorted(missing))))

    memory_col = pick_memory_column(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["cpu_percent"] = pd.to_numeric(df["cpu_percent"], errors="coerce")
    df[memory_col] = pd.to_numeric(df[memory_col], errors="coerce")
    df = df.dropna(subset=["timestamp", "cpu_percent", memory_col]).sort_values("timestamp").copy()
    if df.empty:
        raise SystemExit("{} has no usable samples".format(path))

    df["elapsed_sec"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    if drop_first_sec > 0:
        df = df[df["elapsed_sec"] >= drop_first_sec].copy()
        if df.empty:
            raise SystemExit("{} has no samples after --drop-first-sec".format(path))
        df["elapsed_sec"] = df["elapsed_sec"] - df["elapsed_sec"].iloc[0]

    df["elapsed_min"] = df["elapsed_sec"] / 60.0
    max_elapsed = df["elapsed_sec"].max()
    df["runtime_percent"] = (df["elapsed_sec"] / max_elapsed) * 100.0 if max_elapsed > 0 else 0.0
    df["mode"] = mode
    return df, memory_col


def aggregate_cpu(df: pd.DataFrame, bucket_sec: int) -> pd.DataFrame:
    result = df.copy()
    result["bucket"] = (result["elapsed_sec"] // bucket_sec).astype(int)
    grouped = result.groupby("bucket")
    out = grouped.agg(
        elapsed_min=("elapsed_min", "mean"),
        cpu_median=("cpu_percent", "median"),
        cpu_p95=("cpu_percent", lambda s: s.quantile(0.95)),
        cpu_p25=("cpu_percent", lambda s: s.quantile(0.25)),
        cpu_p75=("cpu_percent", lambda s: s.quantile(0.75)),
        samples=("cpu_percent", "size"),
    ).reset_index(drop=True)
    return out


def aggregate_memory(df: pd.DataFrame, memory_col: str, bucket_sec: int) -> pd.DataFrame:
    result = df.copy()
    result["bucket"] = (result["elapsed_sec"] // bucket_sec).astype(int)
    grouped = result.groupby("bucket")
    out = grouped.agg(
        elapsed_min=("elapsed_min", "mean"),
        memory_mean=(memory_col, "mean"),
        memory_max=(memory_col, "max"),
        memory_p95=(memory_col, lambda s: s.quantile(0.95)),
        samples=(memory_col, "size"),
    ).reset_index(drop=True)
    return out


def aggregate_runtime_percent(
    df: pd.DataFrame,
    memory_col: str,
    bin_percent: float,
    cpu_stat: str,
    memory_stat: str,
) -> pd.DataFrame:
    if bin_percent <= 0:
        raise SystemExit("--runtime-bin-percent must be > 0")

    result = df.copy()
    result["runtime_bin"] = ((result["runtime_percent"] // bin_percent) * bin_percent).clip(upper=100)
    grouped = result.groupby("runtime_bin")

    def quantile_95(series: pd.Series) -> float:
        return series.quantile(0.95)

    stat_funcs = {
        "median": "median",
        "mean": "mean",
        "p95": quantile_95,
        "max": "max",
    }
    out = grouped.agg(
        runtime_percent=("runtime_bin", "first"),
        cpu_usage=("cpu_percent", stat_funcs[cpu_stat]),
        memory_usage_mib=(memory_col, stat_funcs[memory_stat]),
        samples=("cpu_percent", "size"),
    ).reset_index(drop=True)

    out["runtime_percent"] = out["runtime_percent"].clip(0, 100)
    out = out.sort_values("runtime_percent")
    return out


def plot_simple_runtime_usage(
    output_dir: str,
    baseline_runtime: pd.DataFrame,
    proposed_runtime: pd.DataFrame,
    cpu_stat: str,
    memory_stat: str,
) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    colors = {"baseline": "#1f77b4", "proposed": "#ff7f0e"}

    def draw(metric: str, ylabel: str, title: str, filename: str) -> None:
        fig, ax = plt.subplots(figsize=(7.2, 5.2))
        for label, data in (("baseline", baseline_runtime), ("proposed", proposed_runtime)):
            ax.plot(
                data["runtime_percent"],
                data[metric],
                marker="o",
                markersize=4,
                linewidth=1.8,
                color=colors[label],
                label=method_label(label),
            )
        ax.set_title(title, fontsize=15)
        ax.set_xlabel("Runtime %")
        ax.set_ylabel(ylabel)
        ax.set_xlim(0, 100)
        ax.set_xticks(range(0, 101, 10))
        ax.grid(True, alpha=0.45)
        ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.28), ncol=2, frameon=False)
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, filename), dpi=180, bbox_inches="tight", pad_inches=0.03)
        plt.close(fig)

    draw("cpu_usage", "CPU Usage (%)", "CPU over Runtime ({})".format(cpu_stat.upper()), "cpu_runtime_percent_usage.png")
    draw(
        "memory_usage_mib",
        "Memory Usage (MiB)",
        "Memory over Runtime ({})".format(memory_stat.upper()),
        "memory_runtime_percent_usage.png",
    )


def plot_trends(
    output_path: str,
    baseline_cpu: pd.DataFrame,
    proposed_cpu: pd.DataFrame,
    baseline_memory: pd.DataFrame,
    proposed_memory: pd.DataFrame,
) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, axes = plt.subplots(2, 1, figsize=(14, 8.5), sharex=True)

    colors = {"baseline": "#4c78a8", "proposed": "#f58518"}

    for label, cpu in (("baseline", baseline_cpu), ("proposed", proposed_cpu)):
        axes[0].plot(
            cpu["elapsed_min"],
            cpu["cpu_median"],
            color=colors[label],
            linewidth=2.0,
            label="{} CPU median".format(method_label(label)),
        )
        axes[0].plot(
            cpu["elapsed_min"],
            cpu["cpu_p95"],
            color=colors[label],
            linewidth=1.4,
            linestyle="--",
            alpha=0.9,
            label="{} CPU P95".format(method_label(label)),
        )
        axes[0].fill_between(
            cpu["elapsed_min"].to_numpy(),
            cpu["cpu_p25"].to_numpy(),
            cpu["cpu_p75"].to_numpy(),
            color=colors[label],
            alpha=0.12,
            linewidth=0,
        )

    axes[0].set_title("CPU trend per minute", loc="left")
    axes[0].set_ylabel("CPU (%)")
    axes[0].legend(ncol=2, loc="upper right")

    for label, memory in (("baseline", baseline_memory), ("proposed", proposed_memory)):
        axes[1].plot(
            memory["elapsed_min"],
            memory["memory_mean"],
            color=colors[label],
            linewidth=2.0,
            label="{} memory mean".format(method_label(label)),
        )
        axes[1].plot(
            memory["elapsed_min"],
            memory["memory_max"],
            color=colors[label],
            linewidth=1.4,
            linestyle="--",
            alpha=0.9,
            label="{} memory max".format(method_label(label)),
        )

    axes[1].set_title("Memory trend per 5 minutes", loc="left")
    axes[1].set_ylabel("Memory (MiB)")
    axes[1].set_xlabel("Elapsed minutes")
    axes[1].legend(ncol=2, loc="lower right")

    fig.suptitle("Abstract ALW vs CBW Resource Trends", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    ensure_dir(args.output_dir)

    baseline_csv = args.baseline_csv
    proposed_csv = args.proposed_csv

    baseline_df, baseline_memory_col = load_metrics(baseline_csv, "baseline", args.drop_first_sec)
    proposed_df, proposed_memory_col = load_metrics(proposed_csv, "proposed", args.drop_first_sec)

    baseline_cpu = aggregate_cpu(baseline_df, args.cpu_bucket_sec)
    proposed_cpu = aggregate_cpu(proposed_df, args.cpu_bucket_sec)
    baseline_memory = aggregate_memory(baseline_df, baseline_memory_col, args.memory_bucket_sec)
    proposed_memory = aggregate_memory(proposed_df, proposed_memory_col, args.memory_bucket_sec)
    baseline_runtime = aggregate_runtime_percent(
        baseline_df,
        baseline_memory_col,
        args.runtime_bin_percent,
        args.cpu_runtime_stat,
        args.memory_runtime_stat,
    )
    proposed_runtime = aggregate_runtime_percent(
        proposed_df,
        proposed_memory_col,
        args.runtime_bin_percent,
        args.cpu_runtime_stat,
        args.memory_runtime_stat,
    )

    baseline_cpu.assign(mode="baseline").to_csv(os.path.join(args.output_dir, "baseline_cpu_trend.csv"), index=False)
    proposed_cpu.assign(mode="proposed").to_csv(os.path.join(args.output_dir, "proposed_cpu_trend.csv"), index=False)
    baseline_memory.assign(mode="baseline").to_csv(os.path.join(args.output_dir, "baseline_memory_trend.csv"), index=False)
    proposed_memory.assign(mode="proposed").to_csv(os.path.join(args.output_dir, "proposed_memory_trend.csv"), index=False)
    baseline_runtime.assign(mode="baseline").to_csv(os.path.join(args.output_dir, "baseline_runtime_percent_usage.csv"), index=False)
    proposed_runtime.assign(mode="proposed").to_csv(os.path.join(args.output_dir, "proposed_runtime_percent_usage.csv"), index=False)

    plot_path = os.path.join(args.output_dir, "abstract_resource_trends.png")
    plot_trends(plot_path, baseline_cpu, proposed_cpu, baseline_memory, proposed_memory)
    plot_simple_runtime_usage(
        args.output_dir,
        baseline_runtime,
        proposed_runtime,
        args.cpu_runtime_stat,
        args.memory_runtime_stat,
    )

    print("Baseline CSV: {}".format(baseline_csv))
    print("Proposed CSV: {}".format(proposed_csv))
    print("Wrote plot: {}".format(plot_path))
    print("Wrote simple CPU runtime plot: {}".format(os.path.join(args.output_dir, "cpu_runtime_percent_usage.png")))
    print("Wrote simple memory runtime plot: {}".format(os.path.join(args.output_dir, "memory_runtime_percent_usage.png")))
    print("Wrote aggregated trend CSVs under: {}".format(args.output_dir))


if __name__ == "__main__":
    main()
