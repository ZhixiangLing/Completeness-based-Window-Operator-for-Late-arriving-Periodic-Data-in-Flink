#!/usr/bin/env python3
"""Plot output-count + early-purge metrics from a proposed run vs a baseline run.

Reads the per-window CSV files written by `OutputCountAggregator` (see
`flink-app/src/main/java/ge/thesis/evaluation/OutputCountAggregator.java`):

    mode,meterId,seriesId,windowStartMs,windowEndMs,outputCount,earlyPurged

Produces three PNGs in --outdir:
  1. output_count_totals.png       — total emissions per mode (two bars + ratio)
  2. outputs_per_window_dist.png   — histogram + CDF of outputs-per-window
  3. early_purge_ratio.png         — proposed-only early purge fraction, broken down by series

Designed to run on Ubuntu after copying CSVs from the server. Depends only on
pandas / numpy / matplotlib.
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

PALETTE = {
    "proposed": "#3b82f6",
    "baseline": "#ef4444",
    "neutral":  "#10b981",
    "muted":    "#6366f1",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--proposed", required=True, help="Path to proposed run output_counts CSV.")
    p.add_argument("--baseline", required=True, help="Path to baseline run output_counts CSV.")
    p.add_argument("--outdir",   required=True, help="Directory to write PNG files into.")
    return p.parse_args()


def load(csv_path: str, expected_mode: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = {"mode", "meterId", "seriesId", "windowStartMs", "windowEndMs", "outputCount", "earlyPurged"}
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"[error] {csv_path}: missing columns {sorted(missing)}")
    if df.empty:
        sys.exit(f"[error] {csv_path}: file has no data rows")
    actual_modes = set(df["mode"].astype(str).str.upper().unique())
    if expected_mode.upper() not in actual_modes:
        print(f"[warn] {csv_path}: expected mode '{expected_mode}' but file contains modes {sorted(actual_modes)}",
              file=sys.stderr)
    return df


def plot_totals(proposed: pd.DataFrame, baseline: pd.DataFrame, outpath: str) -> None:
    p_total = int(proposed["outputCount"].sum())
    b_total = int(baseline["outputCount"].sum())
    ratio = (b_total / p_total) if p_total > 0 else float("inf")

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(
        ["Proposed", "Baseline"],
        [p_total, b_total],
        color=[PALETTE["proposed"], PALETTE["baseline"]],
        edgecolor="black",
        linewidth=0.6,
    )
    for bar, value in zip(bars, [p_total, b_total]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{value:,}",
                ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_ylabel("Total window outputs")
    ax.set_title(f"Total downstream outputs (baseline / proposed = {ratio:.2f}×)")
    ax.grid(True, axis="y", alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_per_window_distribution(proposed: pd.DataFrame, baseline: pd.DataFrame, outpath: str) -> None:
    p = proposed["outputCount"].to_numpy()
    b = baseline["outputCount"].to_numpy()
    upper = int(np.percentile(np.concatenate([p, b]), 99))
    upper = max(upper, 5)
    bins = np.arange(0, upper + 2) - 0.5

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].hist(p, bins=bins, alpha=0.55, color=PALETTE["proposed"], label="Proposed", edgecolor="black", linewidth=0.4)
    axes[0].hist(b, bins=bins, alpha=0.55, color=PALETTE["baseline"], label="Baseline", edgecolor="black", linewidth=0.4)
    for series, color, label in [(p, PALETTE["proposed"], "Proposed"), (b, PALETTE["baseline"], "Baseline")]:
        median = np.median(series)
        p95 = np.percentile(series, 95)
        axes[0].axvline(median, color=color, linestyle="--", linewidth=1.2, alpha=0.9,
                        label=f"{label} median={median:.1f}")
        axes[0].axvline(p95, color=color, linestyle=":", linewidth=1.2, alpha=0.9,
                        label=f"{label} P95={p95:.1f}")
    axes[0].set_xlabel("Outputs per window")
    axes[0].set_ylabel("Window count")
    axes[0].set_title("Outputs per window — distribution")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(fontsize=8, loc="upper right")

    for series, color, label in [(p, PALETTE["proposed"], "Proposed"), (b, PALETTE["baseline"], "Baseline")]:
        sv = np.sort(series)
        cdf = np.arange(1, len(sv) + 1) / len(sv)
        axes[1].plot(sv, cdf, color=color, linewidth=1.8, label=label)
    axes[1].set_xlabel("Outputs per window")
    axes[1].set_ylabel("Cumulative fraction of windows")
    axes[1].set_xlim(left=0, right=upper + 1)
    axes[1].yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    axes[1].set_title("Outputs per window — CDF")
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(fontsize=9, loc="lower right")

    fig.suptitle("Per-window output count: proposed vs baseline", fontsize=13)
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_early_purge_ratio(proposed: pd.DataFrame, outpath: str) -> None:
    overall = float(proposed["earlyPurged"].mean())
    by_series = (
        proposed.groupby("seriesId")["earlyPurged"]
        .mean()
        .sort_index()
    )

    labels = ["Overall"] + by_series.index.tolist()
    values = [overall] + by_series.values.tolist()
    colors = [PALETTE["neutral"]] + [PALETTE["muted"]] * len(by_series)

    fig, ax = plt.subplots(figsize=(max(7, 1.2 * len(labels) + 4), 5))
    bars = ax.bar(labels, values, color=colors, edgecolor="black", linewidth=0.6)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{value*100:.1f}%",
                ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    ax.set_ylabel("Fraction of proposed windows purged before deadline")
    ax.set_title(f"Early-purge ratio — overall {overall*100:.1f}% across {len(proposed):,} windows")
    ax.grid(True, axis="y", alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")

    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    proposed = load(args.proposed, "PROPOSED")
    baseline = load(args.baseline, "BASELINE")

    plot_totals(proposed, baseline, os.path.join(args.outdir, "output_count_totals.png"))
    plot_per_window_distribution(proposed, baseline, os.path.join(args.outdir, "outputs_per_window_dist.png"))
    plot_early_purge_ratio(proposed, os.path.join(args.outdir, "early_purge_ratio.png"))

    print(f"Wrote 3 PNGs to {args.outdir}")


if __name__ == "__main__":
    main()
