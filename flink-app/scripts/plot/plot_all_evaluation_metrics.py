#!/usr/bin/env python3
"""Plot dropped-record metrics for the Flink smart-meter experiments.

This reduced version only generates dropped_records.png.

It reads:
  run_summary.csv
      - mode
      - runId, optional
      - droppedRecords

  standard_throughput_overall_summary_run8.csv
      - mode
      - runId, optional
      - post_filter_total_events

The dropped-record ratio is computed as:
  droppedRecords / post_filter_total_events

Usage:
  python3 plot_all_evaluation_metrics.py --input-dir <copied/results> --outdir <plots>
"""

from __future__ import annotations

import argparse
import gc
import re
import sys
import traceback
from pathlib import Path
from typing import Iterable, Optional

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd


PALETTE = {
    "proposed": "#3b82f6",
    "baseline": "#ef4444",
    "fast": "#f59e0b",
    "delayed": "#10b981",
}
FALLBACK_COLOR = "#6366f1"

DISPLAY_NAME = {
    "proposed": "CBW",
    "baseline": "ALW",
    "fast": "SW",
    "delayed": "DWW",
}

# Display order: SW -> DWW -> ALW -> CBW
MODE_ORDER = ["fast", "delayed", "baseline", "proposed"]

MODE_FILENAME_PATTERN = re.compile(r"_(proposed|baseline|fast|delayed)_")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--input-dir",
        required=True,
        help="Directory scanned recursively for CSV files.",
    )
    p.add_argument(
        "--outdir",
        required=True,
        help="Directory to write PNG figures into.",
    )
    return p.parse_args()


def warn(msg: str) -> None:
    print(f"[warn] {msg}", file=sys.stderr)


def info(msg: str) -> None:
    print(msg, file=sys.stdout)


def display_name(mode: str) -> str:
    return DISPLAY_NAME.get(str(mode).lower(), str(mode))


def color_for(mode: str) -> str:
    return PALETTE.get(str(mode).lower(), FALLBACK_COLOR)


def discover(input_dir: Path, glob: str) -> list[Path]:
    """Recursively collect files matching glob. Deduplicate by absolute path."""
    seen: dict[str, Path] = {}
    for f in input_dir.rglob(glob):
        if f.is_file():
            seen.setdefault(str(f.resolve()), f)
    return sorted(seen.values())


def mode_from_filename(path: Path) -> Optional[str]:
    m = MODE_FILENAME_PATTERN.search(path.name)
    return m.group(1).lower() if m else None


def load_concat(
    files: Iterable[Path],
    add_mode_from_filename: bool = False,
    usecols: Optional[list[str]] = None,
    dtype: Optional[dict] = None,
) -> pd.DataFrame:
    """Read and concat CSVs, skipping empty/header-only files."""
    frames = []
    for f in files:
        effective_usecols = usecols
        effective_dtype = dtype
        try:
            header = pd.read_csv(f, nrows=0).columns.tolist()
        except Exception as e:
            warn(f"{f}: cannot read header ({e}) - skipped")
            continue

        if usecols is not None:
            effective_usecols = [c for c in usecols if c in header]
            if not effective_usecols:
                warn(f"{f}: none of {usecols} present - skipped")
                continue

        if dtype is not None:
            effective_dtype = {k: v for k, v in dtype.items() if k in header}

        try:
            df = pd.read_csv(
                f,
                low_memory=False,
                usecols=effective_usecols,
                dtype=effective_dtype,
            )
        except pd.errors.EmptyDataError:
            warn(f"{f}: empty CSV - skipped")
            continue

        if df.empty:
            warn(f"{f}: header-only - skipped")
            continue

        if add_mode_from_filename:
            df["mode"] = mode_from_filename(f) or "unknown"

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True, sort=False)
    del frames
    gc.collect()
    return out


def normalise_mode_column(df: pd.DataFrame) -> pd.DataFrame:
    if "mode" in df.columns:
        df["mode"] = df["mode"].astype(str).str.lower()
    return df


def save_fig(fig: plt.Figure, outdir: Path, name: str) -> None:
    path = outdir / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    info(f"  wrote {path}")


def plot_dropped_records(input_dir: Path, outdir: Path) -> None:
    summary_files = discover(input_dir, "run_summary.csv")
    if not summary_files:
        warn("no run_summary.csv files found - skipping dropped-records plot")
        return

    df = load_concat(
        summary_files,
        usecols=["mode", "runId", "droppedRecords"],
        dtype={"mode": "category"},
    )
    if df.empty:
        warn("run_summary.csv files were empty - skipping dropped-records plot")
        return

    df = normalise_mode_column(df)

    if "mode" not in df.columns or "droppedRecords" not in df.columns:
        warn("run_summary.csv missing mode or droppedRecords - skipping dropped-records plot")
        return

    denom_files = discover(input_dir, "standard_throughput_overall_summary_run8.csv")
    if not denom_files:
        warn("no standard_throughput_overall_summary_run8.csv found - skipping dropped-records plot")
        return

    denom = load_concat(
        denom_files,
        usecols=["mode", "runId", "post_filter_total_events"],
        dtype={"mode": "category"},
    )
    if denom.empty:
        warn("standard_throughput_overall_summary_run8.csv was empty - skipping dropped-records plot")
        return

    denom = normalise_mode_column(denom)

    if "mode" not in denom.columns or "post_filter_total_events" not in denom.columns:
        warn("standard_throughput_overall_summary_run8.csv missing mode or post_filter_total_events")
        return

    # Prefer exact run matching when both files have runId. Otherwise match by mode.
    join_cols = ["mode", "runId"] if "runId" in df.columns and "runId" in denom.columns else ["mode"]

    denom = denom[join_cols + ["post_filter_total_events"]].copy()
    denom["post_filter_total_events"] = pd.to_numeric(
        denom["post_filter_total_events"],
        errors="coerce",
    )
    denom = denom.dropna(subset=["post_filter_total_events"])
    denom = denom.groupby(join_cols, as_index=False)["post_filter_total_events"].sum()

    df = df.merge(denom, on=join_cols, how="left")
    df["droppedRecords"] = pd.to_numeric(df["droppedRecords"], errors="coerce")
    df["post_filter_total_events"] = pd.to_numeric(
        df["post_filter_total_events"],
        errors="coerce",
    )

    missing_denominator = df["post_filter_total_events"].isna().sum()
    if missing_denominator:
        warn(f"{missing_denominator} run_summary row(s) had no denominator match")

    df = df.dropna(subset=["droppedRecords", "post_filter_total_events"])
    df = df[df["post_filter_total_events"] > 0]

    if df.empty:
        warn("no valid dropped-record rows after joining denominator - skipping")
        return

    df["droppedRecordRatio"] = df["droppedRecords"] / df["post_filter_total_events"]

    order_index = {m: i for i, m in enumerate(MODE_ORDER)}
    df["__order"] = df["mode"].map(order_index).fillna(len(MODE_ORDER))
    df = (
        df.sort_values(["__order", "mode"])
        .drop(columns="__order")
        .reset_index(drop=True)
    )

    seen: dict[str, int] = {}
    labels = []
    mode_counts = df["mode"].astype(str).value_counts().to_dict()

    for _, row in df.iterrows():
        mode = str(row["mode"])
        seen[mode] = seen.get(mode, 0) + 1
        run = str(row.get("runId", "")).strip()

        if seen[mode] == 1 and mode_counts.get(mode, 0) == 1:
            labels.append(display_name(mode))
        else:
            suffix = run if run else f"#{seen[mode]}"
            labels.append(f"{display_name(mode)} ({suffix})")

    colors = [color_for(m) for m in df["mode"].astype(str).tolist()]
    totals = df["droppedRecords"].astype(int).values
    ratios_pct = (df["droppedRecordRatio"].astype(float) * 100.0).values

    fig, ax = plt.subplots(figsize=(max(8, 1.4 * len(df) + 3), 5))
    bars = ax.bar(labels, totals, color=colors, edgecolor="black", linewidth=0.6)

    for bar, count, pct in zip(bars, totals, ratios_pct):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{int(count):,} ({pct:.2f}%)",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    ax.set_ylabel("Dropped record count")
    ax.set_title("Dropped records per mode")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(True, axis="y", alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")

    save_fig(fig, outdir, "dropped_records.png")


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    outdir = Path(args.outdir).resolve()

    if not input_dir.is_dir():
        print(f"[error] --input-dir does not exist: {input_dir}", file=sys.stderr)
        return 2

    outdir.mkdir(parents=True, exist_ok=True)

    info(f"Scanning {input_dir} (recursive)")
    info(f"Writing PNGs into {outdir}")
    info("")
    info("==> dropped records")

    try:
        plot_dropped_records(input_dir, outdir)
    except Exception as e:
        print(f"[error] dropped-records plot failed: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
