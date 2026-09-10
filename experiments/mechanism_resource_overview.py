#!/usr/bin/env python3
"""
Create separate overview figures for:
  1. Total emitted windows by method.
  2. Total output emissions by method.
  3. CPU usage over normalized runtime by method.
"""

import argparse
import math
import os
from dataclasses import dataclass

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import matplotlib.pyplot as plt
import pandas as pd

from method_labels import method_label, ordered_methods, sort_by_method_order

DEFAULT_EXPERIMENTS = [
    {
        "key": "baseline1",
        "output_counts": "analysis/results_run9_fast_0/output_counts_run9_fast_0.csv",
        "system_metrics": "analysis/results_run9_fast_0/system_metrics_fast_run9.csv",
        "finalization": None,
    },
    {
        "key": "baseline2",
        "output_counts": "analysis/results_run9_delayed_726m/output_counts_run9_delayed_726m.csv",
        "system_metrics": "analysis/results_run9_delayed_726m/system_metrics_delayed_run9.csv",
        "finalization": None,
    },
    {
        "key": "baseline3",
        "output_counts": "analysis/results_run9_baseline_726m/output_counts_run9_baseline_726m.csv",
        "system_metrics": "analysis/results_run9_baseline_726m/system_metrics_baseline_run9.csv",
        "finalization": None,
    },
    {
        "key": "proposed",
        "output_counts": "analysis/results_run9_proposed_726m/output_counts_run9_proposed_726m.csv",
        "system_metrics": "analysis/results_run9_proposed_726m/system_metrics_proposed_run9.csv",
        "finalization": "analysis/results_run9_proposed_726m/finalization_latency_metrics_run9_proposed_726m.csv",
    },
]
DEFAULT_OUTPUT_DIR = "analysis/mechanism_resource_overview_726m"
RUN_ID = "run9"
SINGLE_PHASE_EXPECTED_SLOTS = 8
THREE_PHASE_EXPECTED_SLOTS = 24
THREE_PHASE_SERIES_LABEL = "Spänning L1/L2/L3"
COMPLETENESS_THRESHOLD = 0.75


@dataclass
class OutputCountSummary:
    emitted_windows: int
    total_outputs: int
    valid_window_outputs: int
    missing_window_metadata_outputs: int
    early_purged_windows: int
    duplicate_window_rows: int
    invalid_window_rows: int
    invalid_unique_windows: int


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot method-level window, output, and CPU comparisons across all four methods.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sw-output-counts",
        default=DEFAULT_EXPERIMENTS[0]["output_counts"],
        help="SW output_counts CSV.",
    )
    parser.add_argument(
        "--dww-output-counts",
        default=DEFAULT_EXPERIMENTS[1]["output_counts"],
        help="DWW output_counts CSV.",
    )
    parser.add_argument(
        "--alw-output-counts",
        "--baseline-output-counts",
        default=DEFAULT_EXPERIMENTS[2]["output_counts"],
        help="ALW output_counts CSV.",
    )
    parser.add_argument(
        "--cbw-output-counts",
        "--proposed-output-counts",
        default=DEFAULT_EXPERIMENTS[3]["output_counts"],
        help="CBW output_counts CSV.",
    )
    parser.add_argument(
        "--cbw-finalization",
        "--proposed-finalization",
        default=DEFAULT_EXPERIMENTS[3]["finalization"],
        help="CBW finalization latency CSV used when output_counts is absent.",
    )
    parser.add_argument(
        "--sw-csv",
        default=DEFAULT_EXPERIMENTS[0]["system_metrics"],
        help="SW system metrics CSV.",
    )
    parser.add_argument(
        "--dww-csv",
        default=DEFAULT_EXPERIMENTS[1]["system_metrics"],
        help="DWW system metrics CSV.",
    )
    parser.add_argument(
        "--alw-csv",
        "--baseline-csv",
        default=DEFAULT_EXPERIMENTS[2]["system_metrics"],
        help="ALW system metrics CSV.",
    )
    parser.add_argument(
        "--cbw-csv",
        "--proposed-csv",
        default=DEFAULT_EXPERIMENTS[3]["system_metrics"],
        help="CBW system metrics CSV.",
    )
    parser.add_argument(
        "--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory."
    )
    parser.add_argument(
        "--run-id",
        default=RUN_ID,
        help="Optional run identifier appended to output filenames.",
    )
    parser.add_argument(
        "--runtime-bin-percent",
        type=float,
        default=5.0,
        help="Runtime percentage bin size for CPU curve.",
    )
    parser.add_argument(
        "--cpu-stat",
        default="p95",
        choices=["median", "mean", "p95", "max"],
        help="CPU statistic per runtime bin.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500000,
        help="Rows per chunk when scanning output_counts CSVs.",
    )
    parser.add_argument(
        "--progress-every-chunks",
        type=int,
        default=5,
        help="Print progress after this many chunks while scanning large CSVs; set 0 to disable.",
    )
    return parser.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def output_name(base_name: str, run_id: str) -> str:
    if not run_id:
        return base_name
    stem, ext = os.path.splitext(base_name)
    safe_run_id = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in run_id
    )
    return "{}_{}{}".format(stem, safe_run_id, ext)


def semantic_final_mask(series_id: pd.Series, final_count: pd.Series) -> pd.Series:
    expected = (
        series_id.astype(str)
        .eq(THREE_PHASE_SERIES_LABEL)
        .map({True: THREE_PHASE_EXPECTED_SLOTS, False: SINGLE_PHASE_EXPECTED_SLOTS})
    )
    return pd.to_numeric(final_count, errors="coerce").fillna(0).ge(expected)


def expected_slots(series_id: pd.Series) -> pd.Series:
    return (
        series_id.astype(str)
        .eq(THREE_PHASE_SERIES_LABEL)
        .map({True: THREE_PHASE_EXPECTED_SLOTS, False: SINGLE_PHASE_EXPECTED_SLOTS})
        .astype("int64")
    )


def preliminary_mask(series_id: pd.Series, final_count: pd.Series) -> pd.Series:
    expected = expected_slots(series_id)
    preliminary_threshold = expected.map(
        lambda value: int(math.ceil(value * COMPLETENESS_THRESHOLD))
    )
    return (
        pd.to_numeric(final_count, errors="coerce").fillna(0).ge(preliminary_threshold)
    )


def early_purge_mask_from_finalization(chunk: pd.DataFrame) -> pd.Series:
    if "finalizationReason" in chunk.columns:
        reason = chunk["finalizationReason"].astype(str).str.upper()
        known_reason = reason.isin(["COMPLETENESS", "DEADLINE"])
        semantic_final = semantic_final_mask(chunk["seriesId"], chunk["count"])
        return reason.eq("COMPLETENESS") | (~known_reason & semantic_final)
    return semantic_final_mask(chunk["seriesId"], chunk["count"])


def print_scan_progress(label: str, path: str, chunks: int, rows: int) -> None:
    if not label:
        return
    print(
        "[{}] scanned {:,} rows from {} ({} chunks)".format(
            label,
            rows,
            os.path.basename(path),
            chunks,
        ),
        flush=True,
    )


def summarize_output_counts(
    path: str,
    chunksize: int,
    progress_label: str = "",
    progress_every_chunks: int = 0,
) -> OutputCountSummary:
    emitted_windows = 0
    total_outputs = 0
    missing_window_metadata_outputs = 0
    early_purged_windows = 0
    duplicate_window_rows = 0
    invalid_window_rows = 0
    invalid_window_keys = set()
    seen_window_keys = set()

    key_cols = ["mode", "meterId", "seriesId", "windowStartMs", "windowEndMs"]
    metric_cols = ["outputCount", "earlyPurged", "finalCount"]
    required = set(key_cols + metric_cols)
    rows_scanned = 0
    chunks_scanned = 0
    for chunk in pd.read_csv(
        path,
        usecols=key_cols + metric_cols,
        dtype={
            "mode": "string",
            "meterId": "string",
            "seriesId": "string",
            "earlyPurged": "string",
        },
        chunksize=chunksize,
    ):
        chunks_scanned += 1
        rows_scanned += len(chunk)
        if progress_every_chunks > 0 and chunks_scanned % progress_every_chunks == 0:
            print_scan_progress(progress_label, path, chunks_scanned, rows_scanned)

        missing = required - set(chunk.columns)
        if missing:
            raise SystemExit(
                "{} is missing required columns: {}".format(
                    path, ", ".join(sorted(missing))
                )
            )

        invalid_window = chunk["windowStartMs"].eq(0) & chunk["windowEndMs"].eq(0)
        invalid_window_rows += int(invalid_window.sum())
        if invalid_window.any():
            invalid_hash = pd.util.hash_pandas_object(
                chunk.loc[invalid_window, key_cols], index=False
            )
            invalid_window_keys.update(invalid_hash.astype("uint64").tolist())
            invalid_output_count = (
                pd.to_numeric(
                    chunk.loc[invalid_window, "outputCount"],
                    errors="coerce",
                )
                .fillna(0)
                .astype("int64")
            )
            missing_window_metadata_outputs += int(invalid_output_count.sum())

        chunk = chunk.loc[~invalid_window]
        if chunk.empty:
            continue

        key_hash = pd.util.hash_pandas_object(chunk[key_cols], index=False)
        unique_in_chunk = ~key_hash.duplicated()
        unseen = ~key_hash.isin(seen_window_keys)
        unique_mask = unique_in_chunk & unseen
        duplicate_window_rows += int((~unique_mask).sum())

        if not unique_mask.any():
            continue

        seen_window_keys.update(key_hash[unique_mask].astype("uint64").tolist())
        unique_chunk = chunk.loc[unique_mask]
        output_count = (
            pd.to_numeric(unique_chunk["outputCount"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        semantic_final = semantic_final_mask(
            unique_chunk["seriesId"], unique_chunk["finalCount"]
        )
        early = (
            unique_chunk["earlyPurged"].astype(str).str.lower().eq("true")
            & semantic_final
        )
        emitted_windows += len(unique_chunk)
        total_outputs += int(output_count.sum())
        early_purged_windows += int(early.sum())

    if progress_label:
        print_scan_progress(progress_label, path, chunks_scanned, rows_scanned)

    return OutputCountSummary(
        emitted_windows=emitted_windows,
        total_outputs=total_outputs + missing_window_metadata_outputs,
        valid_window_outputs=total_outputs,
        missing_window_metadata_outputs=missing_window_metadata_outputs,
        early_purged_windows=early_purged_windows,
        duplicate_window_rows=duplicate_window_rows,
        invalid_window_rows=invalid_window_rows,
        invalid_unique_windows=len(invalid_window_keys),
    )


def summarize_finalization_outputs(
    path: str,
    chunksize: int,
    progress_label: str = "",
    progress_every_chunks: int = 0,
) -> OutputCountSummary:
    emitted_windows = 0
    total_outputs = 0
    early_purged_windows = 0
    duplicate_window_rows = 0
    seen_window_keys = set()

    key_cols = ["mode", "meterId", "seriesId", "windowStartMs", "windowEndMs"]
    numeric_cols = ["count"]
    optional_cols = ["finalizationReason"]
    usecols = lambda col: col in set(key_cols + numeric_cols + optional_cols)

    rows_scanned = 0
    chunks_scanned = 0
    for chunk in pd.read_csv(
        path,
        usecols=usecols,
        dtype={
            "mode": "string",
            "meterId": "string",
            "seriesId": "string",
            "finalizationReason": "string",
        },
        chunksize=chunksize,
    ):
        chunks_scanned += 1
        rows_scanned += len(chunk)
        if progress_every_chunks > 0 and chunks_scanned % progress_every_chunks == 0:
            print_scan_progress(progress_label, path, chunks_scanned, rows_scanned)

        missing = set(key_cols + numeric_cols) - set(chunk.columns)
        if missing:
            raise SystemExit(
                "{} is missing required columns: {}".format(
                    path, ", ".join(sorted(missing))
                )
            )

        key_hash = pd.util.hash_pandas_object(chunk[key_cols], index=False)
        unique_in_chunk = ~key_hash.duplicated()
        unseen = ~key_hash.isin(seen_window_keys)
        unique_mask = unique_in_chunk & unseen
        duplicate_window_rows += int((~unique_mask).sum())
        if not unique_mask.any():
            continue

        seen_window_keys.update(key_hash[unique_mask].astype("uint64").tolist())
        unique_chunk = chunk.loc[unique_mask].copy()
        final_count = pd.to_numeric(unique_chunk["count"], errors="coerce").fillna(0)
        output_count = 1 + preliminary_mask(
            unique_chunk["seriesId"], final_count
        ).astype("int64")
        early = early_purge_mask_from_finalization(unique_chunk)

        emitted_windows += len(unique_chunk)
        total_outputs += int(output_count.sum())
        early_purged_windows += int(early.sum())

    if progress_label:
        print_scan_progress(progress_label, path, chunks_scanned, rows_scanned)

    return OutputCountSummary(
        emitted_windows=emitted_windows,
        total_outputs=total_outputs,
        valid_window_outputs=total_outputs,
        missing_window_metadata_outputs=0,
        early_purged_windows=early_purged_windows,
        duplicate_window_rows=duplicate_window_rows,
        invalid_window_rows=0,
        invalid_unique_windows=0,
    )


def load_cpu_runtime(path: str, bin_percent: float, stat: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["timestamp", "cpu_percent"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["cpu_percent"] = pd.to_numeric(df["cpu_percent"], errors="coerce")
    df = df.dropna(subset=["timestamp", "cpu_percent"]).sort_values("timestamp").copy()
    if df.empty:
        raise SystemExit("{} has no usable CPU samples".format(path))

    df["elapsed_sec"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    max_elapsed = df["elapsed_sec"].max()
    df["runtime_percent"] = (
        (df["elapsed_sec"] / max_elapsed) * 100.0 if max_elapsed > 0 else 0.0
    )
    df["runtime_bin"] = ((df["runtime_percent"] // bin_percent) * bin_percent).clip(
        upper=100
    )

    def quantile_95(series: pd.Series) -> float:
        return series.quantile(0.95)

    stat_funcs = {
        "median": "median",
        "mean": "mean",
        "p95": quantile_95,
        "max": "max",
    }
    return (
        df.groupby("runtime_bin")
        .agg(
            runtime_percent=("runtime_bin", "first"),
            cpu_usage=("cpu_percent", stat_funcs[stat]),
        )
        .reset_index(drop=True)
        .sort_values("runtime_percent")
    )


def build_experiments(args) -> list:
    return [
        {
            "key": "baseline1",
            "output_counts": args.sw_output_counts,
            "system_metrics": args.sw_csv,
            "finalization": None,
        },
        {
            "key": "baseline2",
            "output_counts": args.dww_output_counts,
            "system_metrics": args.dww_csv,
            "finalization": None,
        },
        {
            "key": "baseline3",
            "output_counts": args.alw_output_counts,
            "system_metrics": args.alw_csv,
            "finalization": None,
        },
        {
            "key": "proposed",
            "output_counts": args.cbw_output_counts,
            "system_metrics": args.cbw_csv,
            "finalization": args.cbw_finalization,
        },
    ]


def summarize_counts_for_experiment(
    experiment: dict,
    chunksize: int,
    progress_every_chunks: int,
) -> OutputCountSummary:
    output_counts = experiment.get("output_counts")
    finalization = experiment.get("finalization")
    progress_label = method_label(experiment["key"])
    if output_counts and os.path.exists(output_counts):
        print(
            "[{}] scanning output counts: {}".format(progress_label, output_counts),
            flush=True,
        )
        return summarize_output_counts(
            output_counts, chunksize, progress_label, progress_every_chunks
        )
    if finalization and os.path.exists(finalization):
        print(
            "[{}] scanning finalization metrics: {}".format(
                progress_label, finalization
            ),
            flush=True,
        )
        return summarize_finalization_outputs(
            finalization, chunksize, progress_label, progress_every_chunks
        )
    raise SystemExit(
        "Missing output data for {}: expected {}{}".format(
            method_label(experiment["key"]),
            output_counts,
            " or {}".format(finalization) if finalization else "",
        )
    )


def write_summary(path: str, count_rows: pd.DataFrame) -> None:
    count_rows.to_csv(path, index=False)


def annotate_bar(ax, bars) -> None:
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            "{:.2f}".format(height),
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )


def plot_bar_by_method(
    output_path: str,
    df: pd.DataFrame,
    value_col: str,
    title: str,
    ylabel: str,
    color: str,
) -> None:
    plot_df = sort_by_method_order(df, "method").copy()
    fig, ax = plt.subplots(figsize=(6.8, 4.8))
    fig.patch.set_facecolor("white")
    bars = ax.bar(plot_df["method_label"], plot_df[value_col], color=color, width=0.62)
    annotate_bar(ax, bars)
    ax.set_title(title, fontsize=14)
    ax.set_xlabel("Method")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.35)
    ax.set_axisbelow(True)
    ymax = plot_df[value_col].max()
    ax.set_ylim(0, ymax * 1.18 if ymax > 0 else 1)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def plot_cpu(
    output_path: str,
    cpu_by_method: dict,
    cpu_stat: str,
) -> None:
    fig, ax_cpu = plt.subplots(figsize=(6.8, 4.9))
    fig.patch.set_facecolor("white")
    colors = {
        "baseline1": "#4c78a8",
        "baseline2": "#72b7b2",
        "baseline3": "#f58518",
        "proposed": "#54a24b",
    }
    markers = {
        "baseline1": "o",
        "baseline2": "s",
        "baseline3": "^",
        "proposed": "D",
    }

    for method in ordered_methods(cpu_by_method.keys()):
        data = cpu_by_method[method]
        ax_cpu.plot(
            data["runtime_percent"],
            data["cpu_usage"],
            marker=markers.get(method, "o"),
            linewidth=1.9,
            markersize=4,
            color=colors.get(method, "#666666"),
            label=method_label(method),
        )
    ax_cpu.set_title(
        "CPU Usage over Runtime ({})".format(cpu_stat.upper()), fontsize=14
    )
    ax_cpu.set_xlabel("Runtime %")
    ax_cpu.set_ylabel("CPU Usage (%)")
    ax_cpu.set_xlim(0, 100)
    ax_cpu.set_xticks(range(0, 101, 10))
    ax_cpu.grid(True, alpha=0.35)
    ax_cpu.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, -0.23),
        ncol=min(len(cpu_by_method), 4),
        frameon=False,
    )

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    ensure_dir(args.output_dir)

    experiments = build_experiments(args)
    count_rows = []
    cpu_by_method = {}

    for experiment in experiments:
        method = experiment["key"]
        counts = summarize_counts_for_experiment(
            experiment, args.chunksize, args.progress_every_chunks
        )
        count_rows.append(
            {
                "method": method,
                "method_label": method_label(method),
                "emitted_windows": counts.emitted_windows,
                "emitted_windows_m": counts.emitted_windows / 1_000_000,
                "total_outputs": counts.total_outputs,
                "total_outputs_m": counts.total_outputs / 1_000_000,
                "valid_window_outputs": counts.valid_window_outputs,
                "missing_window_metadata_outputs": counts.missing_window_metadata_outputs,
                "early_purged_windows": counts.early_purged_windows,
                "early_purge_ratio": (
                    counts.early_purged_windows / counts.emitted_windows
                    if counts.emitted_windows
                    else 0
                ),
                "duplicate_window_rows": counts.duplicate_window_rows,
                "invalid_window_rows": counts.invalid_window_rows,
                "invalid_unique_windows": counts.invalid_unique_windows,
            }
        )

        system_metrics = experiment.get("system_metrics")
        if not system_metrics or not os.path.exists(system_metrics):
            raise SystemExit(
                "Missing CPU data for {}: {}".format(
                    method_label(method), system_metrics
                )
            )
        cpu_by_method[method] = load_cpu_runtime(
            system_metrics, args.runtime_bin_percent, args.cpu_stat
        )

    count_summary = sort_by_method_order(pd.DataFrame(count_rows), "method")

    summary_path = os.path.join(
        args.output_dir, output_name("mechanism_resource_summary.csv", args.run_id)
    )
    total_windows_plot_path = os.path.join(
        args.output_dir, output_name("total_windows_by_method.png", args.run_id)
    )
    total_outputs_plot_path = os.path.join(
        args.output_dir, output_name("total_outputs_by_method.png", args.run_id)
    )
    cpu_plot_path = os.path.join(
        args.output_dir, output_name("cpu_runtime_overview.png", args.run_id)
    )
    write_summary(summary_path, count_summary)
    plot_bar_by_method(
        total_windows_plot_path,
        count_summary,
        "emitted_windows_m",
        "Total Windows by Method",
        "Windows (millions)",
        "#4c78a8",
    )
    plot_bar_by_method(
        total_outputs_plot_path,
        count_summary,
        "total_outputs_m",
        "Total Outputs by Method",
        "Outputs (millions)",
        "#54a24b",
    )
    plot_cpu(cpu_plot_path, cpu_by_method, args.cpu_stat)

    for method, cpu in cpu_by_method.items():
        filename = "{}_cpu_runtime.csv".format(method_label(method).lower())
        cpu.assign(mode=method, method_label=method_label(method)).to_csv(
            os.path.join(args.output_dir, output_name(filename, args.run_id)),
            index=False,
        )

    print("Wrote summary: {}".format(summary_path))
    print("Wrote total windows plot: {}".format(total_windows_plot_path))
    print("Wrote total outputs plot: {}".format(total_outputs_plot_path))
    print("Wrote CPU plot: {}".format(cpu_plot_path))


if __name__ == "__main__":
    main()
