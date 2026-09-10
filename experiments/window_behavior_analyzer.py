#!/usr/bin/env python3
"""
Analyze window-level behavior across experiment runs.

Outputs are grouped into three categories:
  - latency: finalization latency and latency reduction
  - output: early purge and output count behavior
  - faulty: faulty windows and faulty meters

Edit EXPERIMENTS below when result folders or filenames change.
"""

import argparse
import math
import os
from collections import Counter
from typing import Dict, List, Optional

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import matplotlib.pyplot as plt
import pandas as pd

from method_labels import method_label, ordered_methods, sort_by_method_order

EXPERIMENTS = [
    {
        "label": "baseline1",
        "faulty": "analysis/results_run9_fast_0/native_faulty_window_counts_run9_fast_0.csv",
        "finalization": None,
        "output_counts": "analysis/results_run9_fast_0/output_counts_run9_fast_0.csv",
    },
    {
        "label": "baseline2",
        "faulty": "analysis/results_run9_delayed_726m/native_faulty_window_counts_run9_delayed_726m.csv",
        "finalization": None,
        "output_counts": "analysis/results_run9_delayed_726m/output_counts_run9_delayed_726m.csv",
    },
    {
        "label": "baseline3",
        "faulty": "analysis/results_run9_baseline_726m/native_faulty_window_counts_run9_baseline_726m.csv",
        "finalization": None,
        "output_counts": "analysis/results_run9_baseline_726m/output_counts_run9_baseline_726m.csv",
    },
    {
        "label": "proposed",
        "faulty": "analysis/results_run9_proposed_726m/faulty_window_counts_run9_proposed_726m.csv",
        "finalization": "analysis/results_run9_proposed_726m/finalization_latency_metrics_run9_proposed_726m.csv",
        "output_counts": "analysis/results_run9_proposed_726m/output_counts_run9_proposed_726m.csv",
    },
]

RUN_ID = "run9"
DEFAULT_OUTPUT_DIR = "analysis/window_behavior_run9"
SINGLE_PHASE_EXPECTED_SLOTS = 8
THREE_PHASE_EXPECTED_SLOTS = 24
THREE_PHASE_SERIES_LABEL = "Spänning L1/L2/L3"
COMPLETENESS_THRESHOLD = 0.75
METHOD_COLORS = {
    "SW": "#1f77b4",
    "DWW": "#ff7f0e",
    "ALW": "#2ca02c",
    "CBW": "#d62728",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze faulty windows, finalization latency, and output counts across experiments.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output root directory."
    )
    parser.add_argument(
        "--run-id",
        default=RUN_ID,
        help="Optional run identifier appended to output filenames.",
    )
    parser.add_argument(
        "--chunksize", type=int, default=500000, help="Rows per chunk for large CSVs."
    )
    parser.add_argument(
        "--hist-sample-size",
        type=int,
        default=200000,
        help="Max samples per experiment for distribution plots.",
    )
    return parser.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def existing_path(path: Optional[str]) -> Optional[str]:
    if path and os.path.exists(path):
        return path
    return None


def safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


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


def output_name(base_name: str, run_id: str) -> str:
    if not run_id:
        return base_name
    stem, ext = os.path.splitext(base_name)
    safe_run_id = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in run_id
    )
    return "{}_{}{}".format(stem, safe_run_id, ext)


def save_bar(
    path: str,
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    ylabel: str,
    color: str = "#1f77b4",
) -> None:
    if df.empty:
        return
    plot_df = sort_by_method_order(df, x_col).copy()
    display_x_col = "__display_x"
    plot_df[display_x_col] = plot_df[x_col].map(method_label)
    colors = [METHOD_COLORS.get(label, color) for label in plot_df[display_x_col]]
    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    bars = ax.bar(plot_df[display_x_col], plot_df[y_col], color=colors, width=0.65)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.35)
    ax.tick_params(axis="x", rotation=18)
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
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def save_table(path: str, df: pd.DataFrame, title: str) -> None:
    if df.empty:
        return
    fig_height = max(2.4, 0.62 * (len(df) + 1))
    fig_width = max(11.5, 1.55 * len(df.columns))
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.axis("off")
    ax.set_title(title, fontsize=14, pad=10)
    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1.05, 1.55)
    for (row, _col), cell in table.get_celld().items():
        cell.set_edgecolor("#cccccc")
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#eeeeee")
        else:
            cell.set_facecolor("#ffffff" if row % 2 else "#f8f8f8")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def analyze_latency(
    experiments: List[dict],
    output_dir: str,
    chunksize: int,
    sample_size: int,
    run_id: str,
) -> None:
    ensure_dir(output_dir)
    rows = []
    samples = []

    columns = [
        "finalizationLatencyMs",
        "latencyReductionMs",
        "allowedLatenessMs",
        "faultIndicator",
        "seriesId",
    ]

    for experiment in experiments:
        label = experiment["label"]
        path = existing_path(experiment.get("finalization"))
        if not path:
            continue

        numeric_chunks = []
        sample_chunks = []
        seen = 0
        for chunk in pd.read_csv(
            path, usecols=lambda col: col in columns, chunksize=chunksize
        ):
            for col in (
                "finalizationLatencyMs",
                "latencyReductionMs",
                "allowedLatenessMs",
            ):
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
            clean = chunk.dropna(
                subset=[
                    "finalizationLatencyMs",
                    "latencyReductionMs",
                    "allowedLatenessMs",
                ]
            ).copy()
            if clean.empty:
                continue
            numeric_chunks.append(
                clean[
                    ["finalizationLatencyMs", "latencyReductionMs", "allowedLatenessMs"]
                ]
            )
            remaining = max(0, sample_size - seen)
            if remaining:
                take = min(remaining, len(clean))
                sample_chunks.append(clean.head(take).copy())
                seen += take

        if not numeric_chunks:
            continue

        data = pd.concat(numeric_chunks, ignore_index=True)
        latency_min = data["finalizationLatencyMs"] / 60000.0
        reduction_min = data["latencyReductionMs"] / 60000.0
        reduction_ratio = data["latencyReductionMs"] / data[
            "allowedLatenessMs"
        ].replace(0, pd.NA)

        rows.append(
            {
                "experiment": label,
                "finalized_windows": len(data),
                "finalization_latency_mean_min": latency_min.mean(),
                "finalization_latency_median_min": latency_min.median(),
                "finalization_latency_p95_min": latency_min.quantile(0.95),
                "latency_reduction_mean_min": reduction_min.mean(),
                "latency_reduction_median_min": reduction_min.median(),
                "latency_reduction_p95_min": reduction_min.quantile(0.95),
                "latency_reduction_mean_ratio": reduction_ratio.mean(),
                "latency_reduction_p95_ratio": reduction_ratio.quantile(0.95),
            }
        )

        if sample_chunks:
            sample = pd.concat(sample_chunks, ignore_index=True)
            sample["experiment"] = label
            sample["latency_reduction_min"] = sample["latencyReductionMs"] / 60000.0
            sample["finalization_latency_min"] = (
                sample["finalizationLatencyMs"] / 60000.0
            )
            samples.append(sample)

    summary = sort_by_method_order(pd.DataFrame(rows), "experiment")
    summary.to_csv(
        os.path.join(output_dir, output_name("latency__summary.csv", run_id)),
        index=False,
    )
    if summary.empty:
        return

    summary = sort_by_method_order(summary, "experiment")
    table_df = pd.DataFrame(
        {
            "Experiment": summary["experiment"].map(method_label),
            "Windows": summary["finalized_windows"].map(
                lambda value: "{:,}".format(int(value))
            ),
            "Final mean\n(min)": summary["finalization_latency_mean_min"].map(
                lambda value: "{:.1f}".format(value)
            ),
            "Final P95\n(min)": summary["finalization_latency_p95_min"].map(
                lambda value: "{:.1f}".format(value)
            ),
            "Reduction mean\n(min)": summary["latency_reduction_mean_min"].map(
                lambda value: "{:.1f}".format(value)
            ),
            "Reduction P95\n(min)": summary["latency_reduction_p95_min"].map(
                lambda value: "{:.1f}".format(value)
            ),
            "Reduction mean\n(%)": (
                summary["latency_reduction_mean_ratio"] * 100.0
            ).map(lambda value: "{:.1f}".format(value)),
        }
    )
    save_table(
        os.path.join(
            output_dir, output_name("latency__summary_table_by_experiment.png", run_id)
        ),
        table_df,
        "Latency Summary",
    )

    save_bar(
        os.path.join(
            output_dir,
            output_name("latency__finalization_latency_mean_by_experiment.png", run_id),
        ),
        summary,
        "experiment",
        "finalization_latency_mean_min",
        "Mean Finalization Latency",
        "Minutes",
        "#4c78a8",
    )
    save_bar(
        os.path.join(
            output_dir,
            output_name("latency__latency_reduction_mean_by_experiment.png", run_id),
        ),
        summary,
        "experiment",
        "latency_reduction_mean_min",
        "Mean Latency Reduction",
        "Minutes",
        "#54a24b",
    )
    save_bar(
        os.path.join(
            output_dir,
            output_name("latency__latency_reduction_ratio_by_experiment.png", run_id),
        ),
        summary.assign(
            latency_reduction_mean_percent=summary["latency_reduction_mean_ratio"]
            * 100.0
        ),
        "experiment",
        "latency_reduction_mean_percent",
        "Mean Latency Reduction Ratio",
        "Percent of allowed lateness",
        "#f58518",
    )

    if samples:
        sample_df = pd.concat(samples, ignore_index=True)
        fig, ax = plt.subplots(figsize=(7.2, 4.8))
        colors = {"proposed": "#0f766e"}
        for label in ordered_methods(sample_df["experiment"].dropna().unique()):
            group = sample_df[sample_df["experiment"] == label]
            ax.hist(
                group["latency_reduction_min"],
                bins=50,
                alpha=0.88,
                color=colors.get(label, "#374151"),
                edgecolor="white",
                linewidth=0.35,
                label=method_label(label),
            )
        ax.set_title("Latency Reduction Distribution")
        ax.set_xlabel("Latency reduction (min)")
        ax.set_ylabel("Windows")
        ax.grid(axis="y", alpha=0.35)
        ax.legend()
        fig.tight_layout()
        fig.savefig(
            os.path.join(
                output_dir,
                output_name(
                    "latency__latency_reduction_distribution_by_experiment.png", run_id
                ),
            ),
            dpi=180,
            bbox_inches="tight",
            pad_inches=0.03,
        )
        plt.close(fig)


def analyze_output_counts(
    experiments: List[dict], output_dir: str, chunksize: int, run_id: str
) -> None:
    ensure_dir(output_dir)
    rows = []
    distributions = []
    key_cols = ["mode", "meterId", "seriesId", "windowStartMs", "windowEndMs"]
    metric_cols = ["outputCount", "earlyPurged", "finalCount", "distinctSlotCount"]

    for experiment in experiments:
        label = experiment["label"]
        path = existing_path(experiment.get("output_counts"))
        finalization_path = existing_path(experiment.get("finalization"))
        if not path and finalization_path:
            rows_for_experiment, distributions_for_experiment = (
                summarize_outputs_from_finalization(
                    label,
                    finalization_path,
                    chunksize,
                )
            )
            rows.append(rows_for_experiment)
            distributions.extend(distributions_for_experiment)
            continue
        if not path:
            continue

        emitted_windows = 0
        total_outputs = 0
        missing_metadata_outputs = 0
        early_purged = 0
        duplicate_window_rows = 0
        invalid_window_rows = 0
        invalid_window_keys = set()
        seen_window_keys = set()
        output_dist = Counter()
        final_count_values = []
        distinct_slot_values = []

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
                missing_metadata_outputs += int(invalid_output_count.sum())

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
                pd.to_numeric(chunk["outputCount"], errors="coerce")
                .fillna(0)
                .astype("int64")
            )
            output_count = output_count.loc[unique_mask]
            final_count = pd.to_numeric(unique_chunk["finalCount"], errors="coerce")
            semantic_final = semantic_final_mask(
                unique_chunk["seriesId"], unique_chunk["finalCount"]
            )
            early = (
                unique_chunk["earlyPurged"].astype(str).str.lower().eq("true")
                & semantic_final
            )
            distinct_slots = pd.to_numeric(
                unique_chunk["distinctSlotCount"], errors="coerce"
            )

            emitted_windows += len(unique_chunk)
            total_outputs += int(output_count.sum())
            early_purged += int(early.sum())
            output_dist.update(output_count.tolist())
            final_count_values.append(final_count.dropna())
            distinct_slot_values.append(distinct_slots.dropna())

        final_count_all = (
            pd.concat(final_count_values, ignore_index=True)
            if final_count_values
            else pd.Series(dtype=float)
        )
        distinct_slot_all = (
            pd.concat(distinct_slot_values, ignore_index=True)
            if distinct_slot_values
            else pd.Series(dtype=float)
        )
        rows.append(
            {
                "experiment": label,
                "emitted_windows": emitted_windows,
                "duplicate_window_rows": duplicate_window_rows,
                "invalid_window_rows": invalid_window_rows,
                "invalid_unique_windows": len(invalid_window_keys),
                "early_purged_windows": early_purged,
                "early_purge_ratio": safe_div(early_purged, emitted_windows),
                "valid_window_outputs": total_outputs,
                "missing_window_metadata_outputs": missing_metadata_outputs,
                "total_outputs": total_outputs + missing_metadata_outputs,
                "mean_output_count": safe_div(
                    total_outputs + missing_metadata_outputs, emitted_windows
                ),
                "final_count_mean": final_count_all.mean(),
                "final_count_p95": final_count_all.quantile(0.95),
                "distinct_slot_count_mean": distinct_slot_all.mean(),
                "distinct_slot_count_p95": distinct_slot_all.quantile(0.95),
            }
        )
        for output_count, count in sorted(output_dist.items()):
            distributions.append(
                {
                    "experiment": label,
                    "outputCount": output_count,
                    "windows": count,
                }
            )

    summary = pd.DataFrame(rows)
    dist_df = pd.DataFrame(distributions)
    if not summary.empty:
        baseline_rows = summary[
            summary["experiment"]
            .astype(str)
            .str.contains("baseline", case=False, regex=False)
        ]
        if baseline_rows.empty:
            reference_row = summary.loc[summary["emitted_windows"].idxmax()]
        else:
            reference_row = baseline_rows.loc[baseline_rows["emitted_windows"].idxmax()]
        reference_windows = int(reference_row["emitted_windows"])
        reference_label = str(reference_row["experiment"])
        summary["reference_window_experiment"] = reference_label
        summary["reference_emitted_windows"] = reference_windows
        summary["emitted_window_coverage_ratio"] = summary["emitted_windows"].map(
            lambda value: safe_div(value, reference_windows)
        )

    summary = sort_by_method_order(summary, "experiment")
    dist_df = sort_by_method_order(dist_df, "experiment")
    summary.to_csv(
        os.path.join(output_dir, output_name("output__summary.csv", run_id)),
        index=False,
    )
    dist_df.to_csv(
        os.path.join(
            output_dir, output_name("output__output_count_distribution.csv", run_id)
        ),
        index=False,
    )
    if summary.empty:
        return

    save_bar(
        os.path.join(
            output_dir, output_name("output__emitted_windows_by_experiment.png", run_id)
        ),
        summary.assign(emitted_windows_m=summary["emitted_windows"] / 1_000_000),
        "experiment",
        "emitted_windows_m",
        "Emitted Windows",
        "Windows (millions)",
        "#4c78a8",
    )
    save_bar(
        os.path.join(
            output_dir,
            output_name("output__emitted_window_coverage_by_experiment.png", run_id),
        ),
        summary.assign(
            emitted_window_coverage_percent=summary["emitted_window_coverage_ratio"]
            * 100.0
        ),
        "experiment",
        "emitted_window_coverage_percent",
        "Emitted Window Coverage",
        "Percent of ALW emitted windows",
        "#72b7b2",
    )
    save_bar(
        os.path.join(
            output_dir,
            output_name("output__early_purge_ratio_by_experiment.png", run_id),
        ),
        summary.assign(early_purge_percent=summary["early_purge_ratio"] * 100.0),
        "experiment",
        "early_purge_percent",
        "Early Purge Ratio",
        "Percent",
        "#f58518",
    )
    save_bar(
        os.path.join(
            output_dir, output_name("output__total_outputs_by_experiment.png", run_id)
        ),
        summary.assign(total_outputs_m=summary["total_outputs"] / 1_000_000),
        "experiment",
        "total_outputs_m",
        "Total Outputs",
        "Outputs (millions)",
        "#54a24b",
    )
    save_bar(
        os.path.join(
            output_dir,
            output_name("output__mean_output_count_by_experiment.png", run_id),
        ),
        summary,
        "experiment",
        "mean_output_count",
        "Mean Output Count per Window",
        "Outputs/window",
        "#b279a2",
    )

    if not dist_df.empty:
        pivot = dist_df.pivot_table(
            index="outputCount",
            columns="experiment",
            values="windows",
            aggfunc="sum",
            fill_value=0,
        ).sort_index()
        ordered_columns = [
            label for label in ordered_methods(pivot.columns) if label in pivot.columns
        ]
        pivot = pivot[ordered_columns].rename(columns=method_label)
        pivot = pivot / 1_000_000
        fig, ax = plt.subplots(figsize=(7.2, 4.8))
        pivot.plot(kind="bar", ax=ax)
        ax.set_title("Output Count Distribution")
        ax.set_xlabel("outputCount")
        ax.set_ylabel("Windows (millions)")
        ax.grid(axis="y", alpha=0.35)
        fig.tight_layout()
        fig.savefig(
            os.path.join(
                output_dir,
                output_name(
                    "output__output_count_distribution_by_experiment.png", run_id
                ),
            ),
            dpi=180,
            bbox_inches="tight",
            pad_inches=0.03,
        )
        plt.close(fig)


def summarize_outputs_from_finalization(label: str, path: str, chunksize: int) -> tuple:
    key_cols = ["mode", "meterId", "seriesId", "windowStartMs", "windowEndMs"]
    numeric_cols = ["count"]
    optional_cols = ["finalizationReason"]

    emitted_windows = 0
    total_outputs = 0
    early_purged = 0
    duplicate_window_rows = 0
    seen_window_keys = set()
    output_dist = Counter()
    final_count_values = []

    usecols = lambda col: col in set(key_cols + numeric_cols + optional_cols)
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
        preliminary = preliminary_mask(unique_chunk["seriesId"], final_count)
        output_count = 1 + preliminary.astype("int64")
        early = early_purge_mask_from_finalization(unique_chunk)

        emitted_windows += len(unique_chunk)
        total_outputs += int(output_count.sum())
        early_purged += int(early.sum())
        output_dist.update(output_count.tolist())
        final_count_values.append(final_count.dropna())

    final_count_all = (
        pd.concat(final_count_values, ignore_index=True)
        if final_count_values
        else pd.Series(dtype=float)
    )
    row = {
        "experiment": label,
        "emitted_windows": emitted_windows,
        "duplicate_window_rows": duplicate_window_rows,
        "invalid_window_rows": 0,
        "invalid_unique_windows": 0,
        "early_purged_windows": early_purged,
        "early_purge_ratio": safe_div(early_purged, emitted_windows),
        "valid_window_outputs": total_outputs,
        "missing_window_metadata_outputs": 0,
        "total_outputs": total_outputs,
        "mean_output_count": safe_div(total_outputs, emitted_windows),
        "final_count_mean": final_count_all.mean(),
        "final_count_p95": final_count_all.quantile(0.95),
        "distinct_slot_count_mean": pd.NA,
        "distinct_slot_count_p95": pd.NA,
        "output_count_source": "inferred_from_finalization",
    }
    distributions = [
        {
            "experiment": label,
            "outputCount": output_count,
            "windows": count,
        }
        for output_count, count in sorted(output_dist.items())
    ]
    return row, distributions


def analyze_faulty_windows(
    experiments: List[dict], output_dir: str, run_id: str
) -> None:
    ensure_dir(output_dir)
    rows = []
    daily_frames = []

    for experiment in experiments:
        label = experiment["label"]
        path = existing_path(experiment.get("faulty"))
        if not path:
            continue

        df = pd.read_csv(path)
        if df.empty:
            continue
        required = {"meterId", "seriesId", "windowStart"}
        missing = required - set(df.columns)
        if missing:
            raise SystemExit(
                "{} is missing required columns: {}".format(
                    path, ", ".join(sorted(missing))
                )
            )

        df["windowStart"] = pd.to_datetime(df["windowStart"], errors="coerce")
        df["meterId"] = df["meterId"].astype(str)
        unique_faulty = df["meterId"].nunique()
        single = df[df["seriesId"].astype(str).eq("Spänning L1")]["meterId"].nunique()
        three = df[df["seriesId"].astype(str).str.contains("L1/L2/L3", regex=False)][
            "meterId"
        ].nunique()
        rows.append(
            {
                "experiment": label,
                "faulty_windows": len(df),
                "unique_faulty_meters": unique_faulty,
                "single_phase_unique_faulty_meters": single,
                "three_phase_unique_faulty_meters": three,
                "faulty_windows_per_meter_mean": safe_div(len(df), unique_faulty),
                "faulty_windows_per_meter_p95": df.groupby("meterId")
                .size()
                .quantile(0.95),
            }
        )

        top = (
            df.groupby(["meterId", "seriesId"])
            .size()
            .reset_index(name="faulty_windows")
            .sort_values("faulty_windows", ascending=False)
            .head(50)
        )
        top.to_csv(
            os.path.join(
                output_dir,
                output_name("faulty__top_faulty_meters__{}.csv".format(label), run_id),
            ),
            index=False,
        )

        daily = (
            df.dropna(subset=["windowStart"])
            .assign(day=lambda data: data["windowStart"].dt.date)
            .groupby("day")
            .size()
            .reset_index(name="faulty_windows")
        )
        daily["experiment"] = label
        daily_frames.append(daily)

    summary = pd.DataFrame(rows)
    summary.to_csv(
        os.path.join(output_dir, output_name("faulty__summary.csv", run_id)),
        index=False,
    )
    if summary.empty:
        return

    save_bar(
        os.path.join(
            output_dir, output_name("faulty__faulty_windows_by_experiment.png", run_id)
        ),
        summary,
        "experiment",
        "faulty_windows",
        "Faulty Windows",
        "Faulty windows",
        "#e45756",
    )
    save_bar(
        os.path.join(
            output_dir,
            output_name("faulty__unique_faulty_meters_by_experiment.png", run_id),
        ),
        summary,
        "experiment",
        "unique_faulty_meters",
        "Unique Faulty Meters",
        "Meters",
        "#4c78a8",
    )

    phase = (
        summary[
            [
                "experiment",
                "single_phase_unique_faulty_meters",
                "three_phase_unique_faulty_meters",
            ]
        ]
        .rename(
            columns={
                "single_phase_unique_faulty_meters": "Single-phase",
                "three_phase_unique_faulty_meters": "Three-phase",
            }
        )
        .set_index("experiment")
    )
    phase.index = [method_label(label) for label in phase.index]
    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    phase.plot(kind="bar", ax=ax)
    ax.set_title("Single vs Three Phase Faulty Meters")
    ax.set_ylabel("Unique faulty meters")
    ax.set_xlabel("Experiment")
    ax.tick_params(axis="x", rotation=0)
    ax.set_yscale("log")
    ax.set_ylim(bottom=0.8)
    ax.grid(axis="y", alpha=0.35)
    ax.legend(loc="upper left", frameon=True)
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            if height > 0:
                ax.annotate(
                    "{:.0f}".format(height),
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 4),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )
    fig.tight_layout()
    fig.savefig(
        os.path.join(
            output_dir,
            output_name(
                "faulty__single_vs_three_phase_faulty_meters_by_experiment.png", run_id
            ),
        ),
        dpi=180,
        bbox_inches="tight",
        pad_inches=0.03,
    )
    plt.close(fig)

    if daily_frames:
        daily_all = pd.concat(daily_frames, ignore_index=True)
        daily_all.to_csv(
            os.path.join(
                output_dir, output_name("faulty__faulty_windows_over_time.csv", run_id)
            ),
            index=False,
        )
        fig, ax = plt.subplots(figsize=(8.2, 4.8))
        for label in ordered_methods(daily_all["experiment"].dropna().unique()):
            group = daily_all[daily_all["experiment"] == label]
            ax.plot(
                pd.to_datetime(group["day"]),
                group["faulty_windows"],
                marker="o",
                linewidth=1.6,
                label=method_label(label),
            )
        ax.set_title("Faulty Windows over Time")
        ax.set_xlabel("Window day")
        ax.set_ylabel("Faulty windows")
        ax.grid(True, alpha=0.35)
        ax.legend()
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(
            os.path.join(
                output_dir,
                output_name(
                    "faulty__faulty_windows_over_time_by_experiment.png", run_id
                ),
            ),
            dpi=180,
            bbox_inches="tight",
            pad_inches=0.03,
        )
        plt.close(fig)


def main() -> None:
    args = parse_args()
    latency_dir = os.path.join(args.output_dir, "latency")
    output_dir = os.path.join(args.output_dir, "output")
    faulty_dir = os.path.join(args.output_dir, "faulty")
    ensure_dir(args.output_dir)

    analyze_latency(
        EXPERIMENTS, latency_dir, args.chunksize, args.hist_sample_size, args.run_id
    )
    analyze_output_counts(EXPERIMENTS, output_dir, args.chunksize, args.run_id)
    analyze_faulty_windows(EXPERIMENTS, faulty_dir, args.run_id)

    print("Wrote latency analysis under: {}".format(latency_dir))
    print("Wrote output analysis under: {}".format(output_dir))
    print("Wrote faulty analysis under: {}".format(faulty_dir))


if __name__ == "__main__":
    main()
