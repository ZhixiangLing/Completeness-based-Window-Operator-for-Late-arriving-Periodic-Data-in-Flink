#!/usr/bin/env python3
"""
Plot the proposed method's window finalization latency distribution.

The input is the finalization_latency_metrics CSV emitted by the Flink job.
Latency is measured in event time as:

    finalizationLatencyMs = watermarkAtPurgeMs - windowEndMs

Outputs include histogram and ECDF figures plus CSV summaries that can be
quoted directly in analysis text.
"""

import argparse
import os
from typing import List, Optional

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Rectangle
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from method_labels import method_label

DEFAULT_INPUT = "analysis/results_run8_proposed_735m/finalization_latency_metrics_run8_proposed_735m.csv"
DEFAULT_OUTPUT_DIR = "analysis/window_behavior/latency_distribution_run8"
DEFAULT_RUN_ID = "run8"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create finalization latency distribution plots for the proposed method.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", default=DEFAULT_INPUT, help="Finalization latency metrics CSV."
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated plots and CSVs.",
    )
    parser.add_argument(
        "--run-id",
        default=DEFAULT_RUN_ID,
        help="Optional run identifier appended to output filenames.",
    )
    parser.add_argument(
        "--mode",
        default="PROPOSED",
        help="Mode filter when the CSV contains a mode column.",
    )
    parser.add_argument(
        "--result-type",
        default="FINAL",
        help="Result type filter when the CSV contains a resultType column.",
    )
    parser.add_argument(
        "--chunksize", type=int, default=500000, help="Rows per CSV chunk."
    )
    parser.add_argument(
        "--bin-minutes",
        type=float,
        default=15.0,
        help="Histogram bin width in minutes.",
    )
    parser.add_argument(
        "--max-minutes",
        type=float,
        default=None,
        help="Optional right edge for plots in minutes.",
    )
    parser.add_argument(
        "--deadline-tolerance-ms",
        type=int,
        default=1000,
        help="Rows within this many milliseconds of allowed lateness are counted as deadline finalizations.",
    )
    parser.add_argument(
        "--immediate-tolerance-ms",
        type=int,
        default=0,
        help="Rows at or below this latency are counted as immediate finalizations.",
    )
    parser.add_argument(
        "--sw-latency-min",
        type=float,
        default=0.0,
        help="Deterministic SW finalization latency shown as a reference band.",
    )
    parser.add_argument(
        "--dww-latency-min",
        type=float,
        default=0.0,
        help="Deterministic DWW finalization latency shown as a reference band.",
    )
    parser.add_argument(
        "--alw-latency-min",
        type=float,
        default=None,
        help="Deterministic ALW finalization latency shown as a reference band.",
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


def require_columns(path: str, required: List[str]) -> List[str]:
    columns = pd.read_csv(path, nrows=0).columns.tolist()
    missing = sorted(set(required) - set(columns))
    if missing:
        raise SystemExit(
            "{} is missing required columns: {}".format(path, ", ".join(missing))
        )
    return columns


def read_latency_data(
    path: str,
    mode: Optional[str],
    result_type: Optional[str],
    chunksize: int,
) -> pd.DataFrame:
    available = require_columns(path, ["finalizationLatencyMs"])
    numeric_cols = ["finalizationLatencyMs"]
    for optional in ("allowedLatenessMs", "latencyReductionMs"):
        if optional in available:
            numeric_cols.append(optional)
    text_cols = []
    if "finalizationReason" in available:
        text_cols.append("finalizationReason")

    filter_cols = []
    if mode and "mode" in available:
        filter_cols.append("mode")
    if result_type and "resultType" in available:
        filter_cols.append("resultType")

    usecols = numeric_cols + text_cols + filter_cols
    chunks = []
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
        if mode and "mode" in chunk.columns:
            chunk = chunk[chunk["mode"].astype(str).str.upper().eq(mode.upper())]
        if result_type and "resultType" in chunk.columns:
            chunk = chunk[
                chunk["resultType"].astype(str).str.upper().eq(result_type.upper())
            ]
        if chunk.empty:
            continue

        values = chunk[numeric_cols].apply(pd.to_numeric, errors="coerce")
        values = values.dropna(subset=["finalizationLatencyMs"])
        if not values.empty:
            for col in text_cols:
                values[col] = chunk.loc[values.index, col].astype("string")
            chunks.append(values)

    if not chunks:
        return pd.DataFrame(columns=numeric_cols)
    return pd.concat(chunks, ignore_index=True)


def build_summary(
    data: pd.DataFrame, deadline_tolerance_ms: int, immediate_tolerance_ms: int
) -> pd.DataFrame:
    latency_ms = data["finalizationLatencyMs"]
    latency_min = latency_ms / 60000.0

    summary = {
        "windows": int(len(data)),
        "latency_mean_min": latency_min.mean(),
        "latency_std_min": latency_min.std(),
        "latency_min_min": latency_min.min(),
        "latency_p25_min": latency_min.quantile(0.25),
        "latency_p50_min": latency_min.quantile(0.50),
        "latency_p75_min": latency_min.quantile(0.75),
        "latency_p90_min": latency_min.quantile(0.90),
        "latency_p95_min": latency_min.quantile(0.95),
        "latency_p99_min": latency_min.quantile(0.99),
        "latency_max_min": latency_min.max(),
    }

    immediate = latency_ms.le(immediate_tolerance_ms)
    summary["immediate_windows"] = int(immediate.sum())
    summary["immediate_ratio"] = float(immediate.mean())

    if "allowedLatenessMs" in data.columns:
        allowed_ms = data["allowedLatenessMs"]
        allowed_min = allowed_ms / 60000.0
        if "finalizationReason" in data.columns:
            reason = data["finalizationReason"].astype(str).str.upper()
            deadline = reason.eq("DEADLINE")
            completeness = reason.eq("COMPLETENESS")
            summary["completeness_windows"] = int(completeness.sum())
            summary["completeness_ratio"] = float(completeness.mean())
        else:
            deadline = latency_ms.ge(allowed_ms - deadline_tolerance_ms)
        summary["allowed_lateness_median_min"] = allowed_min.median()
        summary["allowed_lateness_max_min"] = allowed_min.max()
        summary["deadline_windows"] = int(deadline.sum())
        summary["deadline_ratio"] = float(deadline.mean())

    if "latencyReductionMs" in data.columns:
        reduction_min = data["latencyReductionMs"] / 60000.0
        summary["latency_reduction_mean_min"] = reduction_min.mean()
        summary["latency_reduction_p50_min"] = reduction_min.quantile(0.50)
        summary["latency_reduction_p95_min"] = reduction_min.quantile(0.95)

    return pd.DataFrame([summary])


def build_quantiles(data: pd.DataFrame) -> pd.DataFrame:
    probabilities = [0.0, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0]
    latency_min = data["finalizationLatencyMs"] / 60000.0
    rows = []
    for probability in probabilities:
        rows.append(
            {
                "quantile": probability,
                "window_share_percent": probability * 100.0,
                "finalization_latency_min": latency_min.quantile(probability),
            }
        )
    return pd.DataFrame(rows)


def build_histogram(
    data: pd.DataFrame, bin_minutes: float, max_minutes: Optional[float]
) -> pd.DataFrame:
    if bin_minutes <= 0:
        raise SystemExit("--bin-minutes must be positive.")

    latency_min = data["finalizationLatencyMs"].to_numpy(dtype=float) / 60000.0
    right_edge = (
        float(max_minutes) if max_minutes is not None else float(np.nanmax(latency_min))
    )
    if "allowedLatenessMs" in data.columns:
        allowed_right = float(data["allowedLatenessMs"].max() / 60000.0)
        right_edge = max(right_edge, allowed_right)
    right_edge = max(right_edge, bin_minutes)

    edges = np.arange(0.0, right_edge + bin_minutes, bin_minutes)
    if edges[-1] < latency_min.max():
        edges = np.append(edges, latency_min.max())

    counts, edges = np.histogram(latency_min, bins=edges)
    total = counts.sum()
    shares = counts / total if total else counts
    cumulative = np.cumsum(shares)

    return pd.DataFrame(
        {
            "bin_start_min": edges[:-1],
            "bin_end_min": edges[1:],
            "windows": counts,
            "window_share": shares,
            "cumulative_window_share": cumulative,
        }
    )


def add_reference_lines(
    ax, quantiles: pd.DataFrame, include_labels: bool = True
) -> None:
    references = [
        ("P50", 0.50, "#4c78a8"),
        ("P95", 0.95, "#f58518"),
        ("P99", 0.99, "#e45756"),
    ]
    for label, probability, color in references:
        value = quantiles.loc[
            quantiles["quantile"].eq(probability), "finalization_latency_min"
        ]
        if value.empty:
            continue
        x = float(value.iloc[0])
        ax.axvline(
            x,
            color=color,
            linestyle="--",
            linewidth=1.3,
            label="{}: {:.1f} min".format(label, x) if include_labels else "_nolegend_",
        )


def add_method_markers(
    ax,
    sw_latency_min: float,
    dww_latency_min: float,
    alw_latency_min: float,
    bin_minutes: float,
    bar_height: Optional[float] = None,
    include_labels: bool = True,
) -> None:
    top = ax.get_ylim()[1]
    height = bar_height if bar_height is not None else top / 3.0
    band_width = max(bin_minutes * 0.24, 2.0)
    band_gap = max(bin_minutes * 0.04, 0.4)
    sw_right = sw_latency_min - band_gap
    sw_left = sw_right - band_width
    dww_left = dww_latency_min + band_gap
    sw_label = "{}: {:.0f} min".format(method_label("baseline1"), sw_latency_min)
    dww_label = "{}: {:.0f} min".format(method_label("baseline2"), dww_latency_min)
    alw_label = "{}: {:.0f} min".format(method_label("baseline3"), alw_latency_min)
    ax.bar(
        sw_left,
        height,
        width=band_width,
        align="edge",
        color="#029e73",
        alpha=0.32,
        linewidth=0,
        zorder=1,
        label=sw_label if include_labels else "_nolegend_",
    )
    ax.bar(
        dww_left,
        height,
        width=band_width,
        align="edge",
        color="#cc78bc",
        alpha=0.32,
        linewidth=0,
        zorder=1,
        label=dww_label if include_labels else "_nolegend_",
    )
    band_half_width = max(bin_minutes * 0.35, 2.0)
    ax.bar(
        alw_latency_min - band_half_width,
        height,
        width=band_half_width * 2.0,
        align="edge",
        color="#de8f05",
        alpha=0.22,
        linewidth=0,
        label=alw_label if include_labels else "_nolegend_",
    )


def add_full_height_method_bands(
    fig,
    axis,
    lower_ax,
    upper_ax,
    sw_latency_min: float,
    dww_latency_min: float,
    alw_latency_min: float,
    bin_minutes: float,
) -> List[Patch]:
    band_width = max(bin_minutes * 0.24, 2.0)
    band_gap = max(bin_minutes * 0.04, 0.4)
    alw_half_width = max(bin_minutes * 0.35, 2.0)
    bands = [
        (
            sw_latency_min - band_gap - band_width,
            band_width,
            "#029e73",
            0.32,
            "{}: {:.0f} min".format(method_label("baseline1"), sw_latency_min),
        ),
        (
            dww_latency_min + band_gap,
            band_width,
            "#cc78bc",
            0.32,
            "{}: {:.0f} min".format(method_label("baseline2"), dww_latency_min),
        ),
        (
            alw_latency_min - alw_half_width,
            alw_half_width * 2.0,
            "#de8f05",
            0.22,
            "{}: {:.0f} min".format(method_label("baseline3"), alw_latency_min),
        ),
    ]

    figure_transform = fig.transFigure.inverted()
    y_bottom = lower_ax.get_position().y0
    y_top = figure_transform.transform(
        upper_ax.transData.transform((0, 100.0))
    )[1]
    handles = []
    for left, width, color, alpha, label in bands:
        x_left = figure_transform.transform(axis.transData.transform((left, 0)))[0]
        x_right = figure_transform.transform(
            axis.transData.transform((left + width, 0))
        )[0]
        rectangle = Rectangle(
            (x_left, y_bottom),
            x_right - x_left,
            y_top - y_bottom,
            transform=fig.transFigure,
            facecolor=color,
            edgecolor="none",
            alpha=alpha,
            zorder=3,
        )
        fig.add_artist(rectangle)
        handles.append(
            Patch(facecolor=color, edgecolor="none", alpha=alpha, label=label)
        )
    return handles


def add_full_height_reference_lines(
    fig, axis, lower_ax, upper_ax, quantiles: pd.DataFrame
) -> List[Line2D]:
    references = [
        ("P50", 0.50, "#4c78a8"),
        ("P95", 0.95, "#f58518"),
        ("P99", 0.99, "#e45756"),
    ]
    figure_transform = fig.transFigure.inverted()
    y_bottom = lower_ax.get_position().y0
    y_top = upper_ax.get_position().y1
    handles = []
    for label, probability, color in references:
        value = quantiles.loc[
            quantiles["quantile"].eq(probability), "finalization_latency_min"
        ]
        if value.empty:
            continue
        x = float(value.iloc[0])
        x_fig = figure_transform.transform(axis.transData.transform((x, 0)))[0]
        line = Line2D(
            [x_fig, x_fig],
            [y_bottom, y_top],
            transform=fig.transFigure,
            color=color,
            linestyle="--",
            linewidth=1.3,
            zorder=4,
        )
        fig.add_artist(line)
        handles.append(
            Line2D(
                [0],
                [0],
                color=color,
                linestyle="--",
                linewidth=1.3,
                label="{}: {:.1f} min".format(label, x),
            )
        )
    return handles


def resolve_alw_latency_min(
    summary: pd.DataFrame, explicit_value: Optional[float]
) -> float:
    if explicit_value is not None:
        return explicit_value
    if "allowed_lateness_max_min" not in summary.columns:
        raise SystemExit(
            "--alw-latency-min is required when input has no allowedLatenessMs column."
        )
    value = summary.iloc[0]["allowed_lateness_max_min"]
    if pd.isna(value):
        raise SystemExit(
            "--alw-latency-min is required when allowedLatenessMs has no valid values."
        )
    return float(value)


def plot_histogram(
    path: str,
    histogram: pd.DataFrame,
    quantiles: pd.DataFrame,
    summary: pd.DataFrame,
    sw_latency_min: float,
    dww_latency_min: float,
    alw_latency_min: float,
) -> None:
    row = summary.iloc[0]
    fig, (upper_ax, lower_ax) = plt.subplots(
        2,
        1,
        figsize=(8.4, 5.4),
        sharex=True,
        gridspec_kw={"height_ratios": [1.25, 3.8], "hspace": 0.06},
    )
    width = histogram["bin_end_min"] - histogram["bin_start_min"]
    lower_ax.bar(
        histogram["bin_start_min"],
        histogram["window_share"] * 100.0,
        width=width,
        align="edge",
        color="#4c78a8",
        edgecolor="white",
        linewidth=0.3,
        label="{} distribution".format(method_label("proposed")),
    )
    bin_minutes = float(width.median())
    band_width = max(bin_minutes * 0.24, 2.0)
    band_gap = max(bin_minutes * 0.04, 0.4)
    alw_half_width = max(bin_minutes * 0.35, 2.0)
    left_edge = min(
        float(histogram["bin_start_min"].min()),
        sw_latency_min - band_gap - band_width,
    )
    right_edge = max(
        float(histogram["bin_end_min"].max()),
        dww_latency_min + band_gap + band_width,
        alw_latency_min + alw_half_width,
    )
    x_padding = max((right_edge - left_edge) * 0.02, 5.0)
    lower_ax.set_xlim(left_edge - x_padding, right_edge + x_padding)
    lower_top = max(27.0, float((histogram["window_share"] * 100.0).max()) * 1.08)
    lower_ax.set_ylim(0, lower_top)
    upper_ax.set_ylim(96.0, 101.0)
    upper_ax.set_yticks([100])
    lower_ax.set_yticks([0, 5, 10, 15, 20, 25])

    upper_ax.set_title("Window Finalization Latency Distribution")
    lower_ax.set_xlabel("Finalization latency after window end (minutes)")
    lower_ax.set_ylabel("Windows (%)")
    upper_ax.grid(axis="y", alpha=0.35)
    lower_ax.grid(axis="y", alpha=0.35)
    upper_ax.spines["bottom"].set_visible(False)
    lower_ax.spines["top"].set_visible(False)
    upper_ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)

    diagonal = 0.008
    kwargs = {
        "transform": upper_ax.transAxes,
        "color": "black",
        "clip_on": False,
        "linewidth": 1.0,
    }
    upper_ax.plot((-diagonal, +diagonal), (-diagonal, +diagonal), **kwargs)
    upper_ax.plot((1 - diagonal, 1 + diagonal), (-diagonal, +diagonal), **kwargs)
    kwargs["transform"] = lower_ax.transAxes
    lower_ax.plot((-diagonal, +diagonal), (1 - diagonal, 1 + diagonal), **kwargs)
    lower_ax.plot((1 - diagonal, 1 + diagonal), (1 - diagonal, 1 + diagonal), **kwargs)

    text = "n={:,}\nmean={:.1f} min\nmedian={:.1f} min\nP95={:.1f} min".format(
        int(row["windows"]),
        row["latency_mean_min"],
        row["latency_p50_min"],
        row["latency_p95_min"],
    )
    if "deadline_ratio" in summary.columns:
        text += "\ndeadline={:.1f}%".format(row["deadline_ratio"] * 100.0)
    if "completeness_ratio" in summary.columns:
        text += "\ncomplete={:.1f}%".format(row["completeness_ratio"] * 100.0)
    fig.subplots_adjust(right=0.72)
    reference_handles = add_full_height_reference_lines(
        fig, lower_ax, lower_ax, upper_ax, quantiles
    )
    method_handles = add_full_height_method_bands(
        fig,
        lower_ax,
        lower_ax,
        upper_ax,
        sw_latency_min,
        dww_latency_min,
        alw_latency_min,
        bin_minutes,
    )
    fig.text(
        0.745,
        0.50,
        text,
        ha="left",
        va="top",
        fontsize=9,
        bbox={
            "boxstyle": "round,pad=0.35",
            "facecolor": "white",
            "edgecolor": "#cccccc",
            "alpha": 0.92,
        },
    )
    handles, labels = [], []
    for axis in (upper_ax, lower_ax):
        axis_handles, axis_labels = axis.get_legend_handles_labels()
        handles.extend(axis_handles)
        labels.extend(axis_labels)
    handles.extend(reference_handles)
    labels.extend([handle.get_label() for handle in reference_handles])
    handles.extend(method_handles)
    labels.extend([handle.get_label() for handle in method_handles])
    unique = dict(zip(labels, handles))
    upper_ax.legend(
        unique.values(),
        unique.keys(),
        loc="upper left",
        bbox_to_anchor=(1.02, 1.0),
        frameon=True,
        fontsize=9,
    )
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def plot_ecdf(
    path: str,
    histogram: pd.DataFrame,
    quantiles: pd.DataFrame,
    summary: pd.DataFrame,
    sw_latency_min: float,
    dww_latency_min: float,
    alw_latency_min: float,
) -> None:
    fig, ax = plt.subplots(figsize=(8.4, 5.0))
    x = histogram["bin_end_min"]
    y = histogram["cumulative_window_share"] * 100.0
    ax.step(
        x,
        y,
        where="post",
        color="#54a24b",
        linewidth=2.0,
        label="{} ECDF".format(method_label("proposed")),
    )
    add_reference_lines(ax, quantiles)
    add_method_markers(
        ax,
        sw_latency_min,
        dww_latency_min,
        alw_latency_min,
        histogram["bin_end_min"].diff().median(),
    )
    for percent in (50, 95, 99):
        ax.axhline(percent, color="#bbbbbb", linestyle=":", linewidth=0.9)
    ax.set_title("CBW Window Finalization Latency ECDF")
    ax.set_xlabel("Finalization latency after window end (minutes)")
    ax.set_ylabel("Cumulative windows (%)")
    ax.set_ylim(0, 100.5)
    ax.grid(True, alpha=0.35)
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    ax.legend(unique.values(), unique.keys(), loc="lower right", frameon=True)
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    ensure_dir(args.output_dir)

    data = read_latency_data(args.input, args.mode, args.result_type, args.chunksize)
    if data.empty:
        raise SystemExit("No finalization latency rows matched the selected filters.")

    summary = build_summary(
        data, args.deadline_tolerance_ms, args.immediate_tolerance_ms
    )
    quantiles = build_quantiles(data)
    histogram = build_histogram(data, args.bin_minutes, args.max_minutes)
    alw_latency_min = resolve_alw_latency_min(summary, args.alw_latency_min)

    summary_path = os.path.join(
        args.output_dir,
        output_name("window_finalization_latency_summary.csv", args.run_id),
    )
    quantiles_path = os.path.join(
        args.output_dir,
        output_name("window_finalization_latency_quantiles.csv", args.run_id),
    )
    histogram_path = os.path.join(
        args.output_dir,
        output_name("window_finalization_latency_histogram.csv", args.run_id),
    )
    histogram_png = os.path.join(
        args.output_dir,
        output_name("window_finalization_latency_distribution.png", args.run_id),
    )
    ecdf_png = os.path.join(
        args.output_dir,
        output_name("window_finalization_latency_ecdf.png", args.run_id),
    )

    summary.to_csv(summary_path, index=False)
    quantiles.to_csv(quantiles_path, index=False)
    histogram.to_csv(histogram_path, index=False)
    plot_histogram(
        histogram_png,
        histogram,
        quantiles,
        summary,
        args.sw_latency_min,
        args.dww_latency_min,
        alw_latency_min,
    )
    plot_ecdf(
        ecdf_png,
        histogram,
        quantiles,
        summary,
        args.sw_latency_min,
        args.dww_latency_min,
        alw_latency_min,
    )

    row = summary.iloc[0]
    print("Analyzed {:,} finalized windows.".format(int(row["windows"])))
    print("Median finalization latency: {:.2f} min".format(row["latency_p50_min"]))
    print("P95 finalization latency: {:.2f} min".format(row["latency_p95_min"]))
    print("Wrote summary: {}".format(summary_path))
    print("Wrote plots: {}, {}".format(histogram_png, ecdf_png))


if __name__ == "__main__":
    main()
