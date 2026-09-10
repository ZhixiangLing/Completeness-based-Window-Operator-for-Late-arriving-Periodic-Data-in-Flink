#!/usr/bin/env python3
"""
Analyze standard throughput metrics sampled at source ingestion and post-filter.

Input CSV columns:
  timestamp,probe,countInInterval,eventsPerSecond,intervalMs

Default input:
  analysis/standard_throughput_metrics_run5_*.csv

Outputs:
  - standard_throughput_method_summary_<run_id>.csv
  - standard_throughput_overall_summary_<run_id>.csv
  - standard_throughput_ratio_summary_<run_id>.csv
  - standard_throughput_runtime_<run_id>.csv
  - standard_throughput_ratio_samples_<run_id>.csv
  - standard_throughput_post_filter_compare_<run_id>.png
  - standard_throughput_runtime_compare_<run_id>.png
  - standard_throughput_distribution_compare_<run_id>.png
  - standard_throughput_ratio_compare_<run_id>.png
"""

import argparse
import glob
import os
import re
from typing import Dict, Iterable, List

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import matplotlib.pyplot as plt
import pandas as pd

try:
    from method_labels import method_label, method_sort_key
except ImportError:
    METHOD_DISPLAY_LABELS = {
        "fast": "SW",
        "delayed": "DWW",
        "baseline": "ALW",
        "proposed": "CBW",
    }

    def method_label(value: str) -> str:
        return METHOD_DISPLAY_LABELS.get(str(value).strip().lower(), str(value))

    def method_sort_key(value: str) -> int:
        order = {
            "fast": 0,
            "sw": 0,
            "delayed": 1,
            "dww": 1,
            "baseline": 2,
            "alw": 2,
            "proposed": 3,
            "cbw": 3,
        }
        return order.get(str(value).strip().lower(), len(order))


DEFAULT_INPUT_GLOB = (
    "analysis/standard_throughput_metrics_run8/standard_throughput_metrics_run8_*.csv"
)
DEFAULT_OUTPUT_DIR = "analysis/standard_throughput_run8_compare"
DEFAULT_RUN_ID = "run8"
DEFAULT_REFERENCE = "baseline"
DEFAULT_PROPOSED = "proposed"

STAGE_ORDER = ["Source", "Post-filter"]
STAGE_COLORS = {"Source": "#4c78a8", "Post-filter": "#f58518"}
METHOD_COLORS = {
    "SW": "#4c78a8",
    "DWW": "#72b7b2",
    "ALW": "#f58518",
    "CBW": "#54a24b",
}
METHOD_MARKERS = {"SW": "o", "DWW": "s", "ALW": "^", "CBW": "D"}
FILENAME_RE = re.compile(
    r"^standard_throughput_metrics_(?P<run_id>[^_]+)_(?P<method>fast|delayed|baseline|proposed)_(?P<time_setting>.+)\.csv$"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze standard source/post-filter throughput metrics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input CSV. May be passed multiple times. When omitted, --input-glob is used.",
    )
    parser.add_argument(
        "--input-glob",
        default=DEFAULT_INPUT_GLOB,
        help="Glob used when --input is omitted.",
    )
    parser.add_argument(
        "--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory."
    )
    parser.add_argument(
        "--run-id",
        default=DEFAULT_RUN_ID,
        help="Run identifier appended to output files.",
    )
    parser.add_argument(
        "--drop-first",
        type=int,
        default=0,
        help="Drop first N samples per method/stage.",
    )
    parser.add_argument(
        "--ratio-tolerance-ms",
        type=int,
        default=1500,
        help="Max timestamp distance when pairing source/post-filter samples.",
    )
    parser.add_argument(
        "--runtime-bin-sec",
        type=float,
        default=5.0,
        help="Bucket size for smoothed runtime curves.",
    )
    parser.add_argument(
        "--reference-method",
        default=DEFAULT_REFERENCE,
        help="Method used for relative comparison.",
    )
    parser.add_argument(
        "--proposed-method",
        default=DEFAULT_PROPOSED,
        help="Proposed method used for relative comparison.",
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


def resolve_inputs(inputs: Iterable[str], input_glob: str) -> List[str]:
    paths = list(inputs)
    if not paths:
        paths = glob.glob(input_glob)
    paths = sorted(dict.fromkeys(paths))
    if not paths:
        raise SystemExit("No input CSV files matched: {}".format(input_glob))
    missing = [path for path in paths if not os.path.exists(path)]
    if missing:
        raise SystemExit("Input CSV does not exist: {}".format(", ".join(missing)))
    return paths


def infer_metadata(path: str, probes: pd.Series) -> Dict[str, str]:
    name = os.path.basename(path)
    match = FILENAME_RE.match(name)
    if match:
        run_id = match.group("run_id")
        method_key = match.group("method")
        time_setting = match.group("time_setting")
    else:
        first_probe = (
            str(probes.dropna().iloc[0]) if not probes.dropna().empty else "unknown"
        )
        method_key = first_probe.split("_", 1)[0]
        run_id = "unknown"
        time_setting = "unknown"
    return {
        "run_id": run_id,
        "method_key": method_key,
        "method_label": method_label(method_key),
        "time_setting": time_setting,
        "experiment": "{}_{}".format(method_key, time_setting),
    }


def stage_label(probe: str) -> str:
    probe = str(probe)
    if probe.endswith("_source_after_ingestion"):
        return "Source"
    if probe.endswith("_post_filter"):
        return "Post-filter"
    return probe


def stage_order(stage: str) -> int:
    try:
        return STAGE_ORDER.index(stage)
    except ValueError:
        return len(STAGE_ORDER)


def load_one_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {
        "timestamp",
        "probe",
        "countInInterval",
        "eventsPerSecond",
        "intervalMs",
    }
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(
            "{} is missing required columns: {}".format(
                path, ", ".join(sorted(missing))
            )
        )

    metadata = infer_metadata(path, df["probe"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["countInInterval"] = pd.to_numeric(df["countInInterval"], errors="coerce")
    df["eventsPerSecond"] = pd.to_numeric(df["eventsPerSecond"], errors="coerce")
    df["intervalMs"] = pd.to_numeric(df["intervalMs"], errors="coerce")
    df = df.dropna(
        subset=[
            "timestamp",
            "probe",
            "countInInterval",
            "eventsPerSecond",
            "intervalMs",
        ]
    ).copy()
    df = df[df["intervalMs"] > 0].copy()
    if df.empty:
        raise SystemExit("{} has no usable throughput samples".format(path))

    for key, value in metadata.items():
        df[key] = value
    df["input_file"] = path
    df["stage"] = df["probe"].map(stage_label)
    return df


def load_samples(paths: Iterable[str], drop_first: int) -> pd.DataFrame:
    frames = [load_one_file(path) for path in paths]
    df = pd.concat(frames, ignore_index=True)
    df["method_order"] = df["method_key"].map(method_sort_key)
    df["stage_order"] = df["stage"].map(stage_order)
    df = df.sort_values(["method_order", "stage_order", "timestamp"]).copy()
    df["sample_index"] = df.groupby(["experiment", "stage"]).cumcount()
    if drop_first > 0:
        df = df[df["sample_index"] >= drop_first].copy()
        if df.empty:
            raise SystemExit(
                "No samples remain after --drop-first {}".format(drop_first)
            )
        df["sample_index"] = df.groupby(["experiment", "stage"]).cumcount()

    first_by_stage = df.groupby(["experiment", "stage"])["timestamp"].transform("min")
    df["elapsed_sec"] = (df["timestamp"] - first_by_stage).dt.total_seconds()
    max_by_stage = df.groupby(["experiment", "stage"])["elapsed_sec"].transform("max")
    df["runtime_percent"] = (
        df["elapsed_sec"] / max_by_stage.where(max_by_stage > 0, 1.0)
    ) * 100.0
    df["elapsed_min"] = df["elapsed_sec"] / 60.0
    return df.sort_values(["method_order", "stage_order", "timestamp"]).reset_index(
        drop=True
    )


def summarize_by_stage(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_cols = ["method_key", "method_label", "experiment", "time_setting", "stage"]
    for keys, group in df.groupby(group_cols, sort=False):
        method_key, label, experiment, time_setting, stage = keys
        eps = group["eventsPerSecond"]
        interval_sec = group["intervalMs"] / 1000.0
        total_events = int(group["countInInterval"].sum())
        total_interval_sec = float(interval_sec.sum())
        weighted_avg = (
            total_events / total_interval_sec if total_interval_sec > 0 else 0.0
        )
        rows.append(
            {
                "method_key": method_key,
                "method_label": label,
                "experiment": experiment,
                "time_setting": time_setting,
                "stage": stage,
                "probe": group["probe"].iloc[0],
                "samples": int(len(group)),
                "total_events": total_events,
                "duration_sec": float(group["elapsed_sec"].max()),
                "duration_min": float(group["elapsed_sec"].max()) / 60.0,
                "total_interval_sec": total_interval_sec,
                "weighted_avg_events_per_sec": weighted_avg,
                "mean_events_per_sec": float(eps.mean()),
                "median_events_per_sec": float(eps.median()),
                "p05_events_per_sec": float(eps.quantile(0.05)),
                "p25_events_per_sec": float(eps.quantile(0.25)),
                "p75_events_per_sec": float(eps.quantile(0.75)),
                "p95_events_per_sec": float(eps.quantile(0.95)),
                "p99_events_per_sec": float(eps.quantile(0.99)),
                "min_events_per_sec": float(eps.min()),
                "max_events_per_sec": float(eps.max()),
                "std_events_per_sec": float(eps.std(ddof=0)),
                "cv_events_per_sec": (
                    float(eps.std(ddof=0) / eps.mean()) if eps.mean() else 0.0
                ),
                "first_timestamp": group["timestamp"].min().isoformat(),
                "last_timestamp": group["timestamp"].max().isoformat(),
            }
        )
    out = pd.DataFrame(rows)
    out["method_order"] = out["method_key"].map(method_sort_key)
    out["stage_order"] = out["stage"].map(stage_order)
    return out.sort_values(["method_order", "stage_order"]).drop(
        columns=["method_order", "stage_order"]
    )


def add_relative_columns(
    overall: pd.DataFrame, reference_method: str, proposed_method: str
) -> pd.DataFrame:
    out = overall.copy()

    def ref_value(method: str, column: str):
        rows = out[out["method_key"].eq(method)]
        if rows.empty:
            return None
        return float(rows.iloc[0][column])

    baseline_runtime = ref_value(reference_method, "post_filter_duration_min")
    baseline_eps = ref_value(
        reference_method, "post_filter_weighted_avg_events_per_sec"
    )
    proposed_runtime = ref_value(proposed_method, "post_filter_duration_min")
    proposed_eps = ref_value(proposed_method, "post_filter_weighted_avg_events_per_sec")

    if baseline_runtime:
        out["runtime_vs_{}_pct".format(reference_method)] = (
            out["post_filter_duration_min"] / baseline_runtime - 1.0
        ) * 100.0
        out["speedup_vs_{}_pct".format(reference_method)] = (
            baseline_runtime / out["post_filter_duration_min"] - 1.0
        ) * 100.0
    if baseline_eps:
        out["post_filter_eps_vs_{}_pct".format(reference_method)] = (
            out["post_filter_weighted_avg_events_per_sec"] / baseline_eps - 1.0
        ) * 100.0
    if proposed_runtime:
        out["runtime_vs_{}_pct".format(proposed_method)] = (
            out["post_filter_duration_min"] / proposed_runtime - 1.0
        ) * 100.0
    if proposed_eps:
        out["post_filter_eps_vs_{}_pct".format(proposed_method)] = (
            out["post_filter_weighted_avg_events_per_sec"] / proposed_eps - 1.0
        ) * 100.0
    return out


def summarize_overall(
    stage_summary: pd.DataFrame, reference_method: str, proposed_method: str
) -> pd.DataFrame:
    rows = []
    for experiment, group in stage_summary.groupby("experiment", sort=False):
        source = group[group["stage"].eq("Source")]
        post_filter = group[group["stage"].eq("Post-filter")]
        if source.empty or post_filter.empty:
            continue
        src = source.iloc[0]
        post = post_filter.iloc[0]
        rows.append(
            {
                "method_key": post["method_key"],
                "method_label": post["method_label"],
                "experiment": experiment,
                "time_setting": post["time_setting"],
                "source_total_events": int(src["total_events"]),
                "post_filter_total_events": int(post["total_events"]),
                "post_filter_source_event_ratio_pct": (
                    post["total_events"] / src["total_events"]
                )
                * 100.0,
                "source_duration_min": src["duration_min"],
                "post_filter_duration_min": post["duration_min"],
                "source_weighted_avg_events_per_sec": src[
                    "weighted_avg_events_per_sec"
                ],
                "post_filter_weighted_avg_events_per_sec": post[
                    "weighted_avg_events_per_sec"
                ],
                "post_filter_median_events_per_sec": post["median_events_per_sec"],
                "post_filter_p95_events_per_sec": post["p95_events_per_sec"],
                "post_filter_p99_events_per_sec": post["p99_events_per_sec"],
                "post_filter_cv_events_per_sec": post["cv_events_per_sec"],
            }
        )
    out = pd.DataFrame(rows)
    out["method_order"] = out["method_key"].map(method_sort_key)
    out = out.sort_values(["method_order", "experiment"]).drop(columns=["method_order"])
    return add_relative_columns(out, reference_method, proposed_method)


def aggregate_runtime(df: pd.DataFrame, bin_sec: float) -> pd.DataFrame:
    if bin_sec <= 0:
        raise SystemExit("--runtime-bin-sec must be positive")
    result = df.copy()
    result["elapsed_bucket_sec"] = (result["elapsed_sec"] // bin_sec) * bin_sec
    group_cols = [
        "method_key",
        "method_label",
        "experiment",
        "stage",
        "elapsed_bucket_sec",
    ]
    grouped = result.groupby(group_cols, sort=False)
    out = grouped.agg(
        elapsed_sec=("elapsed_sec", "mean"),
        elapsed_min=("elapsed_min", "mean"),
        runtime_percent=("runtime_percent", "mean"),
        mean_events_per_sec=("eventsPerSecond", "mean"),
        median_events_per_sec=("eventsPerSecond", "median"),
        p95_events_per_sec=("eventsPerSecond", lambda s: s.quantile(0.95)),
        samples=("eventsPerSecond", "size"),
    ).reset_index()
    out["method_order"] = out["method_key"].map(method_sort_key)
    out["stage_order"] = out["stage"].map(stage_order)
    return out.sort_values(["method_order", "stage_order", "elapsed_sec"]).drop(
        columns=["method_order", "stage_order"]
    )


def build_ratio(df: pd.DataFrame, tolerance_ms: int) -> pd.DataFrame:
    if tolerance_ms <= 0:
        raise SystemExit("--ratio-tolerance-ms must be positive")
    rows = []
    for experiment, group in df.groupby("experiment", sort=False):
        method_key = group["method_key"].iloc[0]
        label = group["method_label"].iloc[0]
        source = group[group["stage"].eq("Source")].sort_values("timestamp").copy()
        post_filter = (
            group[group["stage"].eq("Post-filter")].sort_values("timestamp").copy()
        )
        if source.empty or post_filter.empty:
            continue

        source = source[
            ["timestamp", "eventsPerSecond", "countInInterval", "elapsed_sec"]
        ].rename(
            columns={
                "eventsPerSecond": "source_events_per_sec",
                "countInInterval": "source_count",
                "elapsed_sec": "source_elapsed_sec",
            }
        )
        post_filter = post_filter[
            ["timestamp", "eventsPerSecond", "countInInterval", "elapsed_sec"]
        ].rename(
            columns={
                "timestamp": "post_filter_timestamp",
                "eventsPerSecond": "post_filter_events_per_sec",
                "countInInterval": "post_filter_count",
                "elapsed_sec": "post_filter_elapsed_sec",
            }
        )
        paired = pd.merge_asof(
            post_filter,
            source,
            left_on="post_filter_timestamp",
            right_on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta(milliseconds=tolerance_ms),
        ).dropna(subset=["source_events_per_sec"])
        if paired.empty:
            continue
        paired["method_key"] = method_key
        paired["method_label"] = label
        paired["experiment"] = experiment
        paired["timestamp_delta_ms"] = (
            paired["post_filter_timestamp"] - paired["timestamp"]
        ).dt.total_seconds() * 1000.0
        paired["throughput_ratio"] = (
            paired["post_filter_events_per_sec"] / paired["source_events_per_sec"]
        )
        paired["filter_drop_ratio"] = 1.0 - paired["throughput_ratio"]
        paired["elapsed_sec"] = paired["post_filter_elapsed_sec"]
        paired["elapsed_min"] = paired["elapsed_sec"] / 60.0
        rows.append(
            paired[
                [
                    "method_key",
                    "method_label",
                    "experiment",
                    "post_filter_timestamp",
                    "timestamp",
                    "timestamp_delta_ms",
                    "elapsed_sec",
                    "elapsed_min",
                    "source_events_per_sec",
                    "post_filter_events_per_sec",
                    "throughput_ratio",
                    "filter_drop_ratio",
                    "source_count",
                    "post_filter_count",
                ]
            ]
        )
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["method_order"] = out["method_key"].map(method_sort_key)
    return out.sort_values(["method_order", "elapsed_sec"]).drop(
        columns=["method_order"]
    )


def summarize_ratio(ratio: pd.DataFrame) -> pd.DataFrame:
    if ratio.empty:
        return pd.DataFrame()
    rows = []
    for experiment, group in ratio.groupby("experiment", sort=False):
        values = group["throughput_ratio"] * 100.0
        rows.append(
            {
                "method_key": group["method_key"].iloc[0],
                "method_label": group["method_label"].iloc[0],
                "experiment": experiment,
                "paired_samples": int(len(group)),
                "mean_ratio_pct": float(values.mean()),
                "median_ratio_pct": float(values.median()),
                "p05_ratio_pct": float(values.quantile(0.05)),
                "p95_ratio_pct": float(values.quantile(0.95)),
                "min_ratio_pct": float(values.min()),
                "max_ratio_pct": float(values.max()),
                "mean_abs_timestamp_delta_ms": float(
                    group["timestamp_delta_ms"].abs().mean()
                ),
            }
        )
    out = pd.DataFrame(rows)
    out["method_order"] = out["method_key"].map(method_sort_key)
    return out.sort_values(["method_order", "experiment"]).drop(
        columns=["method_order"]
    )


def aggregate_ratio_runtime(ratio: pd.DataFrame, bin_sec: float) -> pd.DataFrame:
    if ratio.empty:
        return pd.DataFrame()
    result = ratio.copy()
    result["elapsed_bucket_sec"] = (result["elapsed_sec"] // bin_sec) * bin_sec
    grouped = result.groupby(
        ["method_key", "method_label", "experiment", "elapsed_bucket_sec"], sort=False
    )
    out = grouped.agg(
        elapsed_sec=("elapsed_sec", "mean"),
        elapsed_min=("elapsed_min", "mean"),
        mean_ratio_pct=("throughput_ratio", lambda s: s.mean() * 100.0),
        median_ratio_pct=("throughput_ratio", lambda s: s.median() * 100.0),
        samples=("throughput_ratio", "size"),
    ).reset_index()
    out["method_order"] = out["method_key"].map(method_sort_key)
    return out.sort_values(["method_order", "elapsed_sec"]).drop(
        columns=["method_order"]
    )


def plot_post_filter_compare(path: str, overall: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.0, 4.5))
    labels = overall["method_label"].tolist()
    values = overall["post_filter_weighted_avg_events_per_sec"].tolist()
    colors = [METHOD_COLORS.get(label, "#777777") for label in labels]
    bars = ax.bar(labels, values, color=colors, width=0.62)
    ax.set_title("Post-filter Standard Throughput")
    ax.set_xlabel("Method")
    ax.set_ylabel("Weighted throughput (events/s)")
    ax.grid(axis="y", alpha=0.35)
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value,
            "{:.0f}".format(value),
            ha="center",
            va="bottom",
            fontsize=9,
        )
    upper = max(values) * 1.12 if values else 1
    ax.set_ylim(0, upper)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def plot_runtime_compare(path: str, runtime: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 4.8))
    post_filter = runtime[runtime["stage"].eq("Post-filter")]
    for _, group in post_filter.groupby("experiment", sort=False):
        label = group["method_label"].iloc[0]
        ax.plot(
            group["elapsed_min"],
            group["mean_events_per_sec"],
            marker=METHOD_MARKERS.get(label, "o"),
            markevery=max(1, len(group) // 12),
            markersize=4,
            linewidth=1.5,
            color=METHOD_COLORS.get(label),
            label=label,
        )
    ax.set_title("Post-filter Throughput over Runtime")
    ax.set_xlabel("Elapsed time (minutes)")
    ax.set_ylabel("Throughput (events/s)")
    ax.grid(True, alpha=0.35)
    ax.legend(frameon=True)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def plot_distribution_compare(path: str, samples: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.0, 4.8))
    post_filter = samples[samples["stage"].eq("Post-filter")]
    data = []
    labels = []
    for _, group in post_filter.groupby("experiment", sort=False):
        data.append(group["eventsPerSecond"])
        labels.append(group["method_label"].iloc[0])
    boxplot = ax.boxplot(data, tick_labels=labels, showmeans=True, patch_artist=True)
    for idx, label in enumerate(labels):
        color = METHOD_COLORS.get(label, "#777777")
        boxplot["boxes"][idx].set(
            facecolor=color, edgecolor=color, alpha=0.38, linewidth=1.5
        )
        boxplot["medians"][idx].set(color=color, linewidth=1.8)
        boxplot["fliers"][idx].set(
            marker="o",
            markerfacecolor="none",
            markeredgecolor=color,
            markeredgewidth=0.9,
            alpha=0.75,
        )
        boxplot["means"][idx].set(
            marker="^",
            markerfacecolor=color,
            markeredgecolor=color,
            markersize=7,
        )
        for line in boxplot["whiskers"][idx * 2 : idx * 2 + 2]:
            line.set(color=color, linewidth=1.3)
        for line in boxplot["caps"][idx * 2 : idx * 2 + 2]:
            line.set(color=color, linewidth=1.6)
    ax.set_title("Post-filter Throughput Distribution")
    ax.set_xlabel("Method")
    ax.set_ylabel("Throughput (events/s)")
    ax.grid(axis="y", alpha=0.35)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def plot_ratio_compare(path: str, ratio_runtime: pd.DataFrame) -> None:
    if ratio_runtime.empty:
        return
    fig, ax = plt.subplots(figsize=(8.2, 4.8))
    for _, group in ratio_runtime.groupby("experiment", sort=False):
        label = group["method_label"].iloc[0]
        ax.plot(
            group["elapsed_min"],
            group["mean_ratio_pct"],
            marker=METHOD_MARKERS.get(label, "o"),
            markevery=max(1, len(group) // 12),
            markersize=4,
            linewidth=1.5,
            color=METHOD_COLORS.get(label),
            label=label,
        )
    y = ratio_runtime["mean_ratio_pct"]
    ax.set_ylim(max(0.0, float(y.quantile(0.01)) - 1.0), float(y.quantile(0.99)) + 1.0)
    ax.set_title("Post-filter Throughput as Share of Source")
    ax.set_xlabel("Elapsed time (minutes)")
    ax.set_ylabel("Post-filter / source throughput (%)")
    ax.grid(True, alpha=0.35)
    ax.legend(frameon=True)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    ensure_dir(args.output_dir)

    input_paths = resolve_inputs(args.input, args.input_glob)
    samples = load_samples(input_paths, args.drop_first)
    method_summary = summarize_by_stage(samples)
    overall_summary = summarize_overall(
        method_summary, args.reference_method, args.proposed_method
    )
    runtime = aggregate_runtime(samples, args.runtime_bin_sec)
    ratio_samples = build_ratio(samples, args.ratio_tolerance_ms)
    ratio_summary = summarize_ratio(ratio_samples)
    ratio_runtime = aggregate_ratio_runtime(ratio_samples, args.runtime_bin_sec)

    method_summary_path = os.path.join(
        args.output_dir,
        output_name("standard_throughput_method_summary.csv", args.run_id),
    )
    overall_summary_path = os.path.join(
        args.output_dir,
        output_name("standard_throughput_overall_summary.csv", args.run_id),
    )
    runtime_path = os.path.join(
        args.output_dir, output_name("standard_throughput_runtime.csv", args.run_id)
    )
    ratio_samples_path = os.path.join(
        args.output_dir,
        output_name("standard_throughput_ratio_samples.csv", args.run_id),
    )
    ratio_summary_path = os.path.join(
        args.output_dir,
        output_name("standard_throughput_ratio_summary.csv", args.run_id),
    )
    post_filter_png = os.path.join(
        args.output_dir,
        output_name("standard_throughput_post_filter_compare.png", args.run_id),
    )
    runtime_png = os.path.join(
        args.output_dir,
        output_name("standard_throughput_runtime_compare.png", args.run_id),
    )
    distribution_png = os.path.join(
        args.output_dir,
        output_name("standard_throughput_distribution_compare.png", args.run_id),
    )
    ratio_png = os.path.join(
        args.output_dir,
        output_name("standard_throughput_ratio_compare.png", args.run_id),
    )

    method_summary.to_csv(method_summary_path, index=False)
    overall_summary.to_csv(overall_summary_path, index=False)
    runtime.to_csv(runtime_path, index=False)
    ratio_samples.to_csv(ratio_samples_path, index=False)
    ratio_summary.to_csv(ratio_summary_path, index=False)

    plot_post_filter_compare(post_filter_png, overall_summary)
    plot_runtime_compare(runtime_png, runtime)
    plot_distribution_compare(distribution_png, samples)
    plot_ratio_compare(ratio_png, ratio_runtime)

    print("Read {} input CSV files".format(len(input_paths)))
    print("Wrote method summary: {}".format(method_summary_path))
    print("Wrote overall summary: {}".format(overall_summary_path))
    print("Wrote runtime aggregation: {}".format(runtime_path))
    print("Wrote ratio samples: {}".format(ratio_samples_path))
    print("Wrote ratio summary: {}".format(ratio_summary_path))
    print(
        "Wrote plots: {}, {}, {}".format(post_filter_png, runtime_png, distribution_png)
    )
    if not ratio_runtime.empty:
        print("Wrote ratio plot: {}".format(ratio_png))

    print("\nPost-filter ranking by weighted throughput:")
    ranking = overall_summary.sort_values(
        "post_filter_weighted_avg_events_per_sec", ascending=False
    )
    for _, row in ranking.iterrows():
        print(
            "{} ({}): {:.1f} events/s, duration={:.2f} min, post/source={:.4f}%".format(
                row["method_label"],
                row["experiment"],
                row["post_filter_weighted_avg_events_per_sec"],
                row["post_filter_duration_min"],
                row["post_filter_source_event_ratio_pct"],
            )
        )


if __name__ == "__main__":
    main()
