#!/usr/bin/env python3
"""
Compare CPU and memory runtime-percent trends across multiple experiments.

Edit EXPERIMENTS below when the experiment folders/files change. Missing files
are skipped so the script can still run while only some experiments are present.
"""

import argparse
import os
from typing import Dict, List

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import matplotlib.pyplot as plt
import pandas as pd

from method_labels import method_label

EXPERIMENTS = [
    {
        "label": "baseline1",
        "csv": "analysis/results_run9_fast_0/system_metrics_fast_run9.csv",
        "throughput": "analysis/results_run9_fast_0/throughput_metrics_run9_fast_0.csv",
    },
    {
        "label": "baseline2",
        "csv": "analysis/results_run12_delayed_726m/system_metrics_delayed_run12.csv",
        "throughput": "analysis/results_run12_delayed_726m/throughput_metrics_run12_delayed_726m.csv",
    },
    {
        "label": "baseline3",
        "csv": "analysis/results_run12_baseline_726m/system_metrics_baseline_run12.csv",
        "throughput": "analysis/results_run12_baseline_726m/throughput_metrics_run12_baseline_726m.csv",
    },
    {
        "label": "proposed",
        "csv": "analysis/results_run12_proposed_726m/system_metrics_proposed_run12.csv",
        "throughput": "analysis/results_run12_proposed_726m/throughput_metrics_run12_proposed_726m.csv",
    },
]

DEFAULT_OUTPUT_DIR = "analysis/multi_resource_runtime_comparison_run12"
RUN_ID = "run12"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot CPU and memory runtime-percent comparisons for multiple experiment runs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        help="Runtime percentage bin size.",
    )
    parser.add_argument(
        "--cpu-stat",
        default="mean",
        choices=["median", "mean", "p95", "max"],
        help="CPU statistic per runtime bin.",
    )
    parser.add_argument(
        "--memory-stat",
        default="mean",
        choices=["median", "mean", "p95", "max"],
        help="Memory statistic per runtime bin.",
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


def pick_memory_column(df: pd.DataFrame, path: str) -> str:
    for column in ("rss_mib", "memory_used_mib", "heap_used_mib"):
        if column in df.columns:
            return column
    raise SystemExit(
        "{} must contain one of: rss_mib, memory_used_mib, heap_used_mib".format(path)
    )


def stat_func(name: str):
    if name == "median":
        return "median"
    if name == "mean":
        return "mean"
    if name == "max":
        return "max"
    if name == "p95":
        return lambda s: s.quantile(0.95)
    raise ValueError("unknown stat: {}".format(name))


def load_runtime_usage(
    label: str,
    path: str,
    bin_percent: float,
    cpu_stat: str,
    memory_stat: str,
) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"timestamp", "cpu_percent"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(
            "{} is missing required columns: {}".format(
                path, ", ".join(sorted(missing))
            )
        )

    memory_col = pick_memory_column(df, path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["cpu_percent"] = pd.to_numeric(df["cpu_percent"], errors="coerce")
    df[memory_col] = pd.to_numeric(df[memory_col], errors="coerce")
    df = (
        df.dropna(subset=["timestamp", "cpu_percent", memory_col])
        .sort_values("timestamp")
        .copy()
    )
    df = df[(df["cpu_percent"] > 0) & (df[memory_col] > 0)].copy()
    if df.empty:
        raise SystemExit("{} has no usable samples".format(path))

    df["elapsed_sec"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    max_elapsed = df["elapsed_sec"].max()
    df["runtime_percent"] = (
        (df["elapsed_sec"] / max_elapsed) * 100.0 if max_elapsed > 0 else 0.0
    )
    df["runtime_bin"] = ((df["runtime_percent"] // bin_percent) * bin_percent).clip(
        upper=100
    )

    grouped = (
        df.groupby("runtime_bin")
        .agg(
            runtime_percent=("runtime_bin", "first"),
            cpu_usage=("cpu_percent", stat_func(cpu_stat)),
            memory_usage_mib=(memory_col, stat_func(memory_stat)),
            samples=("cpu_percent", "size"),
        )
        .reset_index(drop=True)
        .sort_values("runtime_percent")
    )
    grouped["experiment"] = label
    grouped["source_csv"] = path
    return grouped


def summarize_system_metrics(label: str, path: str) -> Dict[str, float]:
    df = pd.read_csv(path)
    required = {"timestamp", "cpu_percent"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(
            "{} is missing required columns: {}".format(
                path, ", ".join(sorted(missing))
            )
        )

    memory_col = pick_memory_column(df, path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["cpu_percent"] = pd.to_numeric(df["cpu_percent"], errors="coerce")
    df[memory_col] = pd.to_numeric(df[memory_col], errors="coerce")
    df = (
        df.dropna(subset=["timestamp", "cpu_percent", memory_col])
        .sort_values("timestamp")
        .copy()
    )
    df = df[(df["cpu_percent"] > 0) & (df[memory_col] > 0)].copy()
    if df.empty:
        raise SystemExit("{} has no usable samples".format(path))

    runtime_sec = (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]).total_seconds()
    return {
        "experiment": label,
        "runtime_sec": runtime_sec,
        "runtime_min": runtime_sec / 60.0,
        "cpu_mean_percent": df["cpu_percent"].mean(),
        "cpu_p95_percent": df["cpu_percent"].quantile(0.95),
        "cpu_peak_percent": df["cpu_percent"].max(),
        "memory_mean_mib": df[memory_col].mean(),
        "memory_p95_mib": df[memory_col].quantile(0.95),
        "memory_peak_mib": df[memory_col].max(),
        "system_samples": len(df),
    }


def summarize_throughput(path: str) -> Dict[str, float]:
    empty = {
        "source_throughput_weighted_eps": float("nan"),
        "source_throughput_mean_eps": float("nan"),
        "source_throughput_p95_eps": float("nan"),
        "source_events_total": float("nan"),
        "output_emission_rate_weighted_eps": float("nan"),
        "output_emission_rate_mean_eps": float("nan"),
        "output_emission_rate_p95_eps": float("nan"),
        "output_emitted_events_total": float("nan"),
    }
    if not path or not os.path.exists(path):
        return empty

    df = pd.read_csv(path)
    required = {"probe", "countInInterval", "eventsPerSecond", "intervalMs"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(
            "{} is missing required columns: {}".format(
                path, ", ".join(sorted(missing))
            )
        )

    df["countInInterval"] = pd.to_numeric(df["countInInterval"], errors="coerce")
    df["eventsPerSecond"] = pd.to_numeric(df["eventsPerSecond"], errors="coerce")
    df["intervalMs"] = pd.to_numeric(df["intervalMs"], errors="coerce")
    df = df.dropna(
        subset=["probe", "countInInterval", "eventsPerSecond", "intervalMs"]
    ).copy()

    def summarize_probe(
        contains: str,
        weighted_key: str,
        mean_key: str,
        p95_key: str,
        total_key: str,
    ) -> Dict[str, float]:
        probe_df = df[
            df["probe"].astype(str).str.contains(contains, regex=False)
        ].copy()
        if probe_df.empty:
            return {
                weighted_key: float("nan"),
                mean_key: float("nan"),
                p95_key: float("nan"),
                total_key: float("nan"),
            }
        total_events = probe_df["countInInterval"].sum()
        total_interval_ms = probe_df["intervalMs"].sum()
        return {
            weighted_key: (
                (total_events * 1000.0 / total_interval_ms)
                if total_interval_ms
                else float("nan")
            ),
            mean_key: probe_df["eventsPerSecond"].mean(),
            p95_key: probe_df["eventsPerSecond"].quantile(0.95),
            total_key: total_events,
        }

    result = {}
    result.update(
        summarize_probe(
            "_source_after_parse",
            "source_throughput_weighted_eps",
            "source_throughput_mean_eps",
            "source_throughput_p95_eps",
            "source_events_total",
        )
    )
    result.update(
        summarize_probe(
            "_post_window_aggregation",
            "output_emission_rate_weighted_eps",
            "output_emission_rate_mean_eps",
            "output_emission_rate_p95_eps",
            "output_emitted_events_total",
        )
    )
    return result


def percent_change(new_value: float, old_value: float) -> float:
    if pd.isna(new_value) or pd.isna(old_value) or old_value == 0:
        return float("nan")
    return ((new_value - old_value) / old_value) * 100.0


def percent_saving(reference_value: float, proposed_value: float) -> float:
    if pd.isna(reference_value) or pd.isna(proposed_value) or reference_value == 0:
        return float("nan")
    return ((reference_value - proposed_value) / reference_value) * 100.0


def build_performance_comparison(
    summary: pd.DataFrame, proposed_label: str = "proposed"
) -> pd.DataFrame:
    proposed_rows = summary[summary["experiment"] == proposed_label]
    if proposed_rows.empty:
        return pd.DataFrame()
    proposed = proposed_rows.iloc[0]

    rows = []
    for _, reference in summary[summary["experiment"] != proposed_label].iterrows():
        rows.append(
            {
                "comparison": "{} vs {}".format(
                    proposed_label, reference["experiment"]
                ),
                "reference_experiment": reference["experiment"],
                "proposed_experiment": proposed_label,
                "cpu_mean_saving_percent": percent_saving(
                    reference["cpu_mean_percent"], proposed["cpu_mean_percent"]
                ),
                "cpu_p95_saving_percent": percent_saving(
                    reference["cpu_p95_percent"], proposed["cpu_p95_percent"]
                ),
                "cpu_peak_saving_percent": percent_saving(
                    reference["cpu_peak_percent"], proposed["cpu_peak_percent"]
                ),
                "memory_mean_saving_percent": percent_saving(
                    reference["memory_mean_mib"], proposed["memory_mean_mib"]
                ),
                "memory_p95_saving_percent": percent_saving(
                    reference["memory_p95_mib"], proposed["memory_p95_mib"]
                ),
                "memory_peak_saving_percent": percent_saving(
                    reference["memory_peak_mib"], proposed["memory_peak_mib"]
                ),
                "runtime_change_sec": proposed["runtime_sec"]
                - reference["runtime_sec"],
                "runtime_change_percent": percent_change(
                    proposed["runtime_sec"], reference["runtime_sec"]
                ),
                "source_throughput_change_percent": percent_change(
                    proposed["source_throughput_weighted_eps"],
                    reference["source_throughput_weighted_eps"],
                ),
                "output_emission_rate_change_percent": percent_change(
                    proposed["output_emission_rate_weighted_eps"],
                    reference["output_emission_rate_weighted_eps"],
                ),
                "proposed_cpu_mean_percent": proposed["cpu_mean_percent"],
                "reference_cpu_mean_percent": reference["cpu_mean_percent"],
                "proposed_memory_mean_mib": proposed["memory_mean_mib"],
                "reference_memory_mean_mib": reference["memory_mean_mib"],
                "proposed_runtime_sec": proposed["runtime_sec"],
                "reference_runtime_sec": reference["runtime_sec"],
                "proposed_source_throughput_weighted_eps": proposed[
                    "source_throughput_weighted_eps"
                ],
                "reference_source_throughput_weighted_eps": reference[
                    "source_throughput_weighted_eps"
                ],
                "proposed_output_emission_rate_weighted_eps": proposed[
                    "output_emission_rate_weighted_eps"
                ],
                "reference_output_emission_rate_weighted_eps": reference[
                    "output_emission_rate_weighted_eps"
                ],
            }
        )
    return pd.DataFrame(rows)


def plot_metric(
    output_path: str,
    data_by_label: Dict[str, pd.DataFrame],
    metric: str,
    ylabel: str,
    title: str,
) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    fig.patch.set_facecolor("white")

    markers = ["o", "s", "^", "D", "v", "P"]
    for idx, (label, data) in enumerate(data_by_label.items()):
        ax.plot(
            data["runtime_percent"],
            data[metric],
            marker=markers[idx % len(markers)],
            markersize=4,
            linewidth=1.8,
            label=method_label(label),
        )

    ax.set_title(title, fontsize=15)
    ax.set_xlabel("Runtime %")
    ax.set_ylabel(ylabel)
    ax.set_xlim(0, 100)
    ax.set_xticks(range(0, 101, 10))
    ax.grid(True, alpha=0.4)
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, -0.18),
        ncol=min(len(data_by_label), 4),
        frameon=False,
    )
    fig.tight_layout(pad=0.3)
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    ensure_dir(args.output_dir)

    data_by_label: Dict[str, pd.DataFrame] = {}
    performance_rows: List[Dict[str, float]] = []
    skipped: List[str] = []
    for experiment in EXPERIMENTS:
        label = experiment["label"]
        path = experiment["csv"]
        if not os.path.exists(path):
            skipped.append("{} -> {}".format(label, path))
            continue
        data_by_label[label] = load_runtime_usage(
            label,
            path,
            args.runtime_bin_percent,
            args.cpu_stat,
            args.memory_stat,
        )
        row = summarize_system_metrics(label, path)
        row.update(summarize_throughput(experiment.get("throughput")))
        performance_rows.append(row)

    if not data_by_label:
        raise SystemExit(
            "No experiment CSVs were found. Edit EXPERIMENTS in this script."
        )

    combined = pd.concat(data_by_label.values(), ignore_index=True)
    combined_path = os.path.join(
        args.output_dir, output_name("runtime_usage_comparison.csv", args.run_id)
    )
    combined.to_csv(combined_path, index=False)

    performance_summary = pd.DataFrame(performance_rows)
    performance_summary_path = os.path.join(
        args.output_dir, output_name("experiment_performance_summary.csv", args.run_id)
    )
    performance_summary.to_csv(performance_summary_path, index=False)

    proposed_comparison = build_performance_comparison(performance_summary)
    proposed_comparison_path = os.path.join(
        args.output_dir, output_name("proposed_performance_comparison.csv", args.run_id)
    )
    proposed_comparison.to_csv(proposed_comparison_path, index=False)

    cpu_plot = os.path.join(
        args.output_dir, output_name("cpu_runtime_comparison.png", args.run_id)
    )
    memory_plot = os.path.join(
        args.output_dir, output_name("memory_runtime_comparison.png", args.run_id)
    )
    plot_metric(
        cpu_plot,
        data_by_label,
        "cpu_usage",
        "CPU Usage (%)",
        "CPU over Runtime ({})".format(args.cpu_stat.upper()),
    )
    plot_metric(
        memory_plot,
        data_by_label,
        "memory_usage_mib",
        "Memory Usage (MiB)",
        "Memory over Runtime ({})".format(args.memory_stat.upper()),
    )

    print("Wrote combined CSV: {}".format(combined_path))
    print("Wrote performance summary CSV: {}".format(performance_summary_path))
    print("Wrote proposed comparison CSV: {}".format(proposed_comparison_path))
    print("Wrote CPU plot: {}".format(cpu_plot))
    print("Wrote memory plot: {}".format(memory_plot))
    if skipped:
        print("Skipped missing experiment CSVs:")
        for item in skipped:
            print("  - {}".format(item))


if __name__ == "__main__":
    main()
