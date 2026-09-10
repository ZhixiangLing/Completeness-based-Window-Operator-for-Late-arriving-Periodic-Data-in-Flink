#!/usr/bin/env python3
"""
Compare profiling outputs across baseline / proposed / fast / delayed modes.

Every per-mode input is optional. The script discovers which modes are present
from the CLI arguments and only loads / summarises / plots those. If both
baseline and proposed are present, the summary CSV/MD additionally contains a
delta column for proposed-vs-baseline; otherwise the delta column is omitted.

Style (line widths, colours, layout, stats-box format) is preserved across all
plot functions; nothing is redesigned.
"""

import argparse
import csv
import math
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "matplotlib"))

import matplotlib.pyplot as plt
import pandas as pd

from method_labels import method_label


GC_PATTERN = re.compile(
    r"\[(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{4})\].*?"
    r"Pause (?P<type>Young|Full|Remark|Cleanup).*?"
    r"(?:(?P<before>\d+)[A-Z]?->(?P<after>\d+)[A-Z]?\((?P<capacity>\d+)[A-Z]?\) )?"
    r"(?P<duration>\d+\.\d+)ms"
)

KNOWN_MODES = ("fast", "delayed", "baseline", "proposed")
MODE_ORDER = KNOWN_MODES   # ordering used for legend, summary columns, stats box

# Colours: baseline + proposed unchanged. fast / delayed reuse the existing
# palette family (#54a24b green is already used in plot_finalization for
# the latency-reduction histogram; #e45756 is the matching red).
_FAST_COLOR = "#54a24b"
_DELAYED_COLOR = "#e45756"

MODE_COLORS = {
    "baseline": "#4c78a8",
    "proposed": "#f58518",
    "fast":     _FAST_COLOR,
    "delayed":  _DELAYED_COLOR,
}

# Publication-grade palette and markers, used only by the resource matrix
# figure (plot_resource). Other plots keep their existing MODE_COLORS so the
# debugging dashboards look unchanged.
_MODE_COLORS_PAPER = {
    "baseline": "#0173B2",   # blue       (Okabe-Ito)
    "proposed": "#DE8F05",   # orange
    "fast":     "#029E73",   # teal
    "delayed":  "#CC78BC",   # purple
}
_MODE_MARKERS = {
    "baseline": "o",
    "proposed": "s",
    "fast":     "^",
    "delayed":  "D",
}
_RESOURCE_SMOOTH_WINDOW = 30   # rolling-mean window in samples (~30s @ 1Hz)
_RESOURCE_MARKER_COUNT = 10    # markers drawn along each smoothed line


def warn(msg: str) -> None:
    print("[warn] " + msg, file=sys.stderr)


@dataclass
class MetricRow:
    category: str
    metric: str
    unit: str
    values: Dict[str, Optional[float]] = field(default_factory=dict)  # mode -> value
    lower_is_better: bool = True


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare system metrics, GC logs, throughput and finalization latency "
                    "across any subset of baseline / proposed / fast / delayed modes. "
                    "All per-mode inputs are optional; the script processes whatever is provided.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    for mode in MODE_ORDER:
        parser.add_argument(f"--{mode}-csv",
                            help=f"Optional {mode} system metrics CSV.")
        parser.add_argument(f"--{mode}-gc-log",
                            help=f"Optional {mode} GC log.")
        parser.add_argument(f"--{mode}-throughput",
                            help=f"Optional {mode} throughput metrics CSV.")
    parser.add_argument("--finalization",
                        help="Optional proposed finalization latency metrics CSV.")
    parser.add_argument("--output-dir", default="analysis/comparison",
                        help="Directory for CSV and plot outputs.")
    parser.add_argument("--drop-first-sec", type=float, default=0.0,
                        help="Drop warm-up seconds from each run.")
    return parser.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def require_columns(df: pd.DataFrame, path: str, columns: Iterable[str]) -> None:
    missing = set(columns) - set(df.columns)
    if missing:
        raise SystemExit("{} is missing required columns: {}".format(path, ", ".join(sorted(missing))))


def load_system_metrics(path: str, mode: str, drop_first_sec: float) -> Tuple[pd.DataFrame, str]:
    df = pd.read_csv(path)
    require_columns(df, path, ["timestamp", "cpu_percent"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df = df.sort_values("timestamp").copy()
    df["elapsed_sec"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    if drop_first_sec > 0:
        df = df[df["elapsed_sec"] >= drop_first_sec].copy()
        if df.empty:
            raise SystemExit("{} has no samples after --drop-first-sec".format(path))
        df["elapsed_sec"] = df["elapsed_sec"] - df["elapsed_sec"].iloc[0]

    memory_col = pick_memory_column(df)
    df["mode_for_plot"] = mode
    return df, memory_col


def pick_memory_column(df: pd.DataFrame) -> str:
    for column in ("rss_mib", "memory_used_mib", "heap_used_mib"):
        if column in df.columns:
            return column
    raise SystemExit(
        "System metrics CSV must contain one of: rss_mib, memory_used_mib, heap_used_mib"
    )


def parse_gc_log(path: str, mode: str, drop_first_sec: float) -> pd.DataFrame:
    events = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            match = GC_PATTERN.search(line)
            if not match:
                continue
            before = match.group("before")
            after = match.group("after")
            events.append({
                "timestamp": pd.to_datetime(match.group("timestamp"), utc=True),
                "type": match.group("type"),
                "before_mb": float(before) if before is not None else math.nan,
                "after_mb": float(after) if after is not None else math.nan,
                "capacity_mb": float(match.group("capacity")) if match.group("capacity") else math.nan,
                "duration_ms": float(match.group("duration")),
                "mode": mode,
            })

    df = pd.DataFrame(events)
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "type", "duration_ms", "mode", "elapsed_sec", "recovered_mb"])

    df = df.sort_values("timestamp").copy()
    df["elapsed_sec"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    if drop_first_sec > 0:
        df = df[df["elapsed_sec"] >= drop_first_sec].copy()
        if not df.empty:
            df["elapsed_sec"] = df["elapsed_sec"] - df["elapsed_sec"].iloc[0]
    df["recovered_mb"] = df["before_mb"] - df["after_mb"]
    return df


def pctile(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return math.nan
    return float(clean.quantile(q))


def finite(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return float(value)


def _row(category: str, metric: str, unit: str,
         summaries: Dict[str, Dict[str, float]],
         lower_is_better: bool = True) -> MetricRow:
    """Build a MetricRow by pulling `metric` from each present mode's summary
    dict; missing modes get None. The caller filters which modes are
    'present' by only passing in their summary dicts."""
    values: Dict[str, Optional[float]] = {}
    for mode in MODE_ORDER:
        if mode in summaries:
            values[mode] = finite(summaries[mode].get(metric))
    return MetricRow(category=category, metric=metric, unit=unit,
                     values=values, lower_is_better=lower_is_better)


def system_summary(df: pd.DataFrame, memory_col: str) -> Dict[str, float]:
    duration_sec = float(df["elapsed_sec"].max() - df["elapsed_sec"].min()) if len(df) > 1 else 0.0
    summary = {
        "duration_sec": duration_sec,
        "samples": float(len(df)),
        "cpu_mean": float(df["cpu_percent"].mean()),
        "cpu_p95": pctile(df["cpu_percent"], 0.95),
        "cpu_max": float(df["cpu_percent"].max()),
        "memory_mean": float(df[memory_col].mean()),
        "memory_p95": pctile(df[memory_col], 0.95),
        "memory_max": float(df[memory_col].max()),
    }
    if "memory_percent" in df.columns:
        summary["memory_percent_p95"] = pctile(df["memory_percent"], 0.95)
    if "threads" in df.columns:
        summary["threads_p95"] = pctile(df["threads"], 0.95)
    return summary


def gc_summary(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "gc_events": 0.0,
            "gc_events_per_min": 0.0,
            "gc_total_pause_ms": 0.0,
            "gc_p95_pause_ms": math.nan,
            "gc_max_pause_ms": math.nan,
            "full_gc_events": 0.0,
            "young_gc_events": 0.0,
        }
    duration_min = max(float(df["elapsed_sec"].max() - df["elapsed_sec"].min()) / 60.0, 1e-9)
    return {
        "gc_events": float(len(df)),
        "gc_events_per_min": float(len(df)) / duration_min,
        "gc_total_pause_ms": float(df["duration_ms"].sum()),
        "gc_p95_pause_ms": pctile(df["duration_ms"], 0.95),
        "gc_max_pause_ms": float(df["duration_ms"].max()),
        "full_gc_events": float((df["type"] == "Full").sum()),
        "young_gc_events": float((df["type"] == "Young").sum()),
    }


def parse_probe(probe: str) -> Tuple[str, str]:
    for mode in KNOWN_MODES:
        prefix = mode + "_"
        if probe.startswith(prefix):
            return mode, probe[len(prefix):]
    return "unknown", probe


def load_throughput(path: str, expected_mode: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    require_columns(df, path, ["timestamp", "probe", "countInInterval", "eventsPerSecond", "intervalMs"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df = df.sort_values("timestamp").copy()
    df["elapsed_sec"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    parsed = df["probe"].astype(str).apply(parse_probe)
    df["mode"] = parsed.apply(lambda item: item[0])
    df["stage"] = parsed.apply(lambda item: item[1])
    df.loc[df["mode"] == "unknown", "mode"] = expected_mode
    df["countInInterval"] = pd.to_numeric(df["countInInterval"], errors="coerce")
    df["eventsPerSecond"] = pd.to_numeric(df["eventsPerSecond"], errors="coerce")
    df["intervalMs"] = pd.to_numeric(df["intervalMs"], errors="coerce")
    return df.dropna(subset=["countInInterval", "eventsPerSecond", "intervalMs"])


def throughput_summary(df: pd.DataFrame) -> Dict[str, float]:
    summary = {}
    for stage, group in df.groupby("stage"):
        interval_sec = group["intervalMs"].sum() / 1000.0
        weighted = group["countInInterval"].sum() / interval_sec if interval_sec > 0 else math.nan
        key = stage.replace("-", "_")
        summary["throughput_{}_weighted_eps".format(key)] = float(weighted)
        summary["throughput_{}_p95_eps".format(key)] = pctile(group["eventsPerSecond"], 0.95)
    return summary


def load_finalization(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "resultType" in df.columns:
        df = df[df["resultType"].astype(str).str.upper().str.contains("FINAL|PURGE", regex=True)].copy()
    for column in ("finalizationLatencyMs", "latencyReductionMs", "allowedLatenessMs"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def finalization_summary(df: pd.DataFrame) -> Dict[str, float]:
    required = {"finalizationLatencyMs", "latencyReductionMs", "allowedLatenessMs"}
    if not required.issubset(df.columns):
        return {}
    clean = df.dropna(subset=list(required)).copy()
    if clean.empty:
        return {}
    ratio = clean["latencyReductionMs"] / clean["allowedLatenessMs"].replace(0, math.nan)
    return {
        "finalization_samples": float(len(clean)),
        "finalization_latency_mean_min": float(clean["finalizationLatencyMs"].mean() / 60000.0),
        "finalization_latency_p95_min": pctile(clean["finalizationLatencyMs"] / 60000.0, 0.95),
        "latency_reduction_mean_min": float(clean["latencyReductionMs"].mean() / 60000.0),
        "latency_reduction_p95_min": pctile(clean["latencyReductionMs"] / 60000.0, 0.95),
        "latency_reduction_mean_ratio": float(ratio.mean()),
    }


def _collect_metrics(system_stats: Dict[str, Dict[str, float]],
                     gc_stats: Dict[str, Dict[str, float]],
                     tp_stats: Dict[str, Dict[str, float]],
                     final_stats: Dict[str, float]) -> List[MetricRow]:
    """Build all rows from the per-mode summary dicts. Modes that have no
    summary for a given category are simply absent from `row.values`."""
    rows: List[MetricRow] = []

    if system_stats:
        for metric, unit, lower in [
            ("duration_sec", "sec",  True),
            ("cpu_mean",     "%",    True),
            ("cpu_p95",      "%",    True),
            ("memory_mean",  "MiB",  True),
            ("memory_p95",   "MiB",  True),
            ("memory_max",   "MiB",  True),
        ]:
            rows.append(_row("system", metric, unit, system_stats, lower))

    if gc_stats:
        for metric, unit, lower in [
            ("gc_events_per_min", "events/min", True),
            ("gc_total_pause_ms", "ms",         True),
            ("gc_p95_pause_ms",   "ms",         True),
            ("full_gc_events",    "count",      True),
        ]:
            rows.append(_row("gc", metric, unit, gc_stats, lower))

    if tp_stats:
        # Throughput summary keys vary per stage — union across present modes.
        all_metrics = set()
        for s in tp_stats.values():
            all_metrics.update(s.keys())
        for metric in sorted(all_metrics):
            rows.append(_row("throughput", metric, "events/sec", tp_stats, lower_is_better=False))

    if final_stats:
        # Finalization metrics are proposed-only by design — wrap into the
        # generic _row helper using a single-mode summary dict keyed by "proposed".
        wrapped = {"proposed": final_stats}
        for metric, unit, lower in [
            ("finalization_samples",          "count", False),
            ("finalization_latency_mean_min", "min",   True),
            ("finalization_latency_p95_min",  "min",   True),
            ("latency_reduction_mean_min",    "min",   False),
            ("latency_reduction_p95_min",     "min",   False),
            ("latency_reduction_mean_ratio",  "ratio", False),
        ]:
            rows.append(_row("finalization", metric, unit, wrapped, lower))

    return rows


def _summary_columns(rows: List[MetricRow]) -> Tuple[List[str], bool]:
    """Returns (mode_columns_in_order, include_delta).
    `include_delta` is True only when at least one row has both baseline and
    proposed populated — keeps the delta column out when the comparison pair
    isn't actually present."""
    present_modes = set()
    for r in rows:
        present_modes.update(r.values.keys())
    ordered = [m for m in MODE_ORDER if m in present_modes]
    include_delta = any(
        r.values.get("baseline") is not None and r.values.get("proposed") is not None
        for r in rows
    )
    return ordered, include_delta


def _delta_pair(row: MetricRow) -> Tuple[Optional[float], Optional[float]]:
    """Returns (delta, delta_pct) for proposed-vs-baseline, or (None, None)
    if the pair is missing on this row."""
    b = row.values.get("baseline")
    p = row.values.get("proposed")
    if b is None or p is None:
        return None, None
    delta = p - b
    pct = (delta / b * 100.0) if b != 0 else None
    return delta, pct


def write_summary_csv(path: str, rows: List[MetricRow]) -> None:
    mode_cols, include_delta = _summary_columns(rows)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        header = ["category", "metric", "unit"] + list(mode_cols)
        if include_delta:
            header += ["delta_proposed_vs_baseline", "delta_pct_proposed_vs_baseline"]
        writer.writerow(header)
        for row in rows:
            cells = [row.category, row.metric, row.unit]
            cells += [format_optional(row.values.get(m)) for m in mode_cols]
            if include_delta:
                d, dp = _delta_pair(row)
                cells += [format_optional(d), format_optional(dp)]
            writer.writerow(cells)


def write_summary_md(path: str, rows: List[MetricRow]) -> None:
    mode_cols, include_delta = _summary_columns(rows)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("# Profiling Comparison Summary\n\n")
        if include_delta:
            fh.write("_Delta column compares proposed vs baseline. "
                     "Other modes (when supplied) are shown as absolute values "
                     "and visualised in the accompanying plots._\n\n")
        else:
            fh.write("_Per-mode absolute values; delta column omitted because "
                     "baseline+proposed pair is not both present._\n\n")

        header_cells = ["Category", "Metric", "Unit"] + [method_label(m) for m in mode_cols]
        if include_delta:
            header_cells += ["Delta", "Delta %"]
        fh.write("| " + " | ".join(header_cells) + " |\n")
        align = ["---", "---:", "---:"] + ["---:"] * len(mode_cols)
        if include_delta:
            align += ["---:", "---:"]
        fh.write("|" + "|".join(align) + "|\n")
        for row in rows:
            cells = [row.category, row.metric, row.unit]
            cells += [format_optional(row.values.get(m)) for m in mode_cols]
            if include_delta:
                d, dp = _delta_pair(row)
                cells += [format_optional(d), format_optional(dp)]
            fh.write("| " + " | ".join(cells) + " |\n")


def format_optional(value: Optional[float]) -> str:
    if value is None:
        return ""
    return "{:.4f}".format(value)


def _format_duration_short(seconds: float) -> str:
    """Compact, thesis-friendly duration label (e.g. "4h 1m", "5d 2h", "45m")."""
    if seconds < 60:
        return "{:.0f}s".format(seconds)
    if seconds < 3600:
        return "{:.0f}m".format(seconds / 60.0)
    if seconds < 86400:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return "{}h {:02d}m".format(h, m)
    d = int(seconds // 86400)
    h = int((seconds % 86400) // 3600)
    return "{}d {:02d}h".format(d, h)


# Maps the chosen memory column to a thesis-friendly header line printed
# above the per-mode entries in the memory subplot's stats box.
_MEMORY_METRIC_LABEL = {
    "rss_mib":         "Memory: RSS (resident set size)",
    "memory_used_mib": "Memory: used",
    "heap_used_mib":   "Memory: JVM heap",
}


_STATS_BOX_KW = dict(
    transform=None,            # filled in per-call so we use the right axes
    ha="right", va="top",
    fontsize=8, family="monospace",
    bbox=dict(boxstyle="round,pad=0.4",
              facecolor="white", alpha=0.85, edgecolor="lightgray"),
)


def _stats_box_text_with_max(per_mode_df_col: Dict[str, Tuple[pd.DataFrame, str]],
                             value_unit: str, value_format: str,
                             header: Optional[str] = None) -> str:
    """3-line-per-mode stats text: mode name / scale info / mean+P95+max.
    Used by the CPU and memory subplots. value_unit is appended directly to
    each number (e.g. " MiB", "%"). value_format is a Python format string
    such as "{:,.0f}".
    """
    lines: List[str] = []
    if header:
        lines.append(header)
    for mode, (df, value_col) in per_mode_df_col.items():
        if df.empty or value_col not in df.columns:
            continue
        timestamps = df["timestamp"]
        duration_sec = (timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds()
        intervals = timestamps.diff().dt.total_seconds().dropna()
        interval_med = float(intervals.median()) if len(intervals) else float("nan")
        v = pd.to_numeric(df[value_col], errors="coerce").dropna()
        if v.empty:
            continue
        lines.append(method_label(mode))
        lines.append(
            "N={:,} | {:.1f}s interval | {}".format(
                len(df), interval_med, _format_duration_short(duration_sec))
        )
        lines.append(
            "mean={mean}{u}  P95={p95}{u}  max={mx}{u}".format(
                mean=value_format.format(float(v.mean())),
                p95=value_format.format(float(v.quantile(0.95))),
                mx=value_format.format(float(v.max())),
                u=value_unit,
            )
        )
    return "\n".join(lines)


def _stats_box_text_throughput(per_mode_df: Dict[str, pd.DataFrame],
                               header: Optional[str] = None) -> str:
    """3-line-per-mode stats text for throughput: mode / scale / mean+P95.
    Always uses 'events/s' as the unit (no abbreviations).
    """
    lines: List[str] = []
    if header:
        lines.append(header)
    for mode, df in per_mode_df.items():
        if df.empty or "eventsPerSecond" not in df.columns:
            continue
        timestamps = df["timestamp"]
        duration_sec = (timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds()
        intervals = timestamps.diff().dt.total_seconds().dropna()
        interval_med = float(intervals.median()) if len(intervals) else float("nan")
        v = pd.to_numeric(df["eventsPerSecond"], errors="coerce").dropna()
        if v.empty:
            continue
        lines.append(method_label(mode))
        lines.append(
            "N={:,} | {:.1f}s interval | {}".format(
                len(df), interval_med, _format_duration_short(duration_sec))
        )
        lines.append(
            "mean={:,.0f} events/s  P95={:,.0f} events/s".format(
                float(v.mean()), float(v.quantile(0.95)))
        )
    return "\n".join(lines)


def _draw_stats_box(ax, text: str) -> None:
    """Top-right stats box, shared style across all subplots."""
    if not text:
        return
    ax.text(0.98, 0.97, text,
            transform=ax.transAxes,
            ha="right", va="top",
            fontsize=8, family="monospace",
            bbox=dict(boxstyle="round,pad=0.4",
                      facecolor="white", alpha=0.85, edgecolor="lightgray"))


# --------------------------------------------------------------------------- #
#  Helpers used only by the publication-grade plot_resource matrix figure
# --------------------------------------------------------------------------- #

def _smooth(series: pd.Series, window: int) -> pd.Series:
    """Centred rolling mean; returns the same length as input."""
    return series.rolling(window=window, min_periods=1, center=True).mean()


def _normalized_progress_pct(df: pd.DataFrame) -> pd.Series:
    """Map elapsed_sec to a 0..100 % progress axis for fair cross-run alignment."""
    e = df["elapsed_sec"]
    span = float(e.max() - e.min())
    if span <= 0:
        return pd.Series([0.0] * len(df), index=df.index)
    return (e - e.min()) / span * 100.0


def _despine(ax) -> None:
    """Remove top + right spines (paper-style); avoids needing seaborn."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _save_paper_fig(fig, base_path_no_ext: str, dpi: int = 300) -> List[str]:
    """Save the same figure as PNG (raster, dpi-controlled) and PDF (vector).
    Returns the written paths."""
    paths = []
    for ext in ("png", "pdf"):
        path = base_path_no_ext + "." + ext
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        paths.append(path)
    return paths


def _stats_label_box(ax, text: str) -> None:
    """Smaller, paper-friendly stats label (top-right, single rounded box)."""
    if not text:
        return
    ax.text(0.97, 0.95, text,
            transform=ax.transAxes,
            ha="right", va="top",
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.25",
                      facecolor="white", alpha=0.9, edgecolor="0.7", linewidth=0.6))


def plot_resource(output_path: str,
                  system_data: Dict[str, Tuple[pd.DataFrame, str]]) -> None:
    """Render memory + CPU subplots for whichever modes have data.
    `system_data` maps mode -> (df, memory_column_name).

    Publication-grade 4×2 (or N×2) matrix layout:
      - rows = methods (in MODE_ORDER), columns = [Memory, CPU]
      - sharex=True (0..100% normalized progress); sharey='col' for fair scale
      - rolling-mean smoothing + sparse markers (~10) per curve
      - Okabe-Ito colours + per-mode marker shape, no floating legend
      - P95 / Max stats label inside each subplot (no mean)
      - written as both PNG (raster, dpi=300) and PDF (vector for LaTeX)
    """
    if not system_data:
        warn("no system metrics provided — skipping resource plot")
        return
    present = [m for m in MODE_ORDER if m in system_data]
    n_rows = len(present)
    if n_rows == 0:
        warn("system_data has no recognised modes — skipping resource plot")
        return

    base_path_no_ext = os.path.splitext(output_path)[0]
    fig_height = max(2.5 * n_rows, 3.0)

    with plt.style.context("seaborn-v0_8-paper"):
        fig, axes = plt.subplots(
            n_rows, 2,
            figsize=(8.0, fig_height),
            sharex=True,
            sharey="col",
            squeeze=False,                        # always 2D, even when n_rows == 1
        )

        for i, mode in enumerate(present):
            df, mem_col = system_data[mode]
            color = _MODE_COLORS_PAPER.get(mode, "#444444")
            marker = _MODE_MARKERS.get(mode, "o")
            ax_mem, ax_cpu = axes[i, 0], axes[i, 1]
            n_samples = max(len(df), 1)
            markevery = max(n_samples // _RESOURCE_MARKER_COUNT, 1)
            x_pct = _normalized_progress_pct(df) if not df.empty else pd.Series(dtype=float)

            # ---- Memory ------------------------------------------------------
            if not df.empty and mem_col and mem_col in df.columns:
                mem = pd.to_numeric(df[mem_col], errors="coerce")
                mem_smooth = _smooth(mem, _RESOURCE_SMOOTH_WINDOW)
                ax_mem.plot(x_pct, mem_smooth,
                            color=color, marker=marker,
                            markevery=markevery, markersize=5,
                            linewidth=1.6, label=method_label(mode))
                p95 = float(mem.dropna().quantile(0.95)) if mem.notna().any() else float("nan")
                mx = float(mem.dropna().max()) if mem.notna().any() else float("nan")
                _stats_label_box(ax_mem, "P95 {p95:,.0f} MiB\nMax {mx:,.0f} MiB".format(
                    p95=p95, mx=mx))
            else:
                ax_mem.text(0.5, 0.5, "no data", ha="center", va="center",
                            transform=ax_mem.transAxes, color="0.5")

            ax_mem.set_ylabel("{}\nMemory (MiB)".format(method_label(mode)), fontsize=9)
            _despine(ax_mem)

            # ---- CPU ---------------------------------------------------------
            if not df.empty and "cpu_percent" in df.columns:
                cpu = pd.to_numeric(df["cpu_percent"], errors="coerce")
                cpu_smooth = _smooth(cpu, _RESOURCE_SMOOTH_WINDOW)
                ax_cpu.plot(x_pct, cpu_smooth,
                            color=color, marker=marker,
                            markevery=markevery, markersize=5,
                            linewidth=1.6, label=method_label(mode))
                p95 = float(cpu.dropna().quantile(0.95)) if cpu.notna().any() else float("nan")
                mx = float(cpu.dropna().max()) if cpu.notna().any() else float("nan")
                _stats_label_box(ax_cpu, "P95 {p95:.1f}%\nMax {mx:.1f}%".format(
                    p95=p95, mx=mx))
            else:
                ax_cpu.text(0.5, 0.5, "no data", ha="center", va="center",
                            transform=ax_cpu.transAxes, color="0.5")
            _despine(ax_cpu)

        # Column titles (top row only)
        axes[0, 0].set_title("Memory", loc="left", fontsize=10, fontweight="bold")
        axes[0, 1].set_title("CPU",    loc="left", fontsize=10, fontweight="bold")

        # Shared X label on bottom row
        axes[-1, 0].set_xlabel("Runtime progress (%)")
        axes[-1, 1].set_xlabel("Runtime progress (%)")
        for i in range(n_rows):
            axes[i, 0].set_xlim(0, 100)
            axes[i, 1].set_xlim(0, 100)

        fig.subplots_adjust(hspace=0.35, wspace=0.18)

    written = _save_paper_fig(fig, base_path_no_ext, dpi=300)
    plt.close(fig)
    for p in written:
        info_msg = "  wrote {}".format(p)
        # Use stdout so it shows up alongside the existing main() prints.
        print(info_msg)


def plot_gc(output_path: str, gc_data: Dict[str, pd.DataFrame]) -> None:
    """Render GC pause + event-rate subplots for whichever modes have data."""
    if not gc_data or not any(not df.empty for df in gc_data.values()):
        warn("no GC data provided — skipping GC plot")
        return
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    for mode in MODE_ORDER:
        df = gc_data.get(mode)
        if df is None or df.empty:
            continue
        color = MODE_COLORS.get(mode, "#888")
        axes[0].scatter(df["elapsed_sec"] / 60.0, df["duration_ms"], s=10, alpha=0.45, label=method_label(mode), color=color)
        per_min = df.assign(minute=(df["elapsed_sec"] // 60).astype(int)).groupby("minute").size()
        axes[1].plot(per_min.index, per_min.values, label=method_label(mode), color=color, linewidth=1.4)

    axes[0].set_ylabel("Pause duration (ms)")
    axes[0].set_title("GC pause events", loc="left")
    axes[0].legend()
    axes[1].set_ylabel("GC events/min")
    axes[1].set_xlabel("Elapsed minutes")
    axes[1].set_title("GC event rate", loc="left")
    axes[1].legend()
    fig.suptitle("GC Profile Comparison", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_throughput(output_path: str,
                    tp_data: Dict[str, pd.DataFrame]) -> None:
    """Render per-stage throughput subplots for whichever modes have data."""
    nonempty = {m: df for m, df in tp_data.items() if df is not None and not df.empty}
    if not nonempty:
        warn("no throughput data provided — skipping throughput plot")
        return

    stage_set = set()
    for df in nonempty.values():
        stage_set |= set(df["stage"])
    stages = sorted(stage_set)
    if not stages:
        warn("throughput data has no stages — skipping throughput plot")
        return

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, axes = plt.subplots(len(stages), 1, figsize=(14, max(4, 3.6 * len(stages))), sharex=True)
    if len(stages) == 1:
        axes = [axes]

    for ax, stage in zip(axes, stages):
        per_mode_stage: Dict[str, pd.DataFrame] = {}
        for mode in MODE_ORDER:
            df = nonempty.get(mode)
            if df is None:
                continue
            sub = df[df["stage"] == stage]
            if sub.empty:
                continue
            per_mode_stage[mode] = sub
            ax.plot(
                sub["elapsed_sec"] / 60.0,
                sub["eventsPerSecond"],
                label=method_label(mode),
                color=MODE_COLORS.get(mode, "#888"),
                linewidth=1.0,
                alpha=0.75,
            )
        ax.set_ylabel("Events/sec")
        ax.set_title("Throughput: {}".format(stage), loc="left")
        ax.legend()
        _draw_stats_box(ax, _stats_box_text_throughput(per_mode_stage))

    axes[-1].set_xlabel("Elapsed minutes")
    fig.suptitle("Throughput Comparison", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_finalization(output_path: str, finalization_df: pd.DataFrame) -> bool:
    summary_cols = {"finalizationLatencyMs", "latencyReductionMs"}
    if finalization_df.empty or not summary_cols.issubset(finalization_df.columns):
        return False
    clean = finalization_df.dropna(subset=list(summary_cols)).copy()
    if clean.empty:
        return False

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].hist(clean["finalizationLatencyMs"] / 60000.0, bins=50, color="#f58518", alpha=0.8)
    axes[0].set_title("CBW finalization latency", loc="left")
    axes[0].set_xlabel("Minutes")
    axes[0].set_ylabel("Windows")
    axes[1].hist(clean["latencyReductionMs"] / 60000.0, bins=50, color="#54a24b", alpha=0.8)
    axes[1].set_title("CBW latency reduction vs allowed lateness", loc="left")
    axes[1].set_xlabel("Minutes")
    axes[1].set_ylabel("Windows")
    fig.suptitle("CBW Finalization Advantage", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    return True


def main() -> None:
    args = parse_args()
    ensure_dir(args.output_dir)

    # 1. Discover which modes were provided per data type.
    system_inputs = {m: getattr(args, f"{m}_csv")
                     for m in MODE_ORDER if getattr(args, f"{m}_csv")}
    gc_inputs = {m: getattr(args, f"{m}_gc_log")
                 for m in MODE_ORDER if getattr(args, f"{m}_gc_log")}
    tp_inputs = {m: getattr(args, f"{m}_throughput")
                 for m in MODE_ORDER if getattr(args, f"{m}_throughput")}

    if not system_inputs and not gc_inputs and not tp_inputs and not args.finalization:
        print("[error] no input files provided — pass at least one "
              "--<mode>-{csv,gc-log,throughput} or --finalization", file=sys.stderr)
        return

    # 2. Load whatever's present, gracefully skipping any that fail.
    system_data: Dict[str, Tuple[pd.DataFrame, str]] = {}
    for mode, path in system_inputs.items():
        try:
            df, mem_col = load_system_metrics(path, mode, args.drop_first_sec)
            system_data[mode] = (df, mem_col)
        except Exception as e:
            warn(f"failed to load system metrics for {mode}: {e}")

    gc_data: Dict[str, pd.DataFrame] = {}
    for mode, path in gc_inputs.items():
        try:
            gc_data[mode] = parse_gc_log(path, mode, args.drop_first_sec)
        except Exception as e:
            warn(f"failed to parse GC log for {mode}: {e}")

    tp_data: Dict[str, pd.DataFrame] = {}
    for mode, path in tp_inputs.items():
        try:
            tp_data[mode] = load_throughput(path, mode)
        except Exception as e:
            warn(f"failed to load throughput for {mode}: {e}")

    finalization_df = pd.DataFrame()
    if args.finalization:
        try:
            finalization_df = load_finalization(args.finalization)
        except Exception as e:
            warn(f"failed to load finalization CSV: {e}")

    # 3. Build per-mode summary dicts (only modes whose data loaded).
    system_stats = {m: system_summary(df, col) for m, (df, col) in system_data.items()}
    gc_stats = {m: gc_summary(df) for m, df in gc_data.items()}
    tp_stats = {m: throughput_summary(df) for m, df in tp_data.items()}
    final_stats = finalization_summary(finalization_df) if not finalization_df.empty else {}

    # 4. Summary CSV + MD (variable columns; delta only when both pair modes present).
    rows = _collect_metrics(system_stats, gc_stats, tp_stats, final_stats)
    summary_csv = os.path.join(args.output_dir, "profiling_comparison_summary.csv")
    summary_md = os.path.join(args.output_dir, "profiling_comparison_summary.md")
    write_summary_csv(summary_csv, rows)
    write_summary_md(summary_md, rows)
    print("Wrote summary CSV: {}".format(summary_csv))
    print("Wrote summary Markdown: {}".format(summary_md))

    # 5. Plots — each function silently no-ops with a warn() when it has nothing to render.
    # plot_resource writes both PNG + PDF (paper-grade matrix); it prints the
    # written paths itself, so no extra "Wrote resource plot" line here.
    resource_path = os.path.join(args.output_dir, "resource_comparison.png")
    plot_resource(resource_path, system_data)

    gc_path = os.path.join(args.output_dir, "gc_comparison.png")
    plot_gc(gc_path, gc_data)
    if gc_data and any(not df.empty for df in gc_data.values()):
        print("Wrote GC plot: {}".format(gc_path))

    tp_path = os.path.join(args.output_dir, "throughput_comparison.png")
    plot_throughput(tp_path, tp_data)
    if tp_data and any(not df.empty for df in tp_data.values()):
        print("Wrote throughput plot: {}".format(tp_path))

    if not finalization_df.empty:
        fin_path = os.path.join(args.output_dir, "finalization_advantage.png")
        if plot_finalization(fin_path, finalization_df):
            print("Wrote finalization plot: {}".format(fin_path))


if __name__ == "__main__":
    main()
