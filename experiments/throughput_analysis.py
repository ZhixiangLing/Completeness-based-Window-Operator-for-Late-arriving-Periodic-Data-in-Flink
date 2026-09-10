#!/usr/bin/env python3
"""
throughput_analysis.py
======================

Analyse Flink throughput samples emitted by ThroughputProbe.

Input CSV columns:
  timestamp,probe,countInInterval,eventsPerSecond,intervalMs

The probe name is expected to follow the current application convention:
  <mode>_source_after_parse
  <mode>_post_window_aggregation

Output:
  - Console report grouped by mode and probe stage
  - throughput_summary_<mode>.csv in --output-dir for mode-specific inputs
  - optional cleaned per-sample CSV with parsed mode/stage fields

Examples:
  python3 experiments/throughput_analysis.py \
    --input output/throughput_metrics_proposed.csv \
    --output-dir output/analysis

  python3 experiments/throughput_analysis.py \
    --input output/throughput_metrics_baseline.csv \
    --drop-first 3 \
    --write-cleaned
"""

import argparse
import csv
import logging
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

KNOWN_MODES = ("proposed", "baseline", "delayed", "fast")
DEFAULT_INPUT = (
    "analysis/results_run8_delayed_735m/throughput_metrics_run8_delayed_735m.csv"
)
DEFAULT_OUTPUT_DIR = "analysis/throughput_analysis_run8"


@dataclass
class ThroughputSample:
    timestamp: str
    mode: str
    stage: str
    probe: str
    count: int
    events_per_second: float
    interval_ms: int
    sample_index: int

    @property
    def interval_sec(self) -> float:
        return self.interval_ms / 1000.0


@dataclass
class Summary:
    mode: str
    stage: str
    probe: str
    samples: int
    total_events: int
    total_interval_sec: float
    weighted_avg_eps: float
    mean_eps: float
    min_eps: float
    p25_eps: float
    median_eps: float
    p75_eps: float
    p90_eps: float
    p95_eps: float
    max_eps: float
    stddev_eps: float
    cv_percent: float
    first_timestamp: str
    last_timestamp: str


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyse a single-mode throughput CSV produced by Flink ThroughputProbe.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Input throughput CSV path",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for analysis CSV outputs",
    )
    parser.add_argument(
        "--drop-first",
        type=int,
        default=0,
        metavar="N",
        help="Drop the first N samples per group to remove warm-up effects",
    )
    parser.add_argument(
        "--min-interval-ms",
        type=int,
        default=1,
        help="Ignore rows whose intervalMs is smaller than this value",
    )
    parser.add_argument(
        "--write-cleaned",
        action="store_true",
        help="Also write throughput_samples_cleaned.csv with parsed mode/stage fields",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    args = parser.parse_args()
    if args.drop_first < 0:
        parser.error("--drop-first must be >= 0")
    if args.min_interval_ms <= 0:
        parser.error("--min-interval-ms must be > 0")
    return args


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_probe(probe: str) -> Tuple[str, str]:
    for mode in KNOWN_MODES:
        prefix = mode + "_"
        if probe.startswith(prefix):
            return mode, probe[len(prefix) :]
    return "unknown", probe


def parse_int(value: str, field_name: str, row_number: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "row {} has invalid {}={!r}".format(row_number, field_name, value)
        ) from exc


def parse_float(value: str, field_name: str, row_number: int) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "row {} has invalid {}={!r}".format(row_number, field_name, value)
        ) from exc


def read_samples(input_path: str, min_interval_ms: int) -> List[ThroughputSample]:
    if not os.path.exists(input_path):
        raise SystemExit("Input CSV does not exist: {}".format(input_path))

    samples = []
    per_probe_counts: Dict[str, int] = defaultdict(int)

    with open(input_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        required = {
            "timestamp",
            "probe",
            "countInInterval",
            "eventsPerSecond",
            "intervalMs",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(
                "Input CSV is missing required columns: {}".format(
                    ", ".join(sorted(missing))
                )
            )

        skipped = 0
        for row_number, row in enumerate(reader, start=2):
            probe = (row.get("probe") or "").strip()
            if not probe:
                skipped += 1
                logging.debug("Skipping row %d with empty probe", row_number)
                continue

            try:
                count = parse_int(
                    row.get("countInInterval", ""), "countInInterval", row_number
                )
                eps = parse_float(
                    row.get("eventsPerSecond", ""), "eventsPerSecond", row_number
                )
                interval_ms = parse_int(
                    row.get("intervalMs", ""), "intervalMs", row_number
                )
            except ValueError as exc:
                skipped += 1
                logging.warning("%s; skipping row", exc)
                continue

            if interval_ms < min_interval_ms:
                skipped += 1
                logging.debug(
                    "Skipping row %d because intervalMs=%d < %d",
                    row_number,
                    interval_ms,
                    min_interval_ms,
                )
                continue

            mode, stage = parse_probe(probe)
            per_probe_counts[probe] += 1
            samples.append(
                ThroughputSample(
                    timestamp=(row.get("timestamp") or "").strip(),
                    mode=mode,
                    stage=stage,
                    probe=probe,
                    count=count,
                    events_per_second=eps,
                    interval_ms=interval_ms,
                    sample_index=per_probe_counts[probe],
                )
            )

    logging.info("Loaded %d throughput samples from %s", len(samples), input_path)
    if skipped:
        logging.info("Skipped %d malformed or filtered rows", skipped)
    return samples


def percentile_from_sorted(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if p <= 0:
        return values[0]
    if p >= 100:
        return values[-1]
    k = (len(values) - 1) * (p / 100.0)
    floor_i = math.floor(k)
    ceil_i = math.ceil(k)
    if floor_i == ceil_i:
        return values[int(k)]
    return values[floor_i] * (ceil_i - k) + values[ceil_i] * (k - floor_i)


def mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def stddev(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def group_samples(
    samples: Iterable[ThroughputSample],
) -> Dict[Tuple[str, str, str], List[ThroughputSample]]:
    grouped: Dict[Tuple[str, str, str], List[ThroughputSample]] = defaultdict(list)
    for sample in samples:
        grouped[(sample.mode, sample.stage, sample.probe)].append(sample)
    return grouped


def drop_warmup(
    grouped: Dict[Tuple[str, str, str], List[ThroughputSample]], drop_first: int
) -> List[ThroughputSample]:
    kept = []
    for key in sorted(grouped):
        group = sorted(grouped[key], key=lambda item: item.sample_index)
        kept.extend(group[drop_first:])
    return kept


def summarize_group(
    mode: str, stage: str, probe: str, group: List[ThroughputSample]
) -> Summary:
    group = sorted(group, key=lambda item: item.sample_index)
    eps_values = sorted(sample.events_per_second for sample in group)
    total_events = sum(sample.count for sample in group)
    total_interval_sec = sum(sample.interval_sec for sample in group)
    weighted_avg_eps = (
        total_events / total_interval_sec if total_interval_sec > 0 else 0.0
    )
    mean_eps = mean([sample.events_per_second for sample in group])
    stddev_eps = stddev([sample.events_per_second for sample in group])
    cv_percent = (stddev_eps / mean_eps) * 100.0 if mean_eps > 0 else 0.0

    return Summary(
        mode=mode,
        stage=stage,
        probe=probe,
        samples=len(group),
        total_events=total_events,
        total_interval_sec=total_interval_sec,
        weighted_avg_eps=weighted_avg_eps,
        mean_eps=mean_eps,
        min_eps=percentile_from_sorted(eps_values, 0),
        p25_eps=percentile_from_sorted(eps_values, 25),
        median_eps=percentile_from_sorted(eps_values, 50),
        p75_eps=percentile_from_sorted(eps_values, 75),
        p90_eps=percentile_from_sorted(eps_values, 90),
        p95_eps=percentile_from_sorted(eps_values, 95),
        max_eps=percentile_from_sorted(eps_values, 100),
        stddev_eps=stddev_eps,
        cv_percent=cv_percent,
        first_timestamp=group[0].timestamp if group else "",
        last_timestamp=group[-1].timestamp if group else "",
    )


def summarize(samples: List[ThroughputSample]) -> List[Summary]:
    summaries = []
    for (mode, stage, probe), group in sorted(group_samples(samples).items()):
        summaries.append(summarize_group(mode, stage, probe, group))
    return summaries


def fmt_eps(value: float) -> str:
    return "{:,.2f}".format(value)


def print_report(summaries: List[Summary], dropped: int) -> None:
    print()
    print("=" * 78)
    print("  Throughput Analysis")
    print("=" * 78)
    if dropped:
        print("  Warm-up samples dropped per group: {}".format(dropped))
    print()

    if not summaries:
        print("No throughput samples to report.")
        return

    print("=== Summary by Probe ===")
    for summary in summaries:
        print()
        print("  [{}] {}".format(summary.mode, summary.stage))
        print("    samples              : {}".format(summary.samples))
        print("    total events         : {:,}".format(summary.total_events))
        print("    measured duration    : {:.3f} s".format(summary.total_interval_sec))
        print(
            "    weighted avg         : {} events/s".format(
                fmt_eps(summary.weighted_avg_eps)
            )
        )
        print(
            "    mean / median        : {} / {} events/s".format(
                fmt_eps(summary.mean_eps),
                fmt_eps(summary.median_eps),
            )
        )
        print(
            "    p90 / p95 / max      : {} / {} / {} events/s".format(
                fmt_eps(summary.p90_eps),
                fmt_eps(summary.p95_eps),
                fmt_eps(summary.max_eps),
            )
        )
        print(
            "    stddev / CV          : {} events/s / {:.2f}%".format(
                fmt_eps(summary.stddev_eps),
                summary.cv_percent,
            )
        )

    by_mode: Dict[str, Dict[str, Summary]] = defaultdict(dict)
    for summary in summaries:
        by_mode[summary.mode][summary.stage] = summary

    print()
    print("=== Stage Ratios ===")
    printed_ratio = False
    for mode in sorted(by_mode):
        source = by_mode[mode].get("source_after_parse")
        post = by_mode[mode].get("post_window_aggregation")
        if not source or not post or source.weighted_avg_eps <= 0:
            continue
        ratio = post.weighted_avg_eps / source.weighted_avg_eps
        print(
            "  [{}] post_window_aggregation / source_after_parse : {:.4f} ({:.2f}%)".format(
                mode,
                ratio,
                ratio * 100.0,
            )
        )
        printed_ratio = True
    if not printed_ratio:
        print("  Not enough matching source/post-window probes to compute ratios.")


def write_summary_csv(path: str, summaries: List[Summary]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "mode",
                "stage",
                "probe",
                "samples",
                "total_events",
                "total_interval_sec",
                "weighted_avg_events_per_second",
                "mean_events_per_second",
                "min_events_per_second",
                "p25_events_per_second",
                "median_events_per_second",
                "p75_events_per_second",
                "p90_events_per_second",
                "p95_events_per_second",
                "max_events_per_second",
                "stddev_events_per_second",
                "cv_percent",
                "first_timestamp",
                "last_timestamp",
            ]
        )
        for summary in summaries:
            writer.writerow(
                [
                    summary.mode,
                    summary.stage,
                    summary.probe,
                    summary.samples,
                    summary.total_events,
                    "{:.3f}".format(summary.total_interval_sec),
                    "{:.3f}".format(summary.weighted_avg_eps),
                    "{:.3f}".format(summary.mean_eps),
                    "{:.3f}".format(summary.min_eps),
                    "{:.3f}".format(summary.p25_eps),
                    "{:.3f}".format(summary.median_eps),
                    "{:.3f}".format(summary.p75_eps),
                    "{:.3f}".format(summary.p90_eps),
                    "{:.3f}".format(summary.p95_eps),
                    "{:.3f}".format(summary.max_eps),
                    "{:.3f}".format(summary.stddev_eps),
                    "{:.3f}".format(summary.cv_percent),
                    summary.first_timestamp,
                    summary.last_timestamp,
                ]
            )


def write_cleaned_csv(path: str, samples: List[ThroughputSample]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "timestamp",
                "mode",
                "stage",
                "probe",
                "sample_index",
                "count_in_interval",
                "events_per_second",
                "interval_ms",
                "interval_sec",
            ]
        )
        for sample in samples:
            writer.writerow(
                [
                    sample.timestamp,
                    sample.mode,
                    sample.stage,
                    sample.probe,
                    sample.sample_index,
                    sample.count,
                    "{:.3f}".format(sample.events_per_second),
                    sample.interval_ms,
                    "{:.3f}".format(sample.interval_sec),
                ]
            )


def validate_timestamps(samples: List[ThroughputSample]) -> None:
    for sample in samples:
        if not sample.timestamp:
            continue
        try:
            datetime.fromisoformat(sample.timestamp.replace("Z", "+00:00"))
        except ValueError:
            logging.debug("Timestamp is not ISO-8601 parseable: %s", sample.timestamp)


def output_stem(input_path: str) -> str:
    basename = os.path.basename(input_path)
    prefix = "throughput_metrics_"
    suffix = ".csv"
    if basename.startswith(prefix) and basename.endswith(suffix):
        return basename[len(prefix) : -len(suffix)]
    return ""


def output_paths(input_path: str, output_dir: str) -> Tuple[str, str]:
    stem = output_stem(input_path)
    suffix = "_" + stem if stem else ""
    return (
        os.path.join(output_dir, "throughput_summary{}.csv".format(suffix)),
        os.path.join(output_dir, "throughput_samples_cleaned{}.csv".format(suffix)),
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    samples = read_samples(args.input, args.min_interval_ms)
    validate_timestamps(samples)

    grouped_original = group_samples(samples)
    analysed_samples = drop_warmup(grouped_original, args.drop_first)
    dropped_count = len(samples) - len(analysed_samples)
    if dropped_count:
        logging.info("Dropped %d warm-up samples across all groups", dropped_count)

    summaries = summarize(analysed_samples)
    print_report(summaries, args.drop_first)

    ensure_dir(args.output_dir)
    summary_path, cleaned_path = output_paths(args.input, args.output_dir)
    write_summary_csv(summary_path, summaries)
    logging.info("Summary CSV written to %s", summary_path)

    if args.write_cleaned:
        write_cleaned_csv(cleaned_path, analysed_samples)
        logging.info("Cleaned sample CSV written to %s", cleaned_path)


if __name__ == "__main__":
    main()
