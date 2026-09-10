#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lateness Distribution Analysis — Three-Mode Parallel Version
=============================================================
Single-pass scan that computes three different lateness measurements in parallel:
  • per_meter          — max event_time tracked per meter_id
                         (data characterization: per-meter out-of-orderness)
  • per_meter_series   — max event_time tracked per (meter_id, series_id)
                         (data characterization: per-series out-of-orderness)
  • post_window_delay  — globalMax - windowEnd
                         (matches Flink's drop predicate; use this to pick allowed_lateness)

Output layout:
  lateness_output/
  ├── run.log
  ├── progress.json
  ├── compare_summary.csv         <- side-by-side comparison of the three modes
  ├── per_meter/
  │   ├── summary.csv
  │   ├── per_day_summary.csv
  │   ├── histogram_overall.csv
  │   ├── histogram_YYYYMMDD.csv  (one per day)
  │   ├── cdf_overall.csv
  │   ├── cdf_YYYYMMDD.csv        (one per day)
  │   └── hourly_data.csv
  ├── per_meter_series/           (same layout)
  └── post_window_delay/          (same layout; its p99 is the recommended allowed_lateness)

Memory optimizations:
  • Store event_time as int unix seconds (~40% smaller per entry).
  • sys.intern meter_id / series_id so the per_meter and per_meter_series dicts
    share the same string objects.
  • Histograms are updated in a streaming fashion (no per-day list of raw values).
"""

import bisect
import csv
import gzip
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# ══════════════════════════════════════════════════════════════
#  CONFIG  ← edit these
# ══════════════════════════════════════════════════════════════

INPUT_DIR  = Path("/data/RawData")
DATE_START = date(2025, 4, 16)
DATE_END   = date(2025, 4, 20)
FILE_TMPL  = "raw_meter_data.log-{date}.gz"

OUTPUT_DIR = Path("/data/ling_zhixiang/lateness_output_3modes")

# Which measurement modes to compute.
# The legacy "global" mode (globalMax - event_ts) was dropped: it does not match Flink's actual
# drop predicate (globalMax - windowEnd), so it is not useful for picking allowed_lateness.
# We keep per_meter / per_meter_series for data characterization and post_window_delay for the
# drop-rate prediction.
ENABLE_MODES = ["per_meter", "per_meter_series", "post_window_delay"]

HIST_BINS = [i * 6 for i in range(501)]   # 0..3000 min, 6-min bins

# Must match Flink's WindowConfig.WINDOW_SIZE (2 hours).
# Used by the post_window_delay mode as windowEnd =
#   floor(event_ts / WINDOW_SIZE_SEC) * WINDOW_SIZE_SEC + WINDOW_SIZE_SEC
WINDOW_SIZE_SEC = 2 * 3600

PROGRESS_EVERY = 1_000_000
MAX_RECORDS = None        # Set to e.g. 2_000_000 for a quick sanity-check run.

# ══════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(OUTPUT_DIR / "run.log"), mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("lateness")

# ══════════════════════════════════════════════════════════════
#  HISTOGRAM HELPERS
# ══════════════════════════════════════════════════════════════

N_BINS  = len(HIST_BINS) - 1
_BIN_LO = HIST_BINS[0]
_BIN_HI = HIST_BINS[-1]


def bin_index(lat_min):
    if lat_min < _BIN_LO:
        return 0
    if lat_min >= _BIN_HI:
        return N_BINS - 1
    idx = bisect.bisect_right(HIST_BINS, lat_min) - 1
    if idx < 0:
        return 0
    if idx >= N_BINS:
        return N_BINS - 1
    return idx


def add_histograms(a, b):
    return [x + y for x, y in zip(a, b)]


def hist_percentile(hist_counts, bins, q):
    total = sum(hist_counts)
    if total == 0:
        return None
    threshold = q / 100.0 * total
    cumulative = 0.0
    for i, cnt in enumerate(hist_counts):
        prev = cumulative
        cumulative += cnt
        if cumulative >= threshold:
            return round(bins[i] + ((threshold - prev) / cnt if cnt else 0.0)
                         * (bins[i + 1] - bins[i]), 4)
    return float(bins[-1])


def _linspace(start, stop, n):
    if n <= 1:
        return [float(start)]
    step = (stop - start) / (n - 1)
    return [start + i * step for i in range(n)]


# ══════════════════════════════════════════════════════════════
#  PARSERS
# ══════════════════════════════════════════════════════════════

_struct_re = re.compile(r'(\w+)=([^,}]+)')


def open_file(path):
    p = str(path)
    if p.endswith(".gz"):
        return gzip.open(p, "rt", encoding="utf-8", errors="replace")
    return open(p, "rt", encoding="utf-8", errors="replace")


def parse_struct(line):
    line = line.strip()
    if not line.startswith("Struct{") or not line.endswith("}"):
        return None
    return dict(_struct_re.findall(line))


def parse_datetime(s):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


# ══════════════════════════════════════════════════════════════
#  CSV WRITERS
# ══════════════════════════════════════════════════════════════

def write_histogram_csv(path, hist_counts, bins):
    with open(str(path), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["bin_left_min", "bin_right_min", "count"])
        for left, right, cnt in zip(bins[:-1], bins[1:], hist_counts):
            w.writerow([left, right, cnt])


def write_cdf_csv(path, hist_counts, bins):
    with open(str(path), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["percentile", "lateness_min"])
        for q in _linspace(0, 100, 1001):
            val = hist_percentile(hist_counts, bins, q)
            w.writerow([round(q, 1), val if val is not None else 0.0])


# ══════════════════════════════════════════════════════════════
#  TRACKER
# ══════════════════════════════════════════════════════════════

# A constant key used by modes that maintain a single global max (any hashable works).
_GLOBAL_KEY = 0


def _key_global(m, s):
    return _GLOBAL_KEY


def _key_per_meter(m, s):
    return m


def _key_per_meter_series(m, s):
    return (m, s)


KEY_FNS = {
    "global":            _key_global,
    "per_meter":         _key_per_meter,
    "per_meter_series":  _key_per_meter_series,
    "post_window_delay": _key_global,  # Shares the global key; windowEnd is computed in process().
}


class Tracker:
    """Holds the full state for a single lateness measurement mode."""

    __slots__ = (
        "name", "key_fn", "max_event",
        "day_total", "day_late", "day_hist", "day_sum_lat", "day_max_lat",
        "global_total", "global_late", "hist_counts", "hourly",
        "per_day_summaries",
    )

    def __init__(self, name):
        self.name   = name
        self.key_fn = KEY_FNS[name]
        # max_event[key] = int unix seconds
        self.max_event = {}
        # Per-day accumulators (reset each day in start_day()).
        self.day_total = 0
        self.day_late  = 0
        self.day_hist  = [0] * N_BINS
        self.day_sum_lat = 0.0
        self.day_max_lat = 0.0
        # Cumulative accumulators across all processed days.
        self.global_total = 0
        self.global_late  = 0
        self.hist_counts  = [0] * N_BINS
        self.hourly       = {}    # hr_ts (int unix sec) -> entry dict
        self.per_day_summaries = []

    # ── Reset day_* counters at the start of each day. ──────────
    def start_day(self):
        self.day_total   = 0
        self.day_late    = 0
        self.day_hist    = [0] * N_BINS
        self.day_sum_lat = 0.0
        self.day_max_lat = 0.0

    # ── Record one positive lateness value into day_hist / day_sum / day_max / hourly. ──
    def _record_lateness(self, lat_min, ins_ts):
        self.day_late += 1

        bi = bin_index(lat_min)
        self.day_hist[bi] += 1
        self.day_sum_lat  += lat_min
        if lat_min > self.day_max_lat:
            self.day_max_lat = lat_min

        if ins_ts is not None:
            # Bucket by hour. Using int unix-seconds for the key is smaller than a string.
            hr_ts = (ins_ts // 3600) * 3600
            entry = self.hourly.get(hr_ts)
            if entry is None:
                entry = {
                    "hist":  [0] * N_BINS,
                    "sum":   0.0,
                    "count": 0,
                    "max":   0.0,
                }
                self.hourly[hr_ts] = entry
            entry["hist"][bi] += 1
            entry["sum"]   += lat_min
            entry["count"] += 1
            if lat_min > entry["max"]:
                entry["max"] = lat_min

    # ── Process one record. ────────────────────────────────────
    def process(self, meter_id, series_id, event_ts, ins_ts):
        self.day_total += 1

        # post_window_delay: globalMax(at arrival) - windowEnd(of record).
        # This matches Flink's drop predicate directly, so it has different semantics from
        # the other modes (which use a key-local max) and is handled in its own branch.
        if self.name == "post_window_delay":
            key = self.key_fn(meter_id, series_id)  # Always _GLOBAL_KEY here.
            cur_global_max = self.max_event.get(key)
            if cur_global_max is None or event_ts > cur_global_max:
                self.max_event[key] = event_ts
                cur_global_max = event_ts
            # Same windowEnd formula Flink applies in TumblingEventTimeWindows.of(WINDOW_SIZE).
            window_end_ts = ((event_ts // WINDOW_SIZE_SEC) + 1) * WINDOW_SIZE_SEC
            delay_sec = cur_global_max - window_end_ts
            if delay_sec > 0:
                self._record_lateness(delay_sec / 60.0, ins_ts)
            return

        # Key-local max path for per_meter / per_meter_series (and the legacy global mode).
        key = self.key_fn(meter_id, series_id)
        cur_max = self.max_event.get(key)

        if cur_max is not None and event_ts < cur_max:
            self._record_lateness((cur_max - event_ts) / 60.0, ins_ts)
        else:
            self.max_event[key] = event_ts

    # ── End-of-day reporting and global-state update. ──────────
    def end_day(self, date_tag, output_root):
        mode_dir = output_root / self.name
        mode_dir.mkdir(parents=True, exist_ok=True)

        write_histogram_csv(mode_dir / ("histogram_" + date_tag + ".csv"),
                            self.day_hist, HIST_BINS)
        write_cdf_csv(mode_dir / ("cdf_" + date_tag + ".csv"),
                      self.day_hist, HIST_BINS)

        self.hist_counts = add_histograms(self.hist_counts, self.day_hist)
        self.global_total += self.day_total
        self.global_late  += self.day_late

        if self.day_late > 0:
            day_pcts = {
                "p50_min": hist_percentile(self.day_hist, HIST_BINS, 50),
                "p90_min": hist_percentile(self.day_hist, HIST_BINS, 90),
                "p95_min": hist_percentile(self.day_hist, HIST_BINS, 95),
                "p99_min": hist_percentile(self.day_hist, HIST_BINS, 99),
                "max_min": round(self.day_max_lat, 4),
            }
        else:
            day_pcts = {"p50_min": None, "p90_min": None,
                        "p95_min": None, "p99_min": None, "max_min": None}

        summary = {
            "date":              date_tag,
            "total":             self.day_total,
            "late":              self.day_late,
            "late_ratio":        round(self.day_late / self.day_total, 6) if self.day_total else 0.0,
            "unique_keys":       len(self.max_event),
            "mean_lateness_min": round(self.day_sum_lat / self.day_late, 4) if self.day_late else None,
        }
        summary.update(day_pcts)
        self.per_day_summaries.append(summary)
        log.info("  [%s] day=%s  total=%d  late=%d (%.2f%%)  p99=%s  unique_keys=%d",
                 self.name, date_tag, self.day_total, self.day_late,
                 100.0 * self.day_late / self.day_total if self.day_total else 0.0,
                 day_pcts["p99_min"], len(self.max_event))

    # ── Persist cumulative outputs for this mode. ──────────────
    def save_overall(self, output_root):
        mode_dir = output_root / self.name
        mode_dir.mkdir(parents=True, exist_ok=True)

        # summary.csv
        with open(str(mode_dir / "summary.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "mode", "total_records", "late_records", "late_ratio",
                "unique_keys",
                "p50_min", "p90_min", "p95_min", "p99_min",
            ])
            w.writeheader()
            w.writerow({
                "mode":          self.name,
                "total_records": self.global_total,
                "late_records":  self.global_late,
                "late_ratio":    round(self.global_late / self.global_total, 6) if self.global_total else 0.0,
                "unique_keys":   len(self.max_event),
                "p50_min":  hist_percentile(self.hist_counts, HIST_BINS, 50),
                "p90_min":  hist_percentile(self.hist_counts, HIST_BINS, 90),
                "p95_min":  hist_percentile(self.hist_counts, HIST_BINS, 95),
                "p99_min":  hist_percentile(self.hist_counts, HIST_BINS, 99),
            })

        # per_day_summary.csv
        if self.per_day_summaries:
            keys = list(self.per_day_summaries[0].keys())
            with open(str(mode_dir / "per_day_summary.csv"), "w",
                      newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
                w.writeheader()
                w.writerows(self.per_day_summaries)

        write_histogram_csv(mode_dir / "histogram_overall.csv",
                            self.hist_counts, HIST_BINS)
        write_cdf_csv(mode_dir / "cdf_overall.csv",
                      self.hist_counts, HIST_BINS)

        # hourly_data.csv
        if self.hourly:
            with open(str(mode_dir / "hourly_data.csv"), "w",
                      newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "hour", "count", "mean_min",
                    "p50_min", "p90_min", "p95_min", "p99_min", "max_min",
                ])
                w.writeheader()
                for hr_ts in sorted(self.hourly.keys()):
                    entry = self.hourly[hr_ts]
                    n = entry["count"]
                    if n == 0:
                        continue
                    hr_str = datetime.fromtimestamp(hr_ts).strftime("%Y-%m-%d %H:00")
                    w.writerow({
                        "hour":     hr_str,
                        "count":    n,
                        "mean_min": round(entry["sum"] / n, 4),
                        "p50_min":  hist_percentile(entry["hist"], HIST_BINS, 50),
                        "p90_min":  hist_percentile(entry["hist"], HIST_BINS, 90),
                        "p95_min":  hist_percentile(entry["hist"], HIST_BINS, 95),
                        "p99_min":  hist_percentile(entry["hist"], HIST_BINS, 99),
                        "max_min":  round(entry["max"], 4),
                    })

        log.info("OK  [%s] saved overall outputs to %s", self.name, mode_dir)


# ══════════════════════════════════════════════════════════════
#  PROGRESS
# ══════════════════════════════════════════════════════════════

def save_progress(payload):
    with open(str(OUTPUT_DIR / "progress.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def write_compare_summary(trackers, output_root):
    path = output_root / "compare_summary.csv"
    with open(str(path), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "mode", "total_records", "late_records", "late_ratio",
            "unique_keys",
            "p50_min", "p90_min", "p95_min", "p99_min",
        ])
        w.writeheader()
        for t in trackers:
            w.writerow({
                "mode":          t.name,
                "total_records": t.global_total,
                "late_records":  t.global_late,
                "late_ratio":    round(t.global_late / t.global_total, 6) if t.global_total else 0.0,
                "unique_keys":   len(t.max_event),
                "p50_min":  hist_percentile(t.hist_counts, HIST_BINS, 50),
                "p90_min":  hist_percentile(t.hist_counts, HIST_BINS, 90),
                "p95_min":  hist_percentile(t.hist_counts, HIST_BINS, 95),
                "p99_min":  hist_percentile(t.hist_counts, HIST_BINS, 99),
            })
    log.info("OK  Saved compare_summary.csv  →  %s", path)


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def process_file(filepath, trackers, totals):
    """Single-pass scan of one daily file, feeding each record into every tracker."""
    for t in trackers:
        t.start_day()

    day_errors = 0
    day_processed = 0

    log.info("=" * 60)
    log.info(">  Processing: %s", filepath.name)
    log.info("=" * 60)

    intern = sys.intern  # Local alias for a small hot-loop speedup.

    try:
        with open_file(filepath) as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue

                fields = parse_struct(line)
                if fields is None:
                    day_errors += 1
                    continue

                try:
                    meter_id  = intern(fields["meter_id"])
                    series_id = intern(fields["series_id"])
                    event_dt  = parse_datetime(fields["time"])
                    if event_dt is None:
                        raise ValueError
                    event_ts = int(event_dt.timestamp())
                except (KeyError, ValueError):
                    day_errors += 1
                    continue

                ins_raw = fields.get("insert_time", "")
                ins_ts  = None
                if ins_raw:
                    ins_dt = parse_datetime(ins_raw)
                    if ins_dt is not None:
                        ins_ts = int(ins_dt.timestamp())

                for t in trackers:
                    t.process(meter_id, series_id, event_ts, ins_ts)

                day_processed += 1

                # Progress log every PROGRESS_EVERY records.
                if day_processed % PROGRESS_EVERY == 0:
                    parts = []
                    for t in trackers:
                        parts.append("%s=%.1f%%" % (
                            t.name,
                            100.0 * t.day_late / t.day_total if t.day_total else 0.0))
                    log.info(
                        "[%s] records=%d errors=%d | late ratios: %s",
                        filepath.name, day_processed, day_errors, "  ".join(parts))
                    save_progress({
                        "current_file":  filepath.name,
                        "day_processed": day_processed,
                        "totals":        {**totals,
                                          "current_total": totals["total"] + day_processed},
                    })

                if MAX_RECORDS is not None:
                    if totals["total"] + day_processed >= MAX_RECORDS:
                        log.info("MAX_RECORDS=%d reached — stopping.", MAX_RECORDS)
                        break

    except KeyboardInterrupt:
        log.warning("KeyboardInterrupt during %s", filepath.name)
        # Flush whatever has been processed so far for the current day.
        date_tag = filepath.stem.replace("raw_meter_data.log-", "")
        for t in trackers:
            t.end_day(date_tag, OUTPUT_DIR)
        raise

    totals["total"]  += day_processed
    totals["errors"] += day_errors

    date_tag = filepath.stem.replace("raw_meter_data.log-", "")
    for t in trackers:
        t.end_day(date_tag, OUTPUT_DIR)

    return day_processed, day_errors


def main():
    log.info("=" * 60)
    log.info("Lateness Analysis — Three-mode Parallel (streaming)")
    log.info("  Input dir   : %s", INPUT_DIR)
    log.info("  Date range  : %s -> %s", DATE_START, DATE_END)
    log.info("  Output dir  : %s", OUTPUT_DIR)
    log.info("  Modes       : %s", ENABLE_MODES)
    log.info("  MAX_RECORDS : %s", MAX_RECORDS)
    log.info("=" * 60)

    trackers = [Tracker(name) for name in ENABLE_MODES]

    dates = []
    d = DATE_START
    while d <= DATE_END:
        dates.append(d)
        d += timedelta(days=1)

    totals = {"total": 0, "errors": 0, "days_total": len(dates), "days_done": 0}

    try:
        for i, day in enumerate(dates, 1):
            fname = FILE_TMPL.format(date=day.strftime("%Y%m%d"))
            fpath = INPUT_DIR / fname
            if not fpath.exists():
                log.warning("File not found, skipping: %s", fpath)
                continue

            log.info("Day %d / %d — %s", i, len(dates), fname)
            process_file(fpath, trackers, totals)
            totals["days_done"] = i

            for t in trackers:
                t.save_overall(OUTPUT_DIR)
            write_compare_summary(trackers, OUTPUT_DIR)
            save_progress({"totals": totals, "current_file": fname})
            log.info("Overall outputs updated after day %d.", i)

            if MAX_RECORDS is not None and totals["total"] >= MAX_RECORDS:
                break

    except KeyboardInterrupt:
        log.warning("Interrupted — saving partial results ...")
        for t in trackers:
            t.save_overall(OUTPUT_DIR)
        write_compare_summary(trackers, OUTPUT_DIR)
        save_progress({"totals": totals, "interrupted": True})
        sys.exit(0)

    log.info("=" * 60)
    log.info("DONE.  total_records=%d  parse_errors=%d", totals["total"], totals["errors"])
    log.info("=" * 60)
    for t in trackers:
        t.save_overall(OUTPUT_DIR)
    write_compare_summary(trackers, OUTPUT_DIR)
    save_progress({"totals": totals, "finished": True})


if __name__ == "__main__":
    main()