#!/usr/bin/env python3
"""
Check whether missing expected slots in output_counts can be reconciled with
the droppedRecords total in run_summary.csv.

This is a read-only analysis script. It does not assume a one-to-one mapping
between missing window slots and dropped records; run_summary.csv only contains
a global dropped-record total.
"""

import argparse
import csv
import sys
from collections import defaultdict
from typing import Dict, Iterable, Tuple


THREE_PHASE_SERIES_LABEL = "Spänning L1/L2/L3"
SINGLE_PHASE_EXPECTED_SLOTS = 8
THREE_PHASE_EXPECTED_SLOTS = 24

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare aggregate missing window slots in output_counts with "
            "droppedRecords from run_summary.csv."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output-counts",
        default=(
            "analysis/results_run4_proposed_465m/"
            "output_counts_run4_proposed_465m.csv"
        ),
        help="Path to output_counts_*.csv.",
    )
    parser.add_argument(
        "--run-summary",
        default="analysis/results_run4_proposed_465m/run_summary.csv",
        help="Path to run_summary.csv.",
    )
    parser.add_argument(
        "--missing-basis",
        choices=("distinctSlotCount", "finalCount", "both"),
        default="distinctSlotCount",
        help=(
            "Column used to compute missing slots. distinctSlotCount is usually "
            "the strict completeness basis."
        ),
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="Number of incomplete window examples to print.",
    )
    return parser.parse_args()


def expected_slots(series_id: str) -> int:
    if series_id == THREE_PHASE_SERIES_LABEL:
        return THREE_PHASE_EXPECTED_SLOTS
    return SINGLE_PHASE_EXPECTED_SLOTS


def read_dropped_records(path: str) -> int:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if not rows:
        raise SystemExit("{} has no data rows".format(path))
    if len(rows) > 1:
        print(
            "Warning: {} contains {} rows; using the first row.".format(
                path, len(rows)
            )
        )
    row = rows[0]
    if "droppedRecords" not in row:
        raise SystemExit("{} is missing droppedRecords column".format(path))
    return int(row["droppedRecords"])


def empty_stats() -> Dict[str, int]:
    return {
        "rows": 0,
        "expected": 0,
        "finalCount": 0,
        "distinctSlotCount": 0,
        "missingByFinalCount": 0,
        "missingByDistinctSlotCount": 0,
        "incompleteByFinalCount": 0,
        "incompleteByDistinctSlotCount": 0,
    }


def add_stats(stats: Dict[str, int], expected: int, final_count: int, distinct: int):
    missing_final = max(0, expected - final_count)
    missing_distinct = max(0, expected - distinct)
    stats["rows"] += 1
    stats["expected"] += expected
    stats["finalCount"] += final_count
    stats["distinctSlotCount"] += distinct
    stats["missingByFinalCount"] += missing_final
    stats["missingByDistinctSlotCount"] += missing_distinct
    if missing_final:
        stats["incompleteByFinalCount"] += 1
    if missing_distinct:
        stats["incompleteByDistinctSlotCount"] += 1


def iter_output_rows(path: str) -> Iterable[dict]:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"seriesId", "finalCount", "distinctSlotCount"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(
                "{} is missing required columns: {}".format(
                    path, ", ".join(sorted(missing))
                )
            )
        yield from reader


def analyze_output_counts(path: str, sample_limit: int) -> Tuple[dict, dict, list]:
    total = empty_stats()
    by_phase = defaultdict(empty_stats)
    samples = []

    for row in iter_output_rows(path):
        series_id = row["seriesId"]
        expected = expected_slots(series_id)
        final_count = int(row["finalCount"])
        distinct = int(row["distinctSlotCount"])
        phase = "three_phase" if expected == THREE_PHASE_EXPECTED_SLOTS else "single_phase"

        add_stats(total, expected, final_count, distinct)
        add_stats(by_phase[phase], expected, final_count, distinct)

        if len(samples) < sample_limit and distinct < expected:
            samples.append(
                {
                    "mode": row.get("mode", ""),
                    "meterId": row.get("meterId", ""),
                    "seriesId": series_id,
                    "windowStartMs": row.get("windowStartMs", ""),
                    "windowEndMs": row.get("windowEndMs", ""),
                    "expected": expected,
                    "finalCount": final_count,
                    "distinctSlotCount": distinct,
                    "missingDistinctSlots": expected - distinct,
                }
            )

    return total, dict(by_phase), samples


def print_stats(name: str, stats: Dict[str, int]) -> None:
    print(
        "{name}: rows={rows:,}, expected={expected:,}, finalCount={final:,}, "
        "distinctSlotCount={distinct:,}, missingByFinalCount={missing_final:,}, "
        "missingByDistinctSlotCount={missing_distinct:,}, "
        "incompleteByFinalCount={incomplete_final:,}, "
        "incompleteByDistinctSlotCount={incomplete_distinct:,}".format(
            name=name,
            rows=stats["rows"],
            expected=stats["expected"],
            final=stats["finalCount"],
            distinct=stats["distinctSlotCount"],
            missing_final=stats["missingByFinalCount"],
            missing_distinct=stats["missingByDistinctSlotCount"],
            incomplete_final=stats["incompleteByFinalCount"],
            incomplete_distinct=stats["incompleteByDistinctSlotCount"],
        )
    )


def selected_missing(stats: Dict[str, int], basis: str) -> int:
    if basis == "finalCount":
        return stats["missingByFinalCount"]
    if basis == "distinctSlotCount":
        return stats["missingByDistinctSlotCount"]
    raise ValueError("Use selected_missing only with a single basis")


def main() -> None:
    args = parse_args()
    dropped_records = read_dropped_records(args.run_summary)
    total, by_phase, samples = analyze_output_counts(
        args.output_counts, args.sample_limit
    )

    print("Input files")
    print("  output_counts: {}".format(args.output_counts))
    print("  run_summary  : {}".format(args.run_summary))
    print()

    print("Aggregate totals")
    print_stats("all_windows", total)
    for phase in sorted(by_phase):
        print_stats(phase, by_phase[phase])
    print()

    print("Dropped-record reconciliation")
    print("  droppedRecords: {:,}".format(dropped_records))
    if args.missing_basis == "both":
        for basis in ("distinctSlotCount", "finalCount"):
            missing_total = selected_missing(total, basis)
            print(
                "  missing by {basis}: {missing:,} | diff missing-dropped: {diff:,}".format(
                    basis=basis,
                    missing=missing_total,
                    diff=missing_total - dropped_records,
                )
            )
    else:
        missing_total = selected_missing(total, args.missing_basis)
        print("  missing basis : {}".format(args.missing_basis))
        print("  missing slots : {:,}".format(missing_total))
        print("  difference   : {:,}".format(missing_total - dropped_records))
        print("  matches      : {}".format(missing_total == dropped_records))
    print()

    print("Interpretation")
    print(
        "  A match would only show that the global totals reconcile. It would not "
        "prove per-window correspondence because run_summary.csv does not retain "
        "meterId/seriesId/window timestamps for dropped records."
    )
    print(
        "  A mismatch means missing expected window slots cannot be explained solely "
        "by the global droppedRecords counter."
    )

    if samples:
        print()
        print("Sample incomplete windows")
        fieldnames = [
            "mode",
            "meterId",
            "seriesId",
            "windowStartMs",
            "windowEndMs",
            "expected",
            "finalCount",
            "distinctSlotCount",
            "missingDistinctSlots",
        ]
        print(",".join(fieldnames))
        for sample in samples:
            print(",".join(str(sample[field]) for field in fieldnames))


if __name__ == "__main__":
    main()
