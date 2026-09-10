# Experiments Script Usage

This folder contains analysis scripts for throughput, resource usage, GC behavior, and baseline/proposed comparison.

All commands below assume they are executed from the repository root.

Most scripts define editable `DEFAULT_*` paths near the top of the file. You can either run the script directly with those defaults, or override paths with command-line arguments.

## 1. Single-Run Profiling Plot

Script:

```bash
experiments/combined_profiling_analyzer.py
```

Purpose: combine one system metrics CSV and one GC log into a diagnostic plot.

Default command:

```bash
python3 experiments/combined_profiling_analyzer.py
```

Baseline override example:

```bash
python3 experiments/combined_profiling_analyzer.py \
  --csv analysis/results_baseline_465m/system_metrics_baseline_run1.csv \
  --gc-log analysis/results_baseline_465m/gc_baseline_run1.log \
  --output analysis/results_baseline_465m/combined_profiling_run1.png
```

Proposed override example:

```bash
python3 experiments/combined_profiling_analyzer.py \
  --csv analysis/results_proposed_465m/system_metrics_proposed_run1.csv \
  --gc-log analysis/results_proposed_465m/gc_proposed_run1.log \
  --output analysis/results_proposed_465m/combined_profiling_run1.png
```

Output: one PNG containing memory, CPU, and GC pause events for a single run.

## 2. Baseline vs Proposed Profiling Comparison

Script:

```bash
experiments/profiling_comparison_analyzer.py
```

Purpose: compare baseline and proposed runs using system metrics, GC logs, and optionally throughput/finalization metrics.

Default command:

```bash
python3 experiments/profiling_comparison_analyzer.py
```

Optional finalization metrics:

```bash
  --finalization path/to/finalization_latency_metrics.csv
```

Main outputs:

- `profiling_comparison_summary.csv`
- `profiling_comparison_summary.md`
- `resource_comparison.png`
- `gc_comparison.png`
- `throughput_comparison.png`, when throughput inputs are provided.
- `finalization_advantage.png`, when finalization metrics are provided.

## 3. Throughput Analysis

Script:

```bash
experiments/throughput_analysis.py
```

Purpose: analyze one throughput CSV emitted by `ThroughputProbe`, grouped by mode and probe stage.

Default command:

```bash
python3 experiments/throughput_analysis.py
```

Baseline override example:

```bash
python3 experiments/throughput_analysis.py \
  --input analysis/results_baseline_465m/throughput_metrics_run1_baseline_465m.csv \
  --output-dir analysis/results_baseline_465m/throughput_analysis \
  --write-cleaned
```

Proposed override example:

```bash
python3 experiments/throughput_analysis.py \
  --input analysis/results_proposed_465m/throughput_metrics_run1_proposed_465m.csv \
  --output-dir analysis/results_proposed_465m/throughput_analysis \
  --write-cleaned
```

Useful options:

```bash
--drop-first 3
```

Drops the first 3 samples per group to reduce warm-up effects.

Outputs include a mode-specific throughput summary CSV and, with `--write-cleaned`, cleaned per-sample data with parsed mode/stage columns.

## 4. Abstract Resource Trend Plot

Script:

```bash
experiments/resource_trend_analyzer.py
```

Purpose: create simplified runtime-percentage CPU and memory trend plots from per-second system metrics.

Default command:

```bash
python3 experiments/resource_trend_analyzer.py
```

Main outputs:

- `cpu_runtime_percent_usage.png`
- `memory_runtime_percent_usage.png`
- `baseline_runtime_percent_usage.csv`
- `proposed_runtime_percent_usage.csv`
- `abstract_resource_trends.png`
- detailed trend CSVs for CPU and memory.

Default simplified plot behavior:

- Runtime is normalized to `0-100%`.
- One point is produced per `5%` runtime bin.
- CPU uses `P95` by default, to expose high-load tail pressure.
- Memory uses `mean` by default.

Useful options:

```bash
--runtime-bin-percent 10
```

Use fewer points by aggregating every 10% of runtime.

```bash
--cpu-runtime-stat median
```

Use median CPU instead of P95 for the simplified CPU plot.

```bash
--cpu-runtime-stat max
```

Use max CPU per runtime bin.

```bash
--memory-runtime-stat p95
```

Use P95 memory per runtime bin instead of mean.

## 5. Mechanism Benefit and CPU Overview

Script:

```bash
experiments/mechanism_resource_overview.py
```

Purpose: create a two-panel summary figure that connects proposed mechanism behavior with CPU reduction.

The left panel compares:

- baseline total windows
- proposed early-purged windows
- baseline total outputs
- saved outputs, computed as `baseline total outputs - proposed total outputs`

The right panel plots baseline/proposed CPU usage over normalized runtime percentage.

Common command:

```bash
python3 experiments/mechanism_resource_overview.py \
  --baseline-output-counts analysis/results_baseline_465m/output_counts_run1_baseline_465m.csv \
  --proposed-output-counts analysis/results_proposed_465m/output_counts_run1_proposed_465m.csv \
  --baseline-csv analysis/results_baseline_465m/system_metrics_baseline_run1.csv \
  --proposed-csv analysis/results_proposed_465m/system_metrics_proposed_run1.csv \
  --output-dir analysis/mechanism_resource_overview_465m
```

Main outputs:

- `mechanism_reduction.png`
- `cpu_runtime_overview.png`
- `mechanism_resource_summary.csv`
- `baseline_cpu_runtime.csv`
- `proposed_cpu_runtime.csv`

Default behavior:

- CPU curve uses `P95` per runtime bin.
- Runtime is normalized to `0-100%`.
- One point is produced per `5%` runtime bin.

Useful options:

```bash
--cpu-stat median
```

Use median CPU instead of P95.

```bash
--runtime-bin-percent 10
```

Use fewer CPU curve points by aggregating every 10% of runtime.

## 6. Window Finalization Latency Distribution

Script:

```bash
experiments/window_finalization_latency_distribution.py
```

Purpose: show when proposed windows are actually finalized after the window end.

Default command:

```bash
python3 experiments/window_finalization_latency_distribution.py
```

Override example:

```bash
python3 experiments/window_finalization_latency_distribution.py \
  --input analysis/results_proposed_465m/finalization_latency_metrics_run1_proposed_465m.csv \
  --output-dir analysis/window_behavior/latency_distribution \
  --run-id run1
```

Main outputs:

- `window_finalization_latency_distribution_<run>.png`
- `window_finalization_latency_ecdf_<run>.png`
- `window_finalization_latency_summary_<run>.csv`
- `window_finalization_latency_quantiles_<run>.csv`
- `window_finalization_latency_histogram_<run>.csv`

## 7. Multi-Experiment Resource Runtime Comparison

Script:

```bash
experiments/multi_resource_runtime_compare.py
```

Purpose: compare CPU and memory runtime-percent trends across multiple experiment configurations, such as:

- `proposed_465m`
- `fast_0`
- `delayed_465m`
- `baseline_465m`

The input experiment list is defined near the top of the script in `EXPERIMENTS`. Edit that list when folders or filenames change.

Common command:

```bash
python3 experiments/multi_resource_runtime_compare.py
```

Main outputs:

- `cpu_runtime_comparison.png`
- `memory_runtime_comparison.png`
- `runtime_usage_comparison.csv`

Default behavior:

- Runtime is normalized to `0-100%`.
- One point is produced per `5%` runtime bin.
- CPU uses `P95` per runtime bin.
- Memory uses `mean` per runtime bin.
- Missing experiment CSVs are skipped with a warning.

Useful options:

```bash
--cpu-stat median
```

Use median CPU instead of P95.

```bash
--memory-stat p95
```

Use P95 memory instead of mean.

```bash
--runtime-bin-percent 10
```

Use fewer points by aggregating every 10% of runtime.

## 7. Window Behavior Analysis

Script:

```bash
experiments/window_behavior_analyzer.py
```

Purpose: analyze window-level behavior from `faulty_window_counts`, `finalization_latency_metrics`, and `output_counts` CSVs across experiment configurations.

The input experiment list is defined near the top of the script in `EXPERIMENTS`. Edit that list when folders or filenames change.

Common command:

```bash
python3 experiments/window_behavior_analyzer.py
```

Output root:

```text
analysis/window_behavior/
```

Latency outputs:

- `latency/latency__summary.csv`
- `latency/latency__summary_table_by_experiment.png`
- `latency/latency__finalization_latency_mean_by_experiment.png`
- `latency/latency__latency_reduction_mean_by_experiment.png`
- `latency/latency__latency_reduction_ratio_by_experiment.png`
- `latency/latency__latency_reduction_distribution_by_experiment.png`

Output/early-purge outputs:

- `output/output__summary.csv`
- `output/output__emitted_windows_by_experiment.png`
- `output/output__emitted_window_coverage_by_experiment.png`
- `output/output__early_purge_ratio_by_experiment.png`
- `output/output__total_outputs_by_experiment.png`
- `output/output__mean_output_count_by_experiment.png`
- `output/output__output_count_distribution_by_experiment.png`

Faulty-window outputs:

- `faulty/faulty__summary.csv`
- `faulty/faulty__faulty_windows_by_experiment.png`
- `faulty/faulty__unique_faulty_meters_by_experiment.png`
- `faulty/faulty__single_vs_three_phase_faulty_meters_by_experiment.png`
- `faulty/faulty__faulty_windows_over_time_by_experiment.png`
- `faulty/faulty__top_faulty_meters__<experiment>.csv`
