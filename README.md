# Smart Meter Windowing Experiments with Apache Flink

This repository contains the implementation and analysis tooling for a smart-meter stream-processing study. The main Apache Flink job compares four event-time window finalization methods on single-phase and three-phase voltage data, using either a live Kafka topic or bounded historical gzip files.

The application detects windows containing sustained zero-voltage readings, records window behavior and late-data loss, measures throughput and resource use, and produces CSV files for the analysis scripts under `experiments/`.

## Technology Stack

- Java 17
- Apache Flink 2.0.1
- Flink Kafka Connector 4.0.1-2.0
- Maven
- Python 3 for resource sampling and result analysis
- Kafka for live input

## Experiment Methods

The command-line `mode` selects one of four methods. The abbreviations below are also used in generated figures.

| Mode | Label | Watermark strategy | Allowed lateness | Window behavior |
| --- | --- | --- | --- | --- |
| `fast` | SW | Strict watermark | `0` | Native window output without additional waiting |
| `delayed` | DWW | Watermark delayed by `timeSetting` | `0` | Native window output with delayed watermark progression |
| `baseline` | ALW | Strict watermark | `timeSetting` | Native window output with allowed lateness |
| `proposed` | CBW | Strict watermark | `timeSetting` | Completeness-aware early firing and final purging |

The three native methods (`fast`, `delayed`, and `baseline`) use the same baseline trigger. It emits once when the watermark closes the window and emits an updated result for every later record that Flink still accepts for that window. In practice, repeated late-record updates apply to `baseline` (ALW), which has a non-zero allowed-lateness period. The `fast` (SW) and `delayed` (DWW) modes use zero allowed lateness, so records arriving after their watermark has closed the window are routed to the late-data output instead of producing updates.

For each created window, `proposed` (CBW) emits at most one `PRELIMINARY` result after reaching the configured completeness threshold, followed by exactly one `FINAL` result. The final result is emitted either when the window reaches 100% completeness or when its allowed-lateness deadline is reached; in both cases, the window state is purged.

The proposed method uses a completeness threshold of 75%. A two-hour window expects eight readings per voltage series because meters report every 15 minutes. A complete single-phase window therefore expects 8 readings, while a complete three-phase window expects 24 readings across L1, L2, and L3.

## Processing Pipeline

The main entry point is `ge.thesis.App`.

1. Load single-phase and three-phase meter IDs from classification CSV files.
2. Read raw meter values from Kafka or historical gzip files.
3. Parse each raw record into a `MeterReading` and assign event-time timestamps and watermarks.
4. Keep `Spänning L1` for single-phase meters and `Spänning L1/L2/L3` for three-phase meters.
5. Group readings by meter ID into two-hour tumbling event-time windows.
6. Apply the selected window method and detect three consecutive zero-voltage readings:
   - Single-phase meters must satisfy the condition on L1.
   - Three-phase meters must satisfy the condition independently on L1, L2, and L3.
7. Write window, fault, throughput, late-record, and run-summary metrics to CSV.

The independent `ge.thesis.operator.MeterClassificationJob` consumes the `cm_meter` Kafka topic and regenerates the single-phase and three-phase meter lookup CSV files.

## Repository Structure

```text
.
├── README.md
├── run_experiment.sh
├── flink-app/
│   ├── pom.xml
│   ├── scripts/
│   │   ├── lateness_profiler/
│   │   ├── plot/
│   │   └── resource_profiler/
│   └── src/main/
│       ├── java/ge/thesis/
│       │   ├── App.java
│       │   ├── config/
│       │   ├── evaluation/
│       │   ├── model/
│       │   ├── operator/
│       │   ├── sink/
│       │   ├── source/
│       │   └── window/
│       └── resources/log4j2.properties
├── experiments/
│   ├── README.md
│   ├── combined_profiling_analyzer.py
│   ├── mechanism_resource_overview.py
│   ├── multi_resource_runtime_compare.py
│   ├── profiling_comparison_analyzer.py
│   ├── resource_trend_analyzer.py
│   ├── standard_throughput_analyzer.py
│   ├── throughput_analysis.py
│   ├── window_behavior_analyzer.py
│   └── window_finalization_latency_distribution.py
├── infra/
│   └── flink-jolokia/Dockerfile
└── analysis/
    └── generated experiment results and figures
```

### Main Java Packages

| Package | Responsibility |
| --- | --- |
| `config` | Window parameters and watermark strategies |
| `evaluation` | Throughput probes, output counts, dropped-record counts, fault counts, and finalization latency |
| `model` | Meter records, metadata, lookup tables, parsing, and window-result models |
| `operator` | The meter-classification job and stream operators |
| `sink` | CSV file sinks |
| `source` | Kafka and bounded historical gzip sources |
| `window` | Voltage aggregation and window-result enrichment |
| `window.trigger` | Completeness-aware and native baseline triggers |

## Configuration and Current Assumptions

Core window parameters are defined in `flink-app/src/main/java/ge/thesis/config/WindowConfig.java`:

| Parameter | Current value |
| --- | --- |
| Window size | 2 hours |
| Default allowed lateness | 50 hours |
| Completeness threshold | 0.75 |
| Expected readings per series per window | 8 |
| Job parallelism | 1 |

Several deployment values are currently hard-coded and must match the execution environment:

- Meter classification files:
  - `/data/ling_zhixiang/meter_classification/single_phase_meters.csv`
  - `/data/ling_zhixiang/meter_classification/three_phase_meters.csv`
- Kafka bootstrap server: `psr-kafkabroker:9094`
- Kafka meter-value topic: `cm_meter_values`
- Kafka consumer group: `thesis-experiment-group`
- Default historical input glob: `/data/RawData/raw_meter_data.log-*.gz`

The repository contains a Jolokia-enabled Flink image definition, but it does not currently contain a Docker Compose configuration.

## Build

Build the shaded application JAR from `flink-app/`:

```bash
cd flink-app
mvn clean package -DskipTests
```

The generated application artifact is:

```text
flink-app/target/flink-app-1.0-SNAPSHOT.jar
```

## Run the Main Application

The application accepts positional arguments in this order:

```text
[runId] [mode] [timeSetting] [source] [gzipGlob] [fromDate] [toDate] [outDir]
```

| Position | Argument | Default | Description |
| --- | --- | --- | --- |
| 1 | `runId` | `default_run` | Identifier included in output filenames |
| 2 | `mode` | `proposed` | `proposed`, `baseline`, `fast`, or `delayed` |
| 3 | `timeSetting` | `50h` | Allowed lateness for `proposed`/`baseline`, watermark delay for `delayed`, ignored by `fast` |
| 4 | `source` | `kafka` | `kafka` or `gzip` |
| 5 | `gzipGlob` | `/data/RawData/raw_meter_data.log-*.gz` | Historical gzip file glob |
| 6 | `fromDate` | none | Inclusive `YYYYMMDD` lower date bound for gzip input |
| 7 | `toDate` | none | Inclusive `YYYYMMDD` upper date bound for gzip input |
| 8 | `outDir` | `output/results` | Directory for generated CSV files |

Supported duration formats are `0`, `Nh`, `Nm`, and `Nd`, for example `0`, `50h`, `735m`, or `7d`.

### Historical Gzip Example

From the repository root:

```bash
java -Xmx16g \
  -cp flink-app/target/flink-app-1.0-SNAPSHOT.jar \
  ge.thesis.App \
  run1 proposed 735m gzip \
  '/data/RawData/raw_meter_data.log-*.gz' \
  20250416 20250420 \
  /data/ling_zhixiang/results_run1_proposed_735m
```

### Kafka Example

Arguments after `source` are irrelevant when Kafka input and the default output directory are used:

```bash
java -cp flink-app/target/flink-app-1.0-SNAPSHOT.jar \
  ge.thesis.App run1 baseline 50h kafka
```

### Experiment Runner

`run_experiment.sh` builds the application, starts a bounded gzip experiment with GC logging, and launches the Linux process-level resource sampler.

```bash
./run_experiment.sh [runId] [mode] [timeSetting] [blacklistPath]
```

Example:

```bash
./run_experiment.sh run1 proposed 735m
```

The script is tailored to the current server layout. It uses fixed classification, historical-data, date-range, heap-size, and output-root settings. The optional `blacklistPath` is displayed by the runner but is not consumed by `ge.thesis.App`.

### Regenerate Meter Classification Files

Run the independent classification job only when the meter lookup CSV files need to be rebuilt:

```bash
cd flink-app
mvn exec:java -Dexec.mainClass=ge.thesis.operator.MeterClassificationJob
```

This job reads the `cm_meter` Kafka topic and writes `output/single_phase_meters.csv` and `output/three_phase_meters.csv` relative to its working directory. The main application does not read these relative files directly; copy the generated files to the hard-coded classification paths listed above before running an experiment.

## Output Files

Per-run output names use this identifier sequence:

```text
<metric>_<runId>_<mode>_<timeSetting>.csv
```

| Output | Modes | Purpose |
| --- | --- | --- |
| `output_counts_<runId>_<mode>_<timeSetting>.csv` | All | Per-window output count, early-purge status, final count, and distinct slot count |
| `faulty_window_counts_<runId>_<mode>_<timeSetting>.csv` | `proposed` | Finalized faulty windows and cumulative unique faulty-meter counts |
| `native_faulty_window_counts_<runId>_<mode>_<timeSetting>.csv` | `baseline`, `fast`, `delayed` | Faulty native window emissions |
| `finalization_latency_metrics_<runId>_<mode>_<timeSetting>.csv` | `proposed` | Physical window finalization latency and saved lateness |
| `throughput_metrics_<runId>_<mode>_<timeSetting>.csv` | All | Throughput after parsing and after window aggregation |
| `standard_throughput_metrics_<runId>_<mode>_<timeSetting>.csv` | All | Throughput after source ingestion and after meter/series filtering |
| `ingestion_progress.csv` | Gzip source | Historical source progress by input file |
| `run_summary.csv` | All | Appended run-level input, dropped-record, and timing summary |

The process sampler used by `run_experiment.sh` additionally writes:

```text
system_metrics_<mode>_<runId>.csv
```

GC logs are written as:

```text
gc_<mode>_<runId>.log
```

### Throughput Probe Stages

The two throughput files intentionally measure different boundaries:

| File | Probe stages |
| --- | --- |
| `standard_throughput_metrics_*` | `source_after_ingestion`, `post_filter` |
| `throughput_metrics_*` | `source_after_parse`, `post_window_aggregation` |

Both use the schema:

```text
timestamp,probe,countInInterval,eventsPerSecond,intervalMs
```

## Resource Profiling

`flink-app/scripts/resource_profiler/local_java_process_sampler.py` samples a local Linux Java process through `/proc`. It requires no Docker daemon, Jolokia endpoint, or additional Python package.

List candidate Java processes:

```bash
python3 flink-app/scripts/resource_profiler/local_java_process_sampler.py --list
```

Sample by PID:

```bash
python3 flink-app/scripts/resource_profiler/local_java_process_sampler.py \
  --mode proposed \
  --run-id run1 \
  --target java_local \
  --pid 12345 \
  --interval-sec 1 \
  --output output/system_metrics_proposed_run1.csv
```

The sampler records CPU percentage, resident memory, virtual memory, memory percentage, and thread count.

## Analysis Scripts

Run analysis commands from the repository root. Most scripts provide editable default paths and command-line overrides. See `experiments/README.md` for detailed options and examples.

| Script | Purpose |
| --- | --- |
| `combined_profiling_analyzer.py` | Combine one system-metrics CSV and one GC log into a diagnostic figure |
| `profiling_comparison_analyzer.py` | Compare resource, GC, throughput, and optional finalization results across methods |
| `throughput_analysis.py` | Summarize one legacy throughput metrics file |
| `standard_throughput_analyzer.py` | Compare standard throughput across the four methods |
| `resource_trend_analyzer.py` | Plot normalized CPU and memory trends |
| `multi_resource_runtime_compare.py` | Compare runtime-normalized resource profiles across experiment configurations |
| `mechanism_resource_overview.py` | Relate window/output behavior to CPU usage |
| `window_behavior_analyzer.py` | Analyze output counts, early purging, latency, and faulty windows |
| `window_finalization_latency_distribution.py` | Plot finalization-latency distributions and deterministic method references |
| `check_window_missing_vs_dropped.py` | Compare missing final windows with dropped-record summaries |

Example:

```bash
python3 experiments/standard_throughput_analyzer.py \
  --output-dir analysis/standard_throughput_comparison
```

Generated tables and figures are normally stored under `analysis/` and should not be treated as application source files.

## Known Limitations

- Deployment paths and the Kafka endpoint are hard-coded for the current research environment.
- The main job always loads meter classification data from the fixed `/data/ling_zhixiang/meter_classification/` paths.
- `run_experiment.sh` is Linux-specific and assumes Maven, Java 17, Python 3, `/proc`, and the `/data` directory layout are available.
- There is currently no automated Java test suite in the repository.
- Full Maven build verification still needs to be performed in an environment where Maven is available.
