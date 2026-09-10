#!/bin/bash

# =================================================================
# Experiment runner script. Compiles the Flink job, launches it with
# GC logging, and starts the Python resource-profiling probe.
# =================================================================

# 1. Argument parsing with default values.
RUN_ID=${1:-run1}
MODE=${2:-proposed}
LATENESS=${3:-735m}
BLACKLIST_PATH=${4:-}


# Meter-type classification lookup tables.
CLASSIFICATION_DIR="/data/ling_zhixiang/meter_classification"

# Output directory for this run.
OUT_DIR="/data/ling_zhixiang/results_${RUN_ID}_${MODE}_${LATENESS}"

echo "=========================================="
if [ -z "$BLACKLIST_PATH" ]; then
    echo "Starting local experiment: mode = $MODE, lateness = $LATENESS, runId = $RUN_ID"
else
    echo "Starting local experiment: mode = $MODE, lateness = $LATENESS, runId = $RUN_ID, blacklist = $BLACKLIST_PATH"
fi
echo "=========================================="

# 2. Preparation.
# Ensure the output directory exists. mkdir resolves the /data symlink and creates the subdir.
mkdir -p "$OUT_DIR"

# 3. Build.
echo "Entering flink-app directory to compile the job..."
cd flink-app || { echo "ERROR: flink-app directory not found"; exit 1; }

mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo "ERROR: Maven build failed. Aborting."
    exit 1
fi

echo "Build succeeded. Returning to project root..."
cd ..  # Important: return to project root so the Java and Python relative paths resolve correctly.

# 4. Launch the Java Flink job.
echo "Launching Java process and recording GC logs..."
# At this point we are at the project root, so ${OUT_DIR} can be used directly.
java -Xmx16g \
     -Xlog:gc*,gc+heap=debug,gc+age=trace,safepoint:file=${OUT_DIR}/gc_${MODE}_${RUN_ID}.log:time,uptime,level,tags:filecount=5,filesize=100M \
     -XX:+HeapDumpOnOutOfMemoryError \
     -XX:HeapDumpPath=${OUT_DIR}/heapdump_${MODE}_${RUN_ID}.hprof \
     -cp flink-app/target/flink-app-1.0-SNAPSHOT.jar ge.thesis.App \
     "$RUN_ID" "$MODE" "$LATENESS" gzip '/data/RawData/raw_meter_data.log-*.gz' 20250416 20250420 "${OUT_DIR}" "${CLASSIFICATION_DIR}" &

JAVA_PID=$!
echo "Java process started in the background, PID = $JAVA_PID"

# 5. Launch the Python resource-profiling probe.
# Script location is flink-app/scripts/resource_profiler/.
echo "Launching Python resource probe..."
python3 flink-app/scripts/resource_profiler/local_java_process_sampler.py \
  --mode "$MODE" \
  --run-id "$RUN_ID" \
  --target java_local \
  --pid "$JAVA_PID" \
  --interval-sec 1 \
  --output "${OUT_DIR}/system_metrics_${MODE}_${RUN_ID}.csv" &

PYTHON_PID=$!
echo "Resource probe started, PID = $PYTHON_PID"
echo "Outputs are being written to: ${OUT_DIR}"

# 6. Wait for the Java job to finish.
echo "Waiting for the Java process to finish (Ctrl+C aborts this wait but leaves the process running)..."
wait $JAVA_PID

echo "Experiment run finished."
