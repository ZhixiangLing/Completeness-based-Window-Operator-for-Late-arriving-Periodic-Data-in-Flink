package ge.thesis.model;

public class WindowResult {
    public String meterId;
    public String seriesId;     // Use Case 1: voltage phase (e.g. "Spänning L1")
    public String windowStart;
    public String windowEnd;
    public int count;

    // Heterogeneous fields populated depending on which scenario constructor is used.
    public double sumValue;
    public boolean faultIndicator;

    public String completeness;
    public String resultType;
    public String finalizationReason;

    /** Scenario tag controlling the toString() routing (BASELINE, TRIGGER_TEST, USE_CASE_1). */
    public String scenario;

    /** Use Case 1: wall-clock time at which Flink emitted this result (System.currentTimeMillis()). */
    public long processingTimeMs;

    /** Use Case 1: pipeline mode label ("PROPOSED" or "BASELINE"). */
    public String mode;

    // Use Case 1 evaluation: voltage values inside the finalized window.
    public String voltageData;

    // Evaluation: event-time based window finalization latency metrics
    public long windowStartMs;
    public long windowEndMs;
    public long watermarkAtPurgeMs;
    public long allowedLatenessMs;
    public long finalizationLatencyMs;
    public long latencyReductionMs;

    // Evaluation: number of distinct expected slots observed in this window.
    //   single-phase: distinct 15-min slot indices,            range 0..8
    //   three-phase:  distinct (slot, phase) pair indices,     range 0..24
    // Computed via BitSet in AggregationCase1.add() and exported by
    // OutputCountAggregator into output_counts_*.csv as "distinctSlotCount".
    public int distinctSlotCount;

    public WindowResult() {
    }

    /** Constructor for the standalone Baseline test (no completeness fields). */
    public WindowResult(String meterId, String windowStart, String windowEnd, int count, double sumValue) {
        this.meterId = meterId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.sumValue = sumValue;
        this.scenario = "BASELINE";
    }

    /** Constructor for the standalone Completeness-Trigger test (with completeness + resultType). */
    public WindowResult(String meterId, String windowStart, String windowEnd,
                        int count, double sumValue, String completeness, String resultType) {
        this.meterId = meterId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.sumValue = sumValue;
        this.completeness = completeness;
        this.resultType = resultType;
        this.scenario = "TRIGGER_TEST";
    }

    /**
     * Constructor used by Use Case 1 (faulty-meter detection). Includes {@code mode} and
     * {@code seriesId} for the detection-timing comparison across CBW / baselines.
     */
    public WindowResult(String meterId, String seriesId, String windowStart, String windowEnd,
                        int count, boolean faultIndicator, String completeness,
                        String resultType, long processingTimeMs, String mode) {
        this.meterId = meterId;
        this.seriesId = seriesId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.faultIndicator = faultIndicator;
        this.completeness = completeness;
        this.resultType = resultType;
        this.processingTimeMs = processingTimeMs;
        this.mode = mode;
        this.scenario = "USE_CASE_1";
    }

    public void attachFinalizationLatencyMetrics(
            long windowStartMs,
            long windowEndMs,
            long watermarkAtPurgeMs,
            long allowedLatenessMs,
            long finalizationLatencyMs,
            long latencyReductionMs
    ) {
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.watermarkAtPurgeMs = watermarkAtPurgeMs;
        this.allowedLatenessMs = allowedLatenessMs;
        this.finalizationLatencyMs = finalizationLatencyMs;
        this.latencyReductionMs = latencyReductionMs;
    }

    public void attachWindowBounds(long windowStartMs, long windowEndMs) {
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
    }

    public void attachVoltageData(String voltageData) {
        this.voltageData = voltageData;
    }

    public void attachFinalizationReason(String finalizationReason) {
        this.finalizationReason = finalizationReason;
    }

    public void attachDistinctSlotCount(int distinctSlotCount) {
        this.distinctSlotCount = distinctSlotCount;
    }

    @Override
    public String toString() {
        if (scenario == null) {
            return "WindowResult{meterId='" + meterId + "'}";
        }

        switch (scenario) {
            case "BASELINE":
                return String.format("[BASELINE] Window: %s TO %s | Meter: %s | Count: %d | Sum Value: %.2f",
                        windowStart, windowEnd, meterId, count, sumValue);

            case "TRIGGER_TEST":
                return String.format("[%s] Window: %s TO %s | Meter: %s | Count: %d | Completeness: %s | Sum Value: %.2f",
                        resultType, windowStart, windowEnd, meterId, count, completeness, sumValue);

            case "USE_CASE_1":
                // Derive output-type label from mode + resultType. The finalizationReason
                // field distinguishes completeness-driven purge from deadline fallback.
                //
                //   PROPOSED_PURGE  — proposed method fired FIRE_AND_PURGE (state cleared)
                //   PROPOSED_UPDATE — proposed method emitted a preliminary result (state kept)
                //   BASELINE_PURGE  — baseline deadline reached, FIRE_AND_PURGE (state cleared)
                //   BASELINE_UPDATE — baseline fired at window.end (state kept)
                String outputTypeLabel;
                if ("PROPOSED".equals(mode)) {
                    outputTypeLabel = "FINAL".equals(resultType) ? "PROPOSED_PURGE" : "PROPOSED_UPDATE";
                } else {
                    outputTypeLabel = "FINAL".equals(resultType) ? "BASELINE_PURGE" : "BASELINE_UPDATE";
                }
                return String.format(
                        "[%s] Window: %s TO %s | Meter: %s | Series: %s | Count: %d | Completeness: %s | FinalizationReason: %s | Fault: %b | PurgeTimeMin: %.2f",
                        outputTypeLabel, windowStart, windowEnd, meterId, seriesId,
                        count, completeness, finalizationReason, faultIndicator, processingTimeMs / 60_000.0);

            // case "USE_CASE_2": ...

            default:
                return "Unknown Scenario Result";
        }
    }
}
