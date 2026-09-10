package ge.thesis.evaluation;

import ge.thesis.model.WindowResult;

import java.io.Serializable;
import java.time.Duration;
import java.util.Locale;

/**
 * Evaluation helper for event-time-based window finalization latency.
 *
 * L_finalization = W(t_purge) - windowEnd
 * DeltaL = allowedLateness - L_finalization
 */
public class WindowFinalizationLatencyEvaluator implements Serializable {

    public static Metrics evaluate(long watermarkAtPurgeMs, long windowEndMs, Duration allowedLateness) {
        long allowedLatenessMs = allowedLateness.toMillis();
        long finalizationLatencyMs = clamp(watermarkAtPurgeMs - windowEndMs, 0L, allowedLatenessMs);
        long latencyReductionMs = Math.max(0L, allowedLatenessMs - finalizationLatencyMs);

        return new Metrics(
                watermarkAtPurgeMs,
                windowEndMs,
                allowedLatenessMs,
                finalizationLatencyMs,
                latencyReductionMs
        );
    }

    public static String format(WindowResult result) {
        return String.format(
                Locale.ROOT,
                "[FINALIZATION_LATENCY] Mode: %s | Window: %s TO %s | Meter: %s | Series: %s"
                        + " | Count: %d | Completeness: %s | Fault: %b"
                        + " | FinalizationReason: %s"
                        + " | WatermarkAtPurgeMin: %.2f | WindowEndMin: %.2f"
                        + " | AllowedLatenessMin: %.2f | FinalizationLatencyMin: %.2f"
                        + " | LatencyReductionMin: %.2f",
                result.mode,
                result.windowStart,
                result.windowEnd,
                result.meterId,
                result.seriesId,
                result.count,
                result.completeness,
                result.faultIndicator,
                result.finalizationReason,
                toMinutes(result.watermarkAtPurgeMs),
                toMinutes(result.windowEndMs),
                toMinutes(result.allowedLatenessMs),
                result.finalizationLatencyMs / 60_000.0,
                result.latencyReductionMs / 60_000.0
        );
    }

    public static String csvRow(WindowResult result) {
        return String.format(
                Locale.ROOT,
                "%s,%s,%s,%s,%s,%d,%b,%d,%d,%d,%d,%d,%d",
                safeCsv(result.mode),
                safeCsv(result.resultType),
                safeCsv(result.finalizationReason),
                safeCsv(result.meterId),
                safeCsv(result.seriesId),
                result.count,
                result.faultIndicator,
                result.windowStartMs,
                result.windowEndMs,
                result.watermarkAtPurgeMs,
                result.allowedLatenessMs,
                result.finalizationLatencyMs,
                result.latencyReductionMs
        );
    }

    private static double toMinutes(long millis) {
        return millis / 60_000.0;
    }

    private static String safeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }

    public static class Metrics implements Serializable {
        public final long watermarkAtPurgeMs;
        public final long windowEndMs;
        public final long allowedLatenessMs;
        public final long finalizationLatencyMs;
        public final long latencyReductionMs;

        public Metrics(
                long watermarkAtPurgeMs,
                long windowEndMs,
                long allowedLatenessMs,
                long finalizationLatencyMs,
                long latencyReductionMs
        ) {
            this.watermarkAtPurgeMs = watermarkAtPurgeMs;
            this.windowEndMs = windowEndMs;
            this.allowedLatenessMs = allowedLatenessMs;
            this.finalizationLatencyMs = finalizationLatencyMs;
            this.latencyReductionMs = latencyReductionMs;
        }
    }
}
