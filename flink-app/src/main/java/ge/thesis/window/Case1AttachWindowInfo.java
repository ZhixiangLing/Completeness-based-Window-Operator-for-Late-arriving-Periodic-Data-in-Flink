package ge.thesis.window;

import ge.thesis.config.WindowConfig;
import ge.thesis.evaluation.WindowFinalizationLatencyEvaluator;
import ge.thesis.evaluation.WindowFinalizationLatencyEvaluator.Metrics;
import ge.thesis.model.WindowResult;
import ge.thesis.window.trigger.CompletenessSemantic;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Use-Case-1 window-info attacher. Used by both proposed and baseline pipelines.
 * <ul>
 *   <li>mode = "PROPOSED": pairs with {@link ge.thesis.window.trigger.CompletenessTrigger},
 *       which fires FIRE_AND_PURGE once the window is fully populated.</li>
 *   <li>mode = "BASELINE": pairs with {@link ge.thesis.window.trigger.BaselineTrigger},
 *       which fires once at window.end and again on each late record within allowedLateness.</li>
 * </ul>
 * Each {@code process()} call records {@code System.currentTimeMillis()} as
 * {@code processingTimeMs}, surfaced as the {@code PurgeTime} field in
 * {@link ge.thesis.model.WindowResult#toString()}. For PROPOSED_PURGE / BASELINE_PURGE
 * rows this is the actual wall-clock moment the window state is cleared.
 */
public class Case1AttachWindowInfo extends ProcessWindowFunction<AggregationCase1.Acc, WindowResult, String, TimeWindow> {

    private static final String REASON_COMPLETENESS = "COMPLETENESS";
    private static final String REASON_DEADLINE = "DEADLINE";
    private static final String REASON_PRELIMINARY = "PRELIMINARY";

    private final CompletenessSemantic semantic;
    private final String mode;  // "PROPOSED" or "BASELINE"
    private final java.time.Duration allowedLateness;
    private final String seriesLabel;

    /** Default constructor for PROPOSED mode (mode label hard-coded). */
    public Case1AttachWindowInfo(CompletenessSemantic semantic, java.time.Duration allowedLateness) {
        this(semantic, "PROPOSED", null, allowedLateness);
    }

    /** Constructor accepting an explicit mode label. */
    public Case1AttachWindowInfo(CompletenessSemantic semantic, String mode, java.time.Duration allowedLateness) {
        this(semantic, mode, null, allowedLateness);
    }

    /**
     * Constructor for meterId-keyed aggregation, with an explicit series label included
     * in the output rows.
     */
    public Case1AttachWindowInfo(CompletenessSemantic semantic, String mode, String seriesLabel, java.time.Duration allowedLateness) {
        this.semantic = semantic;
        this.mode = mode;
        this.seriesLabel = seriesLabel;
        this.allowedLateness = allowedLateness;
    }

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    @Override
    public void process(String key, Context context, Iterable<AggregationCase1.Acc> elements, Collector<WindowResult> out) {
        // Wall-clock time at which Flink emits this window result.
        // For PROPOSED_PURGE / BASELINE_PURGE this is the actual state-cleanup moment.
        long processingTimeMs = System.currentTimeMillis();

        AggregationCase1.Acc finalAcc = elements.iterator().next();

        String startT = FORMATTER.format(Instant.ofEpochMilli(context.window().getStart()));
        String endT   = FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));

        String completenessStr = semantic.getFormattedCompleteness(finalAcc.count);
        boolean isComplete     = semantic.isFinalMet(finalAcc.count);

        long currentWatermark = context.currentWatermark();
        long finalDeadline    = context.window().maxTimestamp() + allowedLateness.toMillis();
        boolean isDeadlineReached = currentWatermark >= finalDeadline;

        String resultType = (isComplete || isDeadlineReached) ? "FINAL" : "PRELIMINARY";
        String finalizationReason = finalizationReason(isComplete, isDeadlineReached);

        String meterId, seriesId;

        if (seriesLabel != null) {
            // Current Use Case 1 keys streams by meterId; the caller supplies the series label.
            meterId = key;
            seriesId = seriesLabel;
        } else {
            // Backward-compatible path for legacy keys of the form "meterId_seriesId".
            int sepIdx = key.lastIndexOf("_Spänning");
            if (sepIdx >= 0) {
                meterId  = key.substring(0, sepIdx);
                seriesId = key.substring(sepIdx + 1);
            } else {
                meterId  = key;
                seriesId = "unknown";
            }
        }

        WindowResult result = new WindowResult(
                meterId, seriesId, startT, endT,
                finalAcc.count, finalAcc.faultIndicator,
                completenessStr, resultType,
                processingTimeMs, mode
        );
        result.attachFinalizationReason(finalizationReason);
        result.attachVoltageData(finalAcc.voltageData);
        result.attachWindowBounds(context.window().getStart(), context.window().getEnd());
        result.attachDistinctSlotCount(
                finalAcc.distinctSlotBits == null ? 0 : finalAcc.distinctSlotBits.cardinality()
        );

        // Always carry the window's epoch-ms boundaries so downstream operators
        // (e.g. OutputCountAggregator) can key by them. attachFinalizationLatencyMetrics
        // below would set the same values for FINAL — this just covers PRELIMINARY too.
        result.windowStartMs = context.window().getStart();
        result.windowEndMs   = context.window().getEnd();

        // Distinct slot count for strict slot-completeness.
        // distinctSlotBits is bounded by 8 (single-phase) or 24 (three-phase).
        result.distinctSlotCount = finalAcc.distinctSlotBits != null
                ? finalAcc.distinctSlotBits.cardinality()
                : 0;

        if ("FINAL".equals(resultType)) {
            Metrics metrics = WindowFinalizationLatencyEvaluator.evaluate(
                    currentWatermark,
                    context.window().getEnd(),
                    allowedLateness
            );
            result.attachFinalizationLatencyMetrics(
                    context.window().getStart(),
                    context.window().getEnd(),
                    metrics.watermarkAtPurgeMs,
                    metrics.allowedLatenessMs,
                    metrics.finalizationLatencyMs,
                    metrics.latencyReductionMs
            );
        }

        out.collect(result);
    }

    private String finalizationReason(boolean isComplete, boolean isDeadlineReached) {
        if (isComplete) {
            return REASON_COMPLETENESS;
        }
        if (isDeadlineReached) {
            return REASON_DEADLINE;
        }
        return REASON_PRELIMINARY;
    }
}
