package ge.thesis.window.trigger;

import ge.thesis.model.MeterReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * Completeness-based trigger: fires the window result early once semantic completeness is
 * reached and actively releases window state via {@code FIRE_AND_PURGE}.
 * <p>
 * Contract: at most one PRELIMINARY emission per window plus exactly one FINAL emission,
 * either at completeness or at the allowed-lateness deadline.
 */
public class CompletenessTrigger extends Trigger<MeterReading, TimeWindow> {

    // Unified semantics model replaces the previous scattered parameters.
    private final CompletenessSemantic semantic;
    private final long allowedLatenessMs;    // Allowed-lateness span in milliseconds.

    // Independent record count maintained inside the trigger context.
    private final ValueStateDescriptor<Integer> countDesc =
            new ValueStateDescriptor<>("received-count", Types.INT);

    // Cached meterId so onEventTime can identify the current window in logs / diagnostics.
    private final ValueStateDescriptor<String> meterIdDesc =
            new ValueStateDescriptor<>("meter-id", Types.STRING);

    // Ensures each window fires PRELIMINARY at most once, matching the
    // "at most 1 PRELIMINARY + 1 FINAL" design contract.
    private final ValueStateDescriptor<Boolean> firedPreliminaryDesc =
            new ValueStateDescriptor<>("fired-preliminary", Types.BOOLEAN);

    // Once a window has emitted its FINAL result, later allowed-lateness records
    // must not create a second mini-window and emit again.
    private final ValueStateDescriptor<Boolean> finalEmittedDesc =
            new ValueStateDescriptor<>("final-emitted", Types.BOOLEAN);

    /** Constructs the trigger with the completeness semantics and allowed-lateness span. */
    public CompletenessTrigger(CompletenessSemantic semantic, Duration allowedLateness) {
        this.semantic = semantic;
        this.allowedLatenessMs = allowedLateness.toMillis();
    }

    @Override
    public TriggerResult onElement(MeterReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Boolean> finalEmittedState = ctx.getPartitionedState(finalEmittedDesc);
        if (Boolean.TRUE.equals(finalEmittedState.value())) {
            return TriggerResult.PURGE;
        }

        // Cache the meterId for the eventual onEventTime callback.
        ValueState<String> meterIdState = ctx.getPartitionedState(meterIdDesc);
        if (meterIdState.value() == null) {
            meterIdState.update(element.meterId);
        }

        // 1. Update the per-window record count.
        ValueState<Integer> countState = ctx.getPartitionedState(countDesc);
        Integer count = countState.value();
        if (count == null) {
            count = 0;
        }
        count++;
        countState.update(count);

        // 2. Register the final-deadline timer (idempotent across multiple elements).
        // Flink clears event-time windows at window.maxTimestamp() + allowedLateness,
        // so the final fallback timer must use the same boundary.
        ctx.registerEventTimeTimer(finalDeadline(window));

        // 3. Evaluate completeness in real time: fire early once the data is sufficiently complete.
        return evaluateCompleteness(count, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // The fallback path: the watermark reached the deadline.
        if (time == finalDeadline(window)) {
            ValueState<Boolean> finalEmittedState = ctx.getPartitionedState(finalEmittedDesc);
            if (Boolean.TRUE.equals(finalEmittedState.value())) {
                clearAllState(window, ctx);
                return TriggerResult.PURGE;
            }
            clearAllState(window, ctx);
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // Strictly event-time based; processing time is ignored.
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // Final cleanup hook called when the window's lifecycle ends.
        clearAllState(window, ctx);
    }

    // ================== Private evaluation logic ==================

    /**
     * Evaluates current completeness and decides the next trigger action:
     * <ul>
     *   <li>100% complete → FIRE_AND_PURGE (clear state early).</li>
     *   <li>Threshold reached but not full → FIRE one PRELIMINARY result, then keep state
     *       to collect late records without re-firing.</li>
     *   <li>Otherwise wait until the deadline timer triggers the final FIRE_AND_PURGE.</li>
     * </ul>
     */
    private TriggerResult evaluateCompleteness(int count, TimeWindow window, TriggerContext ctx) throws Exception {
        if (semantic.isFinalMet(count)) {
            ctx.getPartitionedState(finalEmittedDesc).update(true);
            clearActiveState(window, ctx);
            return TriggerResult.FIRE_AND_PURGE;
        }
        if (semantic.isPreliminaryMet(count)) {
            ValueState<Boolean> firedPreliminary = ctx.getPartitionedState(firedPreliminaryDesc);
            if (!Boolean.TRUE.equals(firedPreliminary.value())) {
                firedPreliminary.update(true);
                return TriggerResult.FIRE;
            }
        }
        return TriggerResult.CONTINUE;
    }

    private void clearActiveState(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(countDesc).clear();
        ctx.getPartitionedState(meterIdDesc).clear();
        ctx.getPartitionedState(firedPreliminaryDesc).clear();
    }

    private void clearAllState(TimeWindow window, TriggerContext ctx) throws Exception {
        clearActiveState(window, ctx);
        ctx.getPartitionedState(finalEmittedDesc).clear();

        // Performance note: never call ctx.deleteEventTimeTimer(). Leftover timers in the
        // priority queue expire harmlessly (O(log N) per poll), but explicit deletion forces
        // an O(N) linear scan over Flink's timer array — disastrous at millions of timers.
    }

    private long finalDeadline(TimeWindow window) {
        return window.maxTimestamp() + allowedLatenessMs;
    }
}
