package ge.thesis.window.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * Baseline trigger that mirrors Flink's default EventTimeTrigger behavior:
 * <ol>
 *   <li>FIRE once at {@code window.maxTimestamp()} (normal window emission).</li>
 *   <li>During the allowed-lateness window, FIRE again on every late record.</li>
 *   <li>Does NOT register a deadline FIRE_AND_PURGE — Flink purges the state
 *       automatically when the watermark crosses
 *       {@code window.maxTimestamp() + allowedLateness}.</li>
 * </ol>
 */
public class BaselineTrigger extends Trigger<Object, TimeWindow> {

    private final long allowedLatenessMs;

    public BaselineTrigger(Duration allowedLateness) {
        this.allowedLatenessMs = allowedLateness.toMillis();
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // Watermark has already crossed the window end; this is a late record — fire an update.
            return TriggerResult.FIRE;
        } else {
            // On-time element: register the window-end timer once.
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            // Normal window close: emit intermediate result, keep state for late records.
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // Performance note: never call ctx.deleteEventTimeTimer() — it triggers an O(N) scan
        // over Flink's timer priority queue; leftover timers expire naturally and are ignored.
    }
}
