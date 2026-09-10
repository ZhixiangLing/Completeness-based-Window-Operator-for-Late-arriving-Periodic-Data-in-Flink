package ge.thesis.evaluation;

import ge.thesis.model.WindowResult;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * Aggregates how many WindowResult emissions occur per (meterId, seriesId, windowStartMs) key,
 * captures the count of records that ended up in the window's last emission (used to derive
 * the amplification factor in analysis), the distinct-slot count (used to derive strict
 * slot-completeness), and flags whether the window was purged before its allowed-lateness
 * deadline (PROPOSED only).
 *
 * Output schema (one row per window-key):
 *   mode,meterId,seriesId,windowStartMs,windowEndMs,outputCount,earlyPurged,finalCount,distinctSlotCount
 *
 * Timer-based summary: a single event-time timer is registered at
 *   windowEndMs + allowedLatenessMs + 1
 * which fires after the upstream window operator has finished emitting all results for that
 * window (either via the deadline timer or via end-of-source MAX watermark).
 */
public class OutputCountAggregator
        extends KeyedProcessFunction<String, WindowResult, String> {

    private final long allowedLatenessMs;

    private transient ValueState<Integer> countState;
    private transient ValueState<Boolean> earlyPurgedState;
    private transient ValueState<String> modeState;
    private transient ValueState<String> meterIdState;
    private transient ValueState<String> seriesIdState;
    private transient ValueState<Long> windowStartMsState;
    private transient ValueState<Long> windowEndMsState;
    /** Records-in-window from the most recent WindowResult for this key.
     *  Monotonically non-decreasing across firings of the same window, so the last value is
     *  the authoritative "how many records the window finalised with". */
    private transient ValueState<Integer> lastCountState;
    /** Distinct expected-slot count from the most recent WindowResult for this key.
     *  Bounded by 8 (single-phase) / 24 (three-phase); also monotonically non-decreasing. */
    private transient ValueState<Integer> lastDistinctSlotState;

    public OutputCountAggregator(long allowedLatenessMs) {
        this.allowedLatenessMs = allowedLatenessMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        countState         = getRuntimeContext().getState(new ValueStateDescriptor<>("output-count", Types.INT));
        earlyPurgedState   = getRuntimeContext().getState(new ValueStateDescriptor<>("early-purged", Types.BOOLEAN));
        modeState          = getRuntimeContext().getState(new ValueStateDescriptor<>("mode", Types.STRING));
        meterIdState       = getRuntimeContext().getState(new ValueStateDescriptor<>("meter-id", Types.STRING));
        seriesIdState      = getRuntimeContext().getState(new ValueStateDescriptor<>("series-id", Types.STRING));
        windowStartMsState = getRuntimeContext().getState(new ValueStateDescriptor<>("window-start-ms", Types.LONG));
        windowEndMsState   = getRuntimeContext().getState(new ValueStateDescriptor<>("window-end-ms", Types.LONG));
        lastCountState     = getRuntimeContext().getState(new ValueStateDescriptor<>("last-count", Types.INT));
        lastDistinctSlotState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-distinct-slot", Types.INT));
    }

    @Override
    public void processElement(WindowResult value, Context ctx, Collector<String> out) throws Exception {
        Integer count = countState.value();
        if (count == null) {
            // First element for this window-key: capture metadata and arm the summary timer.
            modeState.update(value.mode);
            meterIdState.update(value.meterId);
            seriesIdState.update(value.seriesId);
            windowStartMsState.update(value.windowStartMs);
            windowEndMsState.update(value.windowEndMs);
            earlyPurgedState.update(false);

            ctx.timerService().registerEventTimeTimer(value.windowEndMs + allowedLatenessMs + 1L);
            count = 0;
        }
        countState.update(count + 1);

        // Capture the records-in-window count from this emission. Last write wins, which is
        // exactly what we want: the upstream count is monotonically non-decreasing across
        // firings of the same window, so the last value is the final/authoritative count.
        lastCountState.update(value.count);
        // Same logic for distinct slot count (bounded by 8 / 24, also monotonic).
        lastDistinctSlotState.update(value.distinctSlotCount);

        // Update windowEndMs from later events too: PRELIMINARY rows do not carry it (== 0L);
        // FINAL rows do. We want the authoritative value to come from FINAL when available.
        if (value.windowEndMs != 0L) {
            windowEndMsState.update(value.windowEndMs);
        }

        // earlyPurged is meaningful only when we observe a FINAL emission whose
        // finalizationLatencyMs is strictly less than the allowed-lateness span,
        // i.e. the proposed trigger fired FIRE_AND_PURGE before the deadline.
        if ("FINAL".equals(value.resultType)
                && "PROPOSED".equals(value.mode)
                && value.allowedLatenessMs > 0L
                && value.finalizationLatencyMs < value.allowedLatenessMs) {
            earlyPurgedState.update(true);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Integer count = countState.value();
        if (count == null || count == 0) {
            return;
        }

        String mode      = nullSafe(modeState.value());
        String meterId   = nullSafe(meterIdState.value());
        String seriesId  = nullSafe(seriesIdState.value());
        long startMs     = nullSafe(windowStartMsState.value());
        long endMs       = nullSafe(windowEndMsState.value());
        boolean early    = Boolean.TRUE.equals(earlyPurgedState.value());
        Integer lastCount = lastCountState.value();
        int finalCount   = lastCount == null ? 0 : lastCount;
        Integer lastDistinct = lastDistinctSlotState.value();
        int distinctSlotCount = lastDistinct == null ? 0 : lastDistinct;

        out.collect(String.format(
                Locale.ROOT,
                "%s,%s,%s,%d,%d,%d,%b,%d,%d",
                safeCsv(mode),
                safeCsv(meterId),
                safeCsv(seriesId),
                startMs,
                endMs,
                count,
                early,
                finalCount,
                distinctSlotCount
        ));

        countState.clear();
        earlyPurgedState.clear();
        modeState.clear();
        meterIdState.clear();
        seriesIdState.clear();
        windowStartMsState.clear();
        windowEndMsState.clear();
        lastCountState.clear();
        lastDistinctSlotState.clear();
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static long nullSafe(Long value) {
        return value == null ? 0L : value;
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
}
