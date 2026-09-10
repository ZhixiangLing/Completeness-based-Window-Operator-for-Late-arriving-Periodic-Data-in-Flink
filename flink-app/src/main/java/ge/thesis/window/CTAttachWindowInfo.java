package ge.thesis.window;

import ge.thesis.config.WindowConfig;
import ge.thesis.model.WindowResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Window-info attacher used by the standalone Completeness-Trigger test.
 * Labels the row as FINAL when either (a) the window is 100% complete or
 * (b) the watermark has crossed the allowed-lateness deadline; otherwise PRELIMINARY.
 */
public class CTAttachWindowInfo extends ProcessWindowFunction<Aggregation.Acc, WindowResult, String, TimeWindow> {

    private final java.time.Duration allowedLateness;

    public CTAttachWindowInfo(java.time.Duration allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    @Override
    public void process(String key, Context context, Iterable<Aggregation.Acc> elements, Collector<WindowResult> out) {
        Aggregation.Acc finalAcc = elements.iterator().next();

        // 1. Format the window bounds.
        String startT = FORMATTER.format(Instant.ofEpochMilli(context.window().getStart()));
        String endT = FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));

        // 2. Completeness = observed count / expected slots.
        double completenessRatio = (double) finalAcc.count / WindowConfig.EXPECTED_SLOTS_PER_WINDOW;
        String completenessStr = String.format("%.1f%%", completenessRatio * 100);

        // 3. FINAL when either condition holds:
        //    A) data is 100% complete, or
        //    B) the watermark has crossed window.maxTimestamp() + allowedLateness
        //       (the Flink event-time cleanup deadline).
        long currentWatermark = context.currentWatermark();
        long finalDeadline = context.window().maxTimestamp() + allowedLateness.toMillis();
        boolean isComplete = completenessRatio >= 1.0;
        boolean isDeadlineReached = currentWatermark >= finalDeadline;
        String resultType = (isComplete || isDeadlineReached) ? "FINAL" : "PRELIMINARY";

        // 4. Assemble the row.
        WindowResult result = new WindowResult(
                key, startT, endT, finalAcc.count, finalAcc.sumValue, completenessStr, resultType
        );

        out.collect(result);
    }
}
