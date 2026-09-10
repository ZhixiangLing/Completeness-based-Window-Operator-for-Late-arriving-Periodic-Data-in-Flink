package ge.thesis.window;

import ge.thesis.model.WindowResult;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Attaches window metadata for native baseline outputs without completeness semantics.
 */
public class NativeAttachWindowInfo extends ProcessWindowFunction<AggregationCase1.Acc, WindowResult, String, TimeWindow> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final String mode;
    private final String seriesLabel;

    public NativeAttachWindowInfo(String mode, String seriesLabel) {
        this.mode = mode;
        this.seriesLabel = seriesLabel;
    }

    @Override
    public void process(String key, Context context, Iterable<AggregationCase1.Acc> elements, Collector<WindowResult> out) {
        AggregationCase1.Acc acc = elements.iterator().next();
        String startT = FORMATTER.format(Instant.ofEpochMilli(context.window().getStart()));
        String endT = FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));

        WindowResult result = new WindowResult(
                key,
                seriesLabel,
                startT,
                endT,
                acc.count,
                acc.faultIndicator,
                "",
                "NATIVE",
                System.currentTimeMillis(),
                mode
        );
        result.attachVoltageData(acc.voltageData);
        result.attachWindowBounds(context.window().getStart(), context.window().getEnd());
        result.attachDistinctSlotCount(
                acc.distinctSlotBits == null ? -1 : acc.distinctSlotBits.cardinality()
        );
        out.collect(result);
    }
}
