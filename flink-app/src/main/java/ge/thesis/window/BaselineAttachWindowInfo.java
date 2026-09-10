package ge.thesis.window;

import ge.thesis.model.WindowResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Window-info attacher used by the standalone Baseline test (no completeness computation).
 */
public class BaselineAttachWindowInfo extends ProcessWindowFunction<Aggregation.Acc, WindowResult, String, TimeWindow> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    @Override
    public void process(String key, Context context, Iterable<Aggregation.Acc> elements, Collector<WindowResult> out) {
        Aggregation.Acc finalAcc = elements.iterator().next();

        String startT = FORMATTER.format(Instant.ofEpochMilli(context.window().getStart()));
        String endT = FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));

        WindowResult result = new WindowResult(
                key, startT, endT, finalAcc.count, finalAcc.sumValue
        );

        out.collect(result);
    }
}