package ge.thesis.evaluation;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.time.Instant;

/**
 * Emits throughput samples to a side output while passing the original stream through unchanged.
 */
public class ThroughputProbe<T> extends ProcessFunction<T, T> {

    private final String probeName;
    private final OutputTag<String> metricsTag;
    private final long intervalMs;

    private transient long intervalStartMs;
    private transient long intervalCount;

    public ThroughputProbe(String probeName, OutputTag<String> metricsTag, long intervalMs) {
        this.probeName = probeName;
        this.metricsTag = metricsTag;
        this.intervalMs = intervalMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        intervalStartMs = 0L;
        intervalCount = 0L;
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
        long nowMs = System.currentTimeMillis();

        if (intervalStartMs == 0L) {
            intervalStartMs = nowMs;
        }

        intervalCount++;

        long elapsedMs = nowMs - intervalStartMs;
        if (elapsedMs >= intervalMs) {
            double intervalSeconds = elapsedMs / 1000.0;
            double eventsPerSecond = intervalCount / intervalSeconds;

            ctx.output(metricsTag, String.format(
                    "%s,%s,%d,%.3f,%d",
                    Instant.ofEpochMilli(nowMs),
                    probeName,
                    intervalCount,
                    eventsPerSecond,
                    elapsedMs
            ));

            intervalStartMs = nowMs;
            intervalCount = 0L;
        }

        out.collect(value);
    }
}
