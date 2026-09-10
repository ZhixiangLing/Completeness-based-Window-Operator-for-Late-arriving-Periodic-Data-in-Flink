package ge.thesis.evaluation;

import ge.thesis.sink.ResultFileSinkFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Wires throughput probes and writes their side-output metrics to CSV.
 */
public class ThroughputMetricsPipeline {

    private static final long DEFAULT_INTERVAL_MS = 1000L;
    private static final String CSV_HEADER = "timestamp,probe,countInInterval,eventsPerSecond,intervalMs";

    private final String mode;
    private final String outDir;
    private final OutputTag<String> sourceThroughputTag;
    private final OutputTag<String> postAggregationThroughputTag;
    private final long intervalMs;

    public ThroughputMetricsPipeline(String mode, String outDir) {
        this(mode, outDir, DEFAULT_INTERVAL_MS);
    }

    public ThroughputMetricsPipeline(String mode, String outDir, long intervalMs) {
        this.mode = mode;
        this.outDir = outDir;
        this.intervalMs = intervalMs;
        this.sourceThroughputTag = new OutputTag<String>("source-throughput") {};
        this.postAggregationThroughputTag = new OutputTag<String>("post-aggregation-throughput") {};
    }

    public <T> SingleOutputStreamOperator<T> attachSourceProbe(DataStream<T> input) {
        return input.process(new ThroughputProbe<>(mode + "_source_after_parse", sourceThroughputTag, intervalMs));
    }

    public <T> SingleOutputStreamOperator<T> attachPostAggregationProbe(DataStream<T> input) {
        return input.process(new ThroughputProbe<>(mode + "_post_window_aggregation", postAggregationThroughputTag, intervalMs));
    }

    public <T> void writeMetrics(SingleOutputStreamOperator<?> sourceProbeStream, DataStream<T> aggregationOutputStream, String suffix) {
        SingleOutputStreamOperator<T> postAggregationProbeStream = attachPostAggregationProbe(aggregationOutputStream);

        DataStream<String> throughputMetricsStream = sourceProbeStream
                .getSideOutput(sourceThroughputTag)
                .union(postAggregationProbeStream.getSideOutput(postAggregationThroughputTag));

        throughputMetricsStream.addSink(new ResultFileSinkFunction(
                outDir + "/throughput_metrics" + suffix + ".csv",
                CSV_HEADER,
                false
        ));
    }
}
