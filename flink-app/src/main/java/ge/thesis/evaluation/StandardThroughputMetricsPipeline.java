package ge.thesis.evaluation;

import ge.thesis.sink.ResultFileSinkFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Wires standard throughput probes and writes their side-output metrics to CSV.
 */
public class StandardThroughputMetricsPipeline {

    private static final long DEFAULT_INTERVAL_MS = 1000L;
    private static final String CSV_HEADER = "timestamp,probe,countInInterval,eventsPerSecond,intervalMs";

    private final String mode;
    private final String outDir;
    private final OutputTag<String> sourceThroughputTag;
    private final OutputTag<String> postFilterThroughputTag;
    private final long intervalMs;

    public StandardThroughputMetricsPipeline(String mode, String outDir) {
        this(mode, outDir, DEFAULT_INTERVAL_MS);
    }

    public StandardThroughputMetricsPipeline(String mode, String outDir, long intervalMs) {
        this.mode = mode;
        this.outDir = outDir;
        this.intervalMs = intervalMs;
        this.sourceThroughputTag = new OutputTag<String>("standard-source-throughput") {};
        this.postFilterThroughputTag = new OutputTag<String>("standard-post-filter-throughput") {};
    }

    public <T> SingleOutputStreamOperator<T> attachSourceProbe(DataStream<T> input) {
        return input.process(new ThroughputProbe<>(mode + "_source_after_ingestion", sourceThroughputTag, intervalMs));
    }

    public <T> SingleOutputStreamOperator<T> attachPostFilterProbe(DataStream<T> input) {
        return input.process(new ThroughputProbe<>(mode + "_post_filter", postFilterThroughputTag, intervalMs));
    }

    public <T> void writeMetrics(SingleOutputStreamOperator<?> sourceProbeStream, DataStream<T> postFilterStream, String suffix) {
        SingleOutputStreamOperator<T> postFilterProbeStream = attachPostFilterProbe(postFilterStream);

        DataStream<String> standardThroughputMetricsStream = sourceProbeStream
                .getSideOutput(sourceThroughputTag)
                .union(postFilterProbeStream.getSideOutput(postFilterThroughputTag));

        standardThroughputMetricsStream.addSink(new ResultFileSinkFunction(
                outDir + "/standard_throughput_metrics" + suffix + ".csv",
                CSV_HEADER,
                false
        ));
    }
}
