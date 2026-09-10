package ge.thesis.operator;

import ge.thesis.model.MeterMetadata;
import ge.thesis.sink.ResultFileSink;
import ge.thesis.source.KafkaSourceBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * One-off preprocessing job for generating single-phase and three-phase meter CSV files.
 */
public class MeterClassificationJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> rawMeterBus = env.fromSource(
                KafkaSourceBuilder.build("cm_meter"),
                WatermarkStrategy.noWatermarks(),
                "Metadata-Source"
        );

        SingleOutputStreamOperator<MeterMetadata> classified = rawMeterBus.process(new MeterClassifier());

        DataStream<MeterMetadata> singlePhaseStream = classified;
        singlePhaseStream
                .map(m -> m.meterId + "," + m.typeDescription + "," + m.phases).returns(String.class)
                .map(new ResultFileSink("output/single_phase_meters.csv"))
                .print(">>> Saved 1-Phase");

        singlePhaseStream.map(new Counter("Single-Phase Total")).print();

        DataStream<MeterMetadata> threePhaseStream = classified.getSideOutput(MeterClassifier.THREE_PHASE_TAG);
        threePhaseStream
                .map(m -> m.meterId + "," + m.typeDescription + "," + m.phases).returns(String.class)
                .map(new ResultFileSink("output/three_phase_meters.csv"))
                .print(">>> Saved 3-Phase");

        threePhaseStream.map(new Counter("Three-Phase Total")).print();

        env.execute("Smart Meter Classification");
    }

    public static class Counter implements MapFunction<MeterMetadata, String> {
        private final String label;
        private long count = 0;

        public Counter(String label) {
            this.label = label;
        }

        @Override
        public String map(MeterMetadata v) {
            return label + ": " + (++count);
        }
    }
}
