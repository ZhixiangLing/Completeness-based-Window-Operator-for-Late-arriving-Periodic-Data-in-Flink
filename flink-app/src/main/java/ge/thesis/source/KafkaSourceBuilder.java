package ge.thesis.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Builds Kafka sources pointing at the Goteborg Energi meter-values topic.
 */
public class KafkaSourceBuilder {

    /** Backward-compatible no-arg builder that uses the default meter-values topic. */
    public static KafkaSource<String> build() {
        return build("cm_meter_values");
    }

    /** Builds a Kafka source for the given topic. */
    public static KafkaSource<String> build(String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers("psr-kafkabroker:9094")
                .setTopics(topic)
                .setGroupId("thesis-experiment-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}