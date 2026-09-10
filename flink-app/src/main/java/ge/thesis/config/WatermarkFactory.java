package ge.thesis.config;

import ge.thesis.model.MeterReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;

public class WatermarkFactory {

    public static WatermarkStrategy<MeterReading> createBasicStrategy() {
        return WatermarkStrategy
                .<MeterReading>forBoundedOutOfOrderness(Duration.ofMinutes(0))
                .withTimestampAssigner((reading, timestamp) -> reading.eventT)
                .withIdleness(Duration.ofMinutes(5));
    }


    /**
     * Delayed watermark strategy used by the "delayed" baseline mode.
     * The watermark trails the max observed event-time by {@code watermarkDelay},
     * so out-of-order tuples within that span are still considered on-time even
     * with zero allowed lateness on the window operator.
     */
    public static WatermarkStrategy<MeterReading> createDelayedStrategy(Duration watermarkDelay) {
        return WatermarkStrategy
                .<MeterReading>forBoundedOutOfOrderness(watermarkDelay)
                .withTimestampAssigner((reading, timestamp) -> reading.eventT)
                .withIdleness(Duration.ofMinutes(5));
    }
}