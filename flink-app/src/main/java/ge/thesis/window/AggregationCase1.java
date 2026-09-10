package ge.thesis.window;

import ge.thesis.config.WindowConfig;
import ge.thesis.model.MeterReading;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Use Case 1 aggregator (faulty-meter detection).
 * <ul>
 *   <li>Single-phase meters: flag the meter as faulty when "Spänning L1" contains
 *       three consecutive zero-voltage readings (sorted by event-time).</li>
 *   <li>Three-phase meters: flag as faulty only when each of "Spänning L1", "L2"
 *       and "L3" independently contains three consecutive zero readings.</li>
 * </ul>
 */
public class AggregationCase1 implements AggregateFunction<MeterReading, AggregationCase1.Acc, AggregationCase1.Acc> {

    public enum MeterPhaseMode {
        SINGLE_PHASE,
        THREE_PHASE
    }

    public static final String SERIES_L1 = "Spänning L1";
    public static final String SERIES_L2 = "Spänning L2";
    public static final String SERIES_L3 = "Spänning L3";

    // Slot bookkeeping for the strict slot-completeness metric.
    // Tumbling windows are aligned to multiples of WINDOW_SIZE (default offset 0),
    // so we can derive the per-window slot index from eventT alone — the
    // AggregateFunction interface does not expose window context inside add().
    private static final long WINDOW_SIZE_MS = WindowConfig.WINDOW_SIZE.toMillis();
    private static final long SLOT_DURATION_MS = WINDOW_SIZE_MS / WindowConfig.EXPECTED_SLOTS_PER_WINDOW;

    private final MeterPhaseMode mode;
    private final boolean trackDistinctSlots;

    private static int phaseIndex(String seriesId) {
        if (SERIES_L1.equals(seriesId)) {
            return 0;
        }
        if (SERIES_L2.equals(seriesId)) {
            return 1;
        }
        if (SERIES_L3.equals(seriesId)) {
            return 2;
        }
        return -1; // defensive: upstream filters guarantee L1/L2/L3 only.
    }

    public AggregationCase1() {
        this(MeterPhaseMode.SINGLE_PHASE);
    }

    public AggregationCase1(MeterPhaseMode mode) {
        this(mode, true);
    }

    public AggregationCase1(MeterPhaseMode mode, boolean trackDistinctSlots) {
        this.mode = mode;
        this.trackDistinctSlots = trackDistinctSlots;
    }

    /** One sample: event-time + voltage. */
    public static class DataPoint {
        public long eventT;
        public double voltage;

        public DataPoint(long eventT, double voltage) {
            this.eventT = eventT;
            this.voltage = voltage;
        }
    }

    /**
     * Accumulator: keeps incoming (possibly out-of-order) data points per series; the
     * faulty-pattern check sorts them by event-time at result-emission time.
     */
    public static class Acc {
        public int count = 0;
        public Map<String, List<DataPoint>> pointsBySeries = new HashMap<>();
        public boolean faultIndicator = false; // Set to true once the consecutive-zero pattern is found.
        public String voltageData = "";
        // Bit i is set iff slot/(slot,phase) index i has been observed at least once.
        // Pre-sized in createAccumulator() to fit fully in a single 64-bit word.
        public BitSet distinctSlotBits;
    }

    @Override
    public Acc createAccumulator() {
        Acc acc = new Acc();
        if (!trackDistinctSlots) {
            return acc;
        }
        int maxBits = (mode == MeterPhaseMode.SINGLE_PHASE)
                ? WindowConfig.EXPECTED_SLOTS_PER_WINDOW           //  8
                : WindowConfig.EXPECTED_SLOTS_PER_WINDOW * 3;       // 24
        acc.distinctSlotBits = new BitSet(maxBits);
        return acc;
    }

    @Override
    public Acc add(MeterReading value, Acc accumulator) {
        accumulator.count++;
        accumulator.pointsBySeries
                .computeIfAbsent(value.seriesId, ignored -> new ArrayList<>())
                .add(new DataPoint(value.eventT, value.value));
        if (trackDistinctSlots) {
            markDistinctSlot(value, accumulator);
        }
        return accumulator;
    }

    private void markDistinctSlot(MeterReading value, Acc accumulator) {
        long windowStart = Math.floorDiv(value.eventT, WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
        int slotIndex = (int) ((value.eventT - windowStart) / SLOT_DURATION_MS);
        int distinctKey;
        if (mode == MeterPhaseMode.SINGLE_PHASE) {
            distinctKey = slotIndex;
        } else {
            int p = phaseIndex(value.seriesId);
            distinctKey = (p >= 0) ? slotIndex * 3 + p : -1;
        }
        if (distinctKey >= 0) {
            accumulator.distinctSlotBits.set(distinctKey);
        }
    }

    @Override
    public Acc getResult(Acc accumulator) {
        if (mode == MeterPhaseMode.THREE_PHASE) {
            accumulator.faultIndicator =
                    hasThreeConsecutiveZeros(accumulator.pointsBySeries.get(SERIES_L1)) &&
                    hasThreeConsecutiveZeros(accumulator.pointsBySeries.get(SERIES_L2)) &&
                    hasThreeConsecutiveZeros(accumulator.pointsBySeries.get(SERIES_L3));
            accumulator.voltageData = formatVoltageData(
                    accumulator.pointsBySeries,
                    SERIES_L1,
                    SERIES_L2,
                    SERIES_L3
            );
        } else {
            accumulator.faultIndicator =
                    hasThreeConsecutiveZeros(accumulator.pointsBySeries.get(SERIES_L1));
            accumulator.voltageData = formatVoltageData(accumulator.pointsBySeries, SERIES_L1);
        }

        return accumulator;
    }

    private boolean hasThreeConsecutiveZeros(List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            return false;
        }

        // Records may have arrived out-of-order; sort by event-time before checking the
        // three-consecutive-zeros pattern.
        points.sort(Comparator.comparingLong(p -> p.eventT));

        int consecutiveZeros = 0;

        for (DataPoint point : points) {
            if (Math.abs(point.voltage) <= 0.001) { 
                consecutiveZeros++;
                if (consecutiveZeros >= 3) {
                    return true;
                }
            } else {
                consecutiveZeros = 0;
            }
        }

        return false;
    }

    private String formatVoltageData(Map<String, List<DataPoint>> pointsBySeries, String... seriesIds) {
        List<String> parts = new ArrayList<>();
        for (String seriesId : seriesIds) {
            List<DataPoint> points = pointsBySeries.get(seriesId);
            if (points == null || points.isEmpty()) {
                parts.add(seriesId + "=[]");
                continue;
            }

            points.sort(Comparator.comparingLong(p -> p.eventT));
            List<String> voltages = new ArrayList<>();
            for (DataPoint point : points) {
                voltages.add(String.format("%.3f", point.voltage));
            }
            parts.add(seriesId + "=[" + String.join(";", voltages) + "]");
        }

        return String.join("|", parts);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.count += b.count;
        for (Map.Entry<String, List<DataPoint>> entry : b.pointsBySeries.entrySet()) {
            a.pointsBySeries
                    .computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                    .addAll(entry.getValue());
        }
        if (!trackDistinctSlots) {
            return a;
        }
        if (a.distinctSlotBits == null) {
            a.distinctSlotBits = (BitSet) (b.distinctSlotBits != null
                    ? b.distinctSlotBits.clone()
                    : new BitSet());
        } else if (b.distinctSlotBits != null) {
            a.distinctSlotBits.or(b.distinctSlotBits);
        }
        return a;
    }
}
