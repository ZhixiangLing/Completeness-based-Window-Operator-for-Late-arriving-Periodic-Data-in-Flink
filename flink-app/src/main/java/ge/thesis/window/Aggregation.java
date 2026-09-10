package ge.thesis.window; 

import ge.thesis.model.MeterReading;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregation function. Input is MeterReading, the accumulator is the
 * inner {@link Acc} class, and the output is also {@link Acc}.
 * Used by the standalone baseline test; the production CBW/baselines use
 * {@link AggregationCase1} instead.
 */
public class Aggregation implements AggregateFunction<MeterReading, Aggregation.Acc, Aggregation.Acc> {

    /** Minimal-footprint accumulator storing only the running sum and count. */
    public static class Acc {
        public double sumValue = 0.0;
        public int count = 0;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(MeterReading value, Acc accumulator) {
        // Update state per record without retaining the raw payload.
        accumulator.sumValue += value.value;
        accumulator.count += 1;
        return accumulator;
    }

    @Override
    public Acc getResult(Acc accumulator) {
        return accumulator;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        // Required for session windows or low-level state merges.
        a.sumValue += b.sumValue;
        a.count += b.count;
        return a;
    }
}