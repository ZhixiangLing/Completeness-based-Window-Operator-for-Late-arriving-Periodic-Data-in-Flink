package ge.thesis.evaluation;

import ge.thesis.model.MeterReading;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

/**
 * Sink that counts how many late tuples were dropped by the upstream window operator's
 * late-data side output. Increments a global Flink {@link LongCounter} accumulator named
 * {@link #ACCUMULATOR_NAME}; the final total is read after job completion in App.printRunSummary
 * and written to the per-run summary CSV.
 */
public class DroppedRecordCounter extends RichSinkFunction<MeterReading> {

    public static final String ACCUMULATOR_NAME = "dropped-records-count";

    private transient LongCounter counter;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        counter = new LongCounter();
        getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, counter);
    }

    @Override
    public void invoke(MeterReading value, Context context) {
        counter.add(1L);
    }
}
