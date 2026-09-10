package ge.thesis.operator;

import ge.thesis.model.MeterMetadata;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Routes raw meter-metadata rows into three streams:
 *   - main stream: single-phase meters
 *   - {@link #THREE_PHASE_TAG} side output: three-phase meters
 *   - {@link #OTHER_METERS_TAG} side output: CT meters and rows that don't match either label
 */
public class MeterClassifier extends ProcessFunction<String, MeterMetadata> {

    public static final OutputTag<MeterMetadata> THREE_PHASE_TAG = new OutputTag<MeterMetadata>("three-phase-stream"){};
    /** Captures CT meters and rows with unrecognised type descriptions, keeping them out of analysis. */
    public static final OutputTag<MeterMetadata> OTHER_METERS_TAG = new OutputTag<MeterMetadata>("other-meters"){};

    @Override
    public void processElement(String value, Context ctx, Collector<MeterMetadata> out) {
        try {
            String[] fields = value.split(",");
            if (fields.length < 4) return;

            String meterId = fields[1].trim();
            String description = fields[3].trim(); // The key field for routing.

            // The trailing numeric column is stored for reference only; routing is based on the
            // description string above.
            int phasesFromRaw = Integer.parseInt(fields[fields.length - 1].trim());
            MeterMetadata metadata = new MeterMetadata(meterId, description, phasesFromRaw);

            // Strict string-based routing.
            if (description.contains("3-Phase")) {
                ctx.output(THREE_PHASE_TAG, metadata);
            } else if (description.contains("1-Phase")) {
                out.collect(metadata); // Single-phase main stream.
            } else {
                // Catches rows like "Kamstrup E-Meter CT" that lack a 1-Phase or 3-Phase tag.
                ctx.output(OTHER_METERS_TAG, metadata);
            }
        } catch (Exception e) {
            // Parse failures are dropped silently — they don't enter any output stream.
        }
    }
}