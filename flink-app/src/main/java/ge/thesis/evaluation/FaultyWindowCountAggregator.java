package ge.thesis.evaluation;

import ge.thesis.model.WindowResult;

import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * Maintains cumulative counts of unique faulty meter IDs by meter phase group.
 */
public class FaultyWindowCountAggregator extends RichMapFunction<WindowResult, String> {

    private static final String THREE_PHASE_LABEL = "Spänning L1/L2/L3";

    private final Set<String> singlePhaseFaultyMeterIds = new HashSet<>();
    private final Set<String> threePhaseFaultyMeterIds = new HashSet<>();

    @Override
    public String map(WindowResult value) {
        if (THREE_PHASE_LABEL.equals(value.seriesId)) {
            threePhaseFaultyMeterIds.add(value.meterId);
        } else {
            singlePhaseFaultyMeterIds.add(value.meterId);
        }

        int singlePhaseFaultCount = singlePhaseFaultyMeterIds.size();
        int threePhaseFaultCount = threePhaseFaultyMeterIds.size();

        return String.format(
                "%s,%s,%s,%s,%s,%d,%d,%d,%s",
                csv(value.meterId),
                csv(value.seriesId),
                csv(value.windowStart),
                csv(value.windowEnd),
                csv(value.mode),
                singlePhaseFaultCount,
                threePhaseFaultCount,
                singlePhaseFaultCount + threePhaseFaultCount,
                csv(value.voltageData)
        );
    }

    private String csv(String value) {
        if (value == null) {
            return "";
        }

        String escaped = value.replace("\"", "\"\"");
        if (escaped.contains(",") || escaped.contains("\"") || escaped.contains("\n")) {
            return "\"" + escaped + "\"";
        }
        return escaped;
    }
}
