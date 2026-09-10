package ge.thesis.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Loads pre-classified meter IDs from CSV files generated from cm_meter metadata.
 */
public class MeterPhaseLookup implements Serializable {

    private final Set<String> singlePhaseMeterIds;
    private final Set<String> threePhaseMeterIds;

    public MeterPhaseLookup(Set<String> singlePhaseMeterIds, Set<String> threePhaseMeterIds) {
        this.singlePhaseMeterIds = Collections.unmodifiableSet(new HashSet<>(singlePhaseMeterIds));
        this.threePhaseMeterIds = Collections.unmodifiableSet(new HashSet<>(threePhaseMeterIds));
    }

    public static MeterPhaseLookup fromCsvFiles(String singlePhasePath, String threePhasePath) throws IOException {
        return new MeterPhaseLookup(
                loadMeterIds(Path.of(singlePhasePath)),
                loadMeterIds(Path.of(threePhasePath))
        );
    }

    public boolean isSinglePhase(String meterId) {
        return singlePhaseMeterIds.contains(meterId);
    }

    public boolean isThreePhase(String meterId) {
        return threePhaseMeterIds.contains(meterId);
    }

    public int singlePhaseCount() {
        return singlePhaseMeterIds.size();
    }

    public int threePhaseCount() {
        return threePhaseMeterIds.size();
    }

    private static Set<String> loadMeterIds(Path path) throws IOException {
        Set<String> meterIds = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }

                String[] fields = trimmed.split(",", 2);
                String meterId = fields[0].trim();
                if (!meterId.isEmpty() && !"meterId".equalsIgnoreCase(meterId)) {
                    meterIds.add(meterId);
                }
            }
        }

        return meterIds;
    }
}
