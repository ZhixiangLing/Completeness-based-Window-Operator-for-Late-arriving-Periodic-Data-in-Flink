package ge.thesis.model;

import java.io.Serializable;

/**
 * POJO carrying the static metadata of a meter.
 */
public class MeterMetadata implements Serializable {
    public String meterId;
    public String typeDescription; // e.g. "Kamstrup E-Meter 3-Phase"
    public int phases;             // 1 or 3

    public MeterMetadata() {}

    public MeterMetadata(String meterId, String typeDescription, int phases) {
        this.meterId = meterId;
        this.typeDescription = typeDescription;
        this.phases = phases;
    }

    @Override
    public String toString() {
        return String.format("MeterID:%s | Type:%s", meterId, typeDescription);
    }
}