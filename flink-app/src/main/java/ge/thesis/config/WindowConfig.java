package ge.thesis.config;

import java.time.Duration;

/**
 * Static configuration for the Use Case 1 window pipeline.
 * Values are read by both the proposed (CBW) and baseline (SW/DWW/ALW) operator builders.
 */
public class WindowConfig {

    /** Tumbling event-time window size. Two hours match the Use Case 1 thesis setup. */
    public static final Duration WINDOW_SIZE = Duration.ofHours(2);

    /**
     * Default allowed-lateness span used when the CLI does not override it.
     * The thesis evaluation covers lateness up to seven days; 50 hours is the
     * day-to-day development default.
     */
    public static final Duration ALLOWED_LATENESS = Duration.ofHours(50);

    /**
     * Completeness threshold theta. A value of 0.75 means a preliminary result is
     * fired as soon as 75% of the expected slots have been observed.
     */
    public static final double COMPLETENESS_THRESHOLD = 0.75;

    /**
     * Expected number of records per window. With a 2-hour window and a 15-minute
     * reporting interval, each meter is expected to emit eight readings per series.
     */
    public static final int EXPECTED_SLOTS_PER_WINDOW = 8;
}
