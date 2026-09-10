package ge.thesis.window.trigger;

import java.io.Serializable;

/**
 * Domain model for window completeness: encapsulates the ratio computation, threshold
 * checks, and percentage formatting.
 * <p>
 * Implements {@link Serializable} so Flink can ship it to task managers.
 */
public class CompletenessSemantic implements Serializable {

    private final double threshold;      // Preliminary-firing threshold (theta).
    private final int expectedTotal;     // Expected number of records per window.

    public CompletenessSemantic(double threshold, int expectedTotal) {
        this.threshold = threshold;
        this.expectedTotal = expectedTotal;
    }

    /** Current completeness ratio in [0.0, 1.0+] given the count seen so far. */
    public double getRatio(int currentCount) {
        return (double) currentCount / expectedTotal;
    }

    /**
     * Whether the preliminary-firing threshold has been reached.
     * Uses {@code >=} to handle the boundary safely (the original code used {@code ==}
     * which could miss the trigger if the count jumped past the threshold).
     */
    public boolean isPreliminaryMet(int currentCount) {
        return getRatio(currentCount) >= threshold;
    }

    /** Whether 100% completeness (final result) has been reached. */
    public boolean isFinalMet(int currentCount) {
        return currentCount >= expectedTotal;
    }

    /** Formatted completeness percentage used by WindowInfo when assembling the result row. */
    public String getFormattedCompleteness(int currentCount) {
        return String.format("%.1f%%", getRatio(currentCount) * 100);
    }
}
