package com.uber.data.kafka.connect.workload.tracking;

import org.apache.kafka.common.utils.Time;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.workload.common.Workload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Tracks segments for a single workload. Segments will be considered in-progress until a
 * user-specified duration has elapsed, at which point the segment will be considered complete, and
 * a new active segment will be started.
 */
class WorkloadCollector {

    private static final Logger log = LoggerFactory.getLogger(WorkloadCollector.class);

    private final Time time;
    private final Duration maxSegmentAge;

    private long startMs;
    private long totalBytes;
    private long totalMessages;

    WorkloadCollector(Duration maxSegmentAge, Time time) {
        this.maxSegmentAge = Objects.requireNonNull(maxSegmentAge, "max segment age may not be null");
        this.time = Objects.requireNonNull(time, "time may not be null");
        reset();
    }

    /**
     * Record the workload for zero or more messages, and if there is an in-progress segment whose
     * age has exceeded the maximum segment age, return that segment's workload and begin a new
     * segment.
     * <p>
     * Note that if a {@link Optional#isPresent() present Optional} is returned, the
     * workload information contained in it will be dropped by this collector after the method
     * returns. It is the responsibility of the caller to store the workload information for as long as it is needed.
     * @param bytes the total bytes consumed for the messages; may not be negative
     * @param messages the total messages consumed; may not be negative
     * @return the workload for the previous segment, if one was just completed
     */
    public synchronized Optional<Workload> record(long bytes, long messages) {
        if (bytes < 0) {
            log.warn("Ignoring invalid workload with negative number of bytes: {}", bytes);
            return Optional.empty();
        }
        if (messages < 0) {
            log.warn("Ignoring invalid workload with negative number of messages: {}", messages);
            return Optional.empty();
        }

        Optional<Workload> result;
        Duration age = age();

        if (age.compareTo(maxSegmentAge) >= 0) {
            result = Optional.ofNullable(collect(totalBytes, totalMessages, age));
            reset();
        } else {
            result = Optional.empty();
        }

        this.totalBytes += bytes;
        this.totalMessages += messages;

        return result;
    }

    private Duration age() {
        return Duration.ofMillis(time.milliseconds() - startMs);
    }

    @VisibleForTesting
    static Workload collect(long totalBytes, long totalMessages, Duration age) {
        double durationSeconds = Math.ceil(age.toMillis() / 1000.0);
        if (durationSeconds <= 0) {
            log.warn(
                    "Tried to collect workload from segment whose age in seconds ({}) is " +
                            "non-positive; discarding this segment and starting fresh",
                    durationSeconds
            );
            return null;
        }

        return new Workload(
                totalBytes / durationSeconds,
                totalMessages / durationSeconds
        );
    }

    private void reset() {
        this.startMs = time.milliseconds();
        this.totalBytes = 0;
        this.totalMessages = 0;
    }

}
