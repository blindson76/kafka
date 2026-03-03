package com.uber.data.kafka.connect.workload.tracking;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.uber.data.kafka.connect.workload.common.Workload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tracks {@link Workload workloads} on a per-topic-partition basis, allowing users to periodically
 * {@link #record(TopicPartition, long, long) record} new workload information for partitions and,
 * once workload information for a topic partition has matured, retrieve a complete snapshot of the
 * average per-second workload of that partition.
 */
class WorkloadTracker {

    private static final Logger log = LoggerFactory.getLogger(WorkloadTracker.class);

    private final Duration maxSegmentAge;
    private final Time time;
    private final LoadingCache<TopicPartition, WorkloadCollector> activeSegments;
    private final Map<TopicPartition, Workload> staleWorkloads;

    public WorkloadTracker(Duration maxSegmentAge) {
        this(
                maxSegmentAge,
                Time.SYSTEM,
                Ticker.systemTicker()
        );
    }

    @VisibleForTesting
    WorkloadTracker(
            Duration maxSegmentAge,
            Time time,
            Ticker ticker
    ) {
        this.maxSegmentAge = maxSegmentAge;
        this.time = time;

        this.activeSegments = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterAccess(maxSegmentAge.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener(new ExpiredCollectorListener())
                .build(new WorkloadCollectorCacheLoader());
        this.staleWorkloads = new HashMap<>();
    }

    /**
     * Record the throughput for a single record, and if the maximum segment age has been reached
     * for the given topic partition, return the workload for its just-completed segment
     * @param topicPartition the topic partition that the record came from; may not be null
     * @param bytes the size of the record in bytes; may not be negative
     * @param messages the number of records; may not be negative
     * @return the workload for the current segment, if it has reached the maximum segment age
     */
    public Optional<Workload> record(TopicPartition topicPartition, long bytes, long messages) {
        return activeSegments.getUnchecked(topicPartition)
                .record(bytes, messages);
    }

    /**
     * Returns and then discards all stale workloads; that is, workloads for segments that have
     * reached or exceeded the maximum segment age, and for which no throughputs have recently been
     * recorded. Should be invoked periodically in order to prevent memory leaks.
     * @return all known stale workloads; may be empty, but never null
     */
    public Map<TopicPartition, Workload> collectStaleWorkloads() {
        activeSegments.cleanUp();

        Map<TopicPartition, Workload> result;
        synchronized (staleWorkloads) {
            result = new HashMap<>(staleWorkloads);
            staleWorkloads.clear();
        }

        return result;
    }

    /**
     * Clear all workloads, both normal and stale.
     */
    public void clear() {
        activeSegments.asMap().clear();
        staleWorkloads.clear();
    }

    private class WorkloadCollectorCacheLoader extends CacheLoader<TopicPartition, WorkloadCollector> {

        @Override
        public WorkloadCollector load(TopicPartition topicPartition) throws Exception {
            log.debug("Started recording workload for topic partition {}", topicPartition);
            return new WorkloadCollector(maxSegmentAge, time);
        }
    }

    private class ExpiredCollectorListener implements RemovalListener<TopicPartition, WorkloadCollector> {
        @Override
        public void onRemoval(RemovalNotification<TopicPartition, WorkloadCollector> removal) {
            if (!RemovalCause.EXPIRED.equals(removal.getCause())) {
                return;
            }

            // We just simulate recording a throughput of 0 bytes/0 messages; this forces a check
            // to see if the segment is complete, but doesn't have any effect on the throughputs
            // for the topic partition
            Optional<Workload> finalSegmentWorkload = removal.getValue().record(0, 0);
            finalSegmentWorkload.ifPresent(workload -> {
                synchronized (staleWorkloads) {
                    staleWorkloads.put(removal.getKey(), workload);
                }
            });
        }
    }

}
