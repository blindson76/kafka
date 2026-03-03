package com.uber.data.kafka.connect.workload.common;

import org.apache.kafka.common.utils.Time;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


/**
 * Cache for topic workloads, with logic for automatic eviction of stale workloads and calculating
 * aggregate workloads when multiple samples from different time windows are available.
 * <p>
 * This class is fully thread-safe: all methods may be safely invoked concurrently.
 */
public class WorkloadCache {

    private static final Logger log = LoggerFactory.getLogger(WorkloadCache.class);

    private final Time time;
    private final long maxAgeMs;
    private final long estimationLookbackWindowMs;
    private final Map<ReplicatedTopicPartition, LinkedList<CachedWorkload>> cachedWorkloads;

    /**
     * Create a new cache.
     * @param maxAgeMs the maximum age of any workloads before they are expired from the cache
     * @param estimationLookbackWindowMs the preferred age of workloads when computing the
     *                                   aggregate workload for a topic or topic partition
     */
    public WorkloadCache(
            long maxAgeMs,
            long estimationLookbackWindowMs
    ) {
        this(maxAgeMs, estimationLookbackWindowMs, Time.SYSTEM);
    }

    /**
     * Create a new cache.
     * @param maxAgeMs the maximum age of any workloads before they are expired from the cache
     * @param estimationLookbackWindowMs the preferred age of workloads when computing the
     *                                   aggregate workload for a topic or topic partition
     * @param time pluggable time interface; useful for mocking; may not be null
     */
    @VisibleForTesting
    WorkloadCache(
            long maxAgeMs,
            long estimationLookbackWindowMs,
            Time time
    ) {
        this.time = time;
        this.maxAgeMs = maxAgeMs;
        this.estimationLookbackWindowMs = estimationLookbackWindowMs;
        this.cachedWorkloads = new HashMap<>();
    }

    /**
     * Add a workload for a replicated topic to the cache. Stale workloads for the topic submitted here will
     * also be automatically expired from the cache.
     * @param replicatedTopicPartition the replicated topic partition; may not be null
     * @param workload the topic workload; may not be null
     * @param lastUpdated the time when the workload was recorded
     */
    public void put(ReplicatedTopicPartition replicatedTopicPartition, Workload workload, long lastUpdated) {
        CachedWorkload cachedWorkload = new CachedWorkload(lastUpdated, workload);
        LinkedList<CachedWorkload> workloads;

        if (isStale(cachedWorkload)) {
            // Don't bother inserting the workload if it's already stale
            synchronized (this) {
                workloads = cachedWorkloads.get(replicatedTopicPartition);
            }
        } else {
            synchronized (this) {
                workloads = cachedWorkloads.computeIfAbsent(replicatedTopicPartition, ignored -> new LinkedList<>());
            }
            synchronized (workloads) {
                if (workloads.isEmpty() || workloads.getLast().lastUpdated() < cachedWorkload.lastUpdated()) {
                    workloads.add(cachedWorkload);
                }
            }
        }

        if (workloads == null) {
            return;
        }

        synchronized (workloads) {
            // purge the data points out of the valid window
            while (!workloads.isEmpty() && isStale(workloads.getFirst())) {
                long workloadTimestamp = workloads.getFirst().lastUpdated();
                log.trace("Expiring old workload for {} with timestamp of {}", replicatedTopicPartition, workloadTimestamp);
                workloads.removeFirst();
            }
        }
    }

    /**
     * Gets the workload for a replicated topic partition
     * @param replicatedTopicPartition the replicated topic partition; may not be null
     * @return the cached and aggregated workload for the replicated topic partition, if one can be found
     */
    public Optional<Workload> get(ReplicatedTopicPartition replicatedTopicPartition) {
        Objects.requireNonNull(replicatedTopicPartition, "replicated topic partition may not be null");
        LinkedList<CachedWorkload> allCachedWorkloads = workloadSnapshot(replicatedTopicPartition);
        if (allCachedWorkloads == null) {
            log.debug("There is no workload for {}.", replicatedTopicPartition);
            return Optional.empty();
        }

        // we will return the max workload during the valid window
        long current = time.milliseconds();
        long lookbackWindow = lookbackWindow(current, allCachedWorkloads);

        CachedWorkload maxWorkload = null;
        for (CachedWorkload workload : allCachedWorkloads) {
            if (current - workload.lastUpdated() > lookbackWindow) {
                continue;
            }

            if (maxWorkload == null || maxWorkload.compareTo(workload) < 0) {
                maxWorkload = workload;
            }
        }

        return Optional.ofNullable(maxWorkload).map(CachedWorkload::workload);
    }

    /**
     * Gets the workloads of all known replicated topic partitions
     * @return a map from replicated topic partition to workload; may be empty, but never null
     */
    public Map<ReplicatedTopicPartition, Workload> getAll() {
        Set<ReplicatedTopicPartition> allPartitions;
        synchronized (this) {
            allPartitions = new HashSet<>(cachedWorkloads.keySet());
        }

        Map<ReplicatedTopicPartition, Workload> result = new HashMap<>();
        for (ReplicatedTopicPartition replicatedTopicPartition : allPartitions) {
            Optional<Workload> workload = get(replicatedTopicPartition);
            workload.ifPresent(w -> result.put(replicatedTopicPartition, w));
        }
        return result;
    }

    /**
     * Gets all cached workloads for a replicated topic partition
     * @param replicatedTopicPartition the replicated topic partition; may not be null
     * @return all cached workloads for the replicated topic partition; may be null, and may be empty
     */
    public List<CachedWorkload> getHistorical(ReplicatedTopicPartition replicatedTopicPartition) {
        return workloadSnapshot(replicatedTopicPartition);
    }

    /**
     * Removes all cached workloads for a replicated topic partition
     * @param replicatedTopicPartition the replicated topic partition; may not be null
     * @return whether any workloads were removed for the specified partition
     */
    public boolean remove(ReplicatedTopicPartition replicatedTopicPartition) {
        synchronized (this) {
            return cachedWorkloads.remove(replicatedTopicPartition) != null;
        }
    }

    /**
     * Clear all entries from the cache
     */
    public void clear() {
        synchronized (this) {
            cachedWorkloads.clear();
        }
    }

    private long lookbackWindow(long current, Collection<CachedWorkload> workloads) {
        // if there are workloads inside the estimation lookback window (e.g., 1 hour), then we only consider those
        boolean recentWorkloadsAvailable = workloads.stream()
                .map(CachedWorkload::lastUpdated)
                .anyMatch(lastUpdated -> current - lastUpdated < estimationLookbackWindowMs);
        return recentWorkloadsAvailable ? estimationLookbackWindowMs : maxAgeMs;
    }

    private boolean isStale(CachedWorkload workload) {
        return time.milliseconds() - workload.lastUpdated() > maxAgeMs;
    }

    /**
     * Take a snapshot of the latest-known workloads for a replicated topic partition. This snapshot will
     * belong solely to the caller and is guaranteed not to be modified by other threads or side effects
     * of other modifications made to the cache.
     * @param replicatedTopicPartition the replicated topic partition; may not be null
     * @return the workload snapshot for the partition; may be empty, may be null
     */
    private LinkedList<CachedWorkload> workloadSnapshot(ReplicatedTopicPartition replicatedTopicPartition) {
        List<CachedWorkload> cachedWorkloads;
        synchronized (this) {
            cachedWorkloads = this.cachedWorkloads.get(replicatedTopicPartition);
            if (cachedWorkloads == null) {
                return null;
            }
        }

        synchronized (cachedWorkloads) {
            return new LinkedList<>(cachedWorkloads);
        }
    }

}
