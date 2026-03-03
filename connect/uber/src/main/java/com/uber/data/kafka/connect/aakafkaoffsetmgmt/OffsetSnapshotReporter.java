package com.uber.data.kafka.connect.aakafkaoffsetmgmt;

import org.apache.kafka.common.TopicPartition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.uber.data.kafka.connect.utils.ExecutorUtils;
import com.uber.jfx.core.UberEnvironment;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Report offset snapshot to AAKafkaOffsetMgmt backend to persist the offset mapping information
 */
public class OffsetSnapshotReporter {

    private static final Logger log = LoggerFactory.getLogger(OffsetSnapshotReporter.class);

    private static final String PELOTON_INSTANCE_ID_KEY = "PELOTON_INSTANCE_ID";

    private final String srcClusterName;
    private final String dstClusterName;
    private final Map<String, PartitionOffsetMap> topicPartitionOffsetMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reportExecutor;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final int postIntervalInMs;
    private final Map<TopicPartition, Long> lastCommittedSrcOffset = new ConcurrentHashMap<>();
    private final OffsetTracker offsetTracker;
    private final OffsetSnapshotClient snapshotClient;
    private final Scope scope;
    private final String routeId;

    public OffsetSnapshotReporter(
            String srcClusterName,
            String dstClusterName,
            int postIntervalInMs,
            OffsetSnapshotClient snapshotClient,
            String routeId
    ) {
        this(srcClusterName, dstClusterName, postIntervalInMs, snapshotClient, routeId, new OffsetTracker(), buildDefaultScope());
    }

    private static Scope buildDefaultScope() {
        return new RootScopeBuilder().reportEvery(Duration.ofSeconds(10));
    }

    @VisibleForTesting
    OffsetSnapshotReporter(
            String srcClusterName,
            String dstClusterName,
            int postIntervalInMs,
            OffsetSnapshotClient snapshotClient,
            String routeId,
            OffsetTracker offsetTracker,
            Scope scope
    ) {
        this.srcClusterName = srcClusterName;
        this.dstClusterName = dstClusterName;
        this.postIntervalInMs = postIntervalInMs;
        this.offsetTracker = offsetTracker;
        this.snapshotClient = snapshotClient;

        this.reportExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("offset-snapshot-reporter-%d").build());
        Map<String, String> envVars = System.getenv();

        @SuppressWarnings("deprecation")
        String deployment = envVars.getOrDefault(UberEnvironment.DEPLOYMENT_KEY, "unknown");
        String pelotonInstanceId = envVars.getOrDefault(PELOTON_INSTANCE_ID_KEY, "unknown");
        this.scope = scope.tagged(ImmutableMap.of(
                "deployment", deployment,
                "peloton_instance_id", pelotonInstanceId));
        this.routeId = routeId;
    }

    /**
     *  Starts to report offset
     */
    public void start() {
        int delayInMs = ThreadLocalRandom.current().nextInt();
        log.info("Start Offset Snapshot Reporter with srcClusterName={}, dstClusterName={}, "
                        + "delayInMs={} and postIntervalInMs={}", srcClusterName, dstClusterName, delayInMs,
                postIntervalInMs);
        reportExecutor.scheduleWithFixedDelay(createReportSnapshotTask(), delayInMs, postIntervalInMs, TimeUnit.MILLISECONDS);
        isStarted.set(true);
    }

    public void record(String srcTopic, int srcPartition, long srcOffset, int dstPartition, long dstOffset) {
        PartitionOffsetMap partitionOffsetMap = topicPartitionOffsetMap.computeIfAbsent(srcTopic, k -> new PartitionOffsetMap());
        partitionOffsetMap.put(dstPartition, srcPartition, dstOffset, srcOffset);
    }

    void updateTopicPartitionOffset(TopicPartition topicPartition) {
        long lastCommitted = lastCommittedSrcOffset.getOrDefault(topicPartition, -1L);
        long lastOffsetCopied = offsetTracker.getHighestCopied().getOrDefault(topicPartition, -1L);
        long lastSkipped = offsetTracker.getHighestSkipped().getOrDefault(topicPartition, -1L);
        if (lastOffsetCopied == -1 && lastCommitted == -1L) {
            log.info(
                    "TopicPartition {} has skipped range of {}->{}, last committed {}. Fetching from AA..",
                    topicPartition, lastOffsetCopied, lastSkipped, lastCommitted);

            Optional<Long> reversedLookup = fetchSnapshotFromAA(topicPartition, lastSkipped);
            if (reversedLookup.isPresent()) {
                PartitionOffsetMap offsetMap = topicPartitionOffsetMap
                        .computeIfAbsent(topicPartition.topic(), k -> new PartitionOffsetMap());
                log.info(
                        "Fetched from AA. Snapshot offset {}, {} for {}->{}. Faking snapshot of {}->{} ",
                        reversedLookup.get(), topicPartition, dstClusterName, srcClusterName, lastSkipped,
                        reversedLookup.get());
                offsetMap.put(topicPartition.partition(), topicPartition.partition(), reversedLookup.get(),
                        lastSkipped);
            } else {
                log.info("Failed to fetch snapshot from AA {}", topicPartition);
            }
        }
    }

    private Runnable createReportSnapshotTask() {
        return () -> {
            try{
                reportSnapshot();
                scope.tagged(ImmutableMap.of("routeid", routeId))
                        .counter("computemapping")
                        .inc(1);
            } catch (Exception e) {
                log.error("Failed to report snapshot", e);
                snapshotClient.markSnapshotReportFailure();
            }
            clearAfterReport();
        };
    }

    @VisibleForTesting
    void reportSnapshot() {
        mergeToLastCommitedMap();
        offsetTracker.getHighestSkipped().keySet().forEach(this::updateTopicPartitionOffset);
        snapshotClient.reportSnapshot(topicPartitionOffsetMap, srcClusterName, dstClusterName);
    }

    @VisibleForTesting
    void clearAfterReport() {
        lastCommittedSrcOffset.clear();
        topicPartitionOffsetMap.clear();
        offsetTracker.clear();
    }

    @VisibleForTesting
    Optional<Long> fetchSnapshotFromAA(TopicPartition topicPartition, long lastSkipped) {
        return snapshotClient.convertOffsetBySourceCluster(topicPartition, dstClusterName, srcClusterName, lastSkipped);
    }

    @VisibleForTesting
    void mergeToLastCommitedMap() {
        topicPartitionOffsetMap.keySet().forEach( topic -> {
            PartitionOffsetMap offsetMap = topicPartitionOffsetMap.get(topic);
            Map<Pair<Integer, Integer>, Pair<Long, Long>> partitionOffsetMap = offsetMap.getPartitionOffsetMap();
            for (Map.Entry<Pair<Integer, Integer>, Pair<Long, Long>> mapping : partitionOffsetMap.entrySet()) {
                int paritionId = mapping.getKey().getRight();
                long offset = mapping.getValue().getRight();
                TopicPartition topicPartition = new TopicPartition(topic, paritionId);
                lastCommittedSrcOffset.put(topicPartition, offset);
            }
        });
    }

    @VisibleForTesting
    Optional<Long> getLastCommittedOffset(TopicPartition tp) {
        return Optional.ofNullable(lastCommittedSrcOffset.get(tp));
    }

    /**
     * Shuts down the reporter
     */
    public void shutdown() {
        log.info("Shutdown Offset Snapshot Reporter");
        if (isStarted.compareAndSet(true, false)) {
            ExecutorUtils.shutdown(reportExecutor, "offset snapshot reporter thread", 1, TimeUnit.SECONDS);
            try {
                snapshotClient.close();
            } catch (IOException ex) {
                log.error("Failed to close snapshot client");
            }
            topicPartitionOffsetMap.clear();
        }
    }

    public boolean isStarted() {
        return isStarted.get();
    }

}