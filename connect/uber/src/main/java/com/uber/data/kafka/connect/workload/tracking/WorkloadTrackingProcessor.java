package com.uber.data.kafka.connect.workload.tracking;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.utils.ExecutorUtils;
import com.uber.data.kafka.connect.utils.MetricsUtils;
import com.uber.data.kafka.connect.workload.common.Workload;
import com.uber.data.kafka.connect.workload.transformation.WorkloadTrackingTransformation;
import com.uber.m3.tally.Scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class WorkloadTrackingProcessor implements AutoCloseable {

    @VisibleForTesting
    static final String WORKLOAD_BYTE_RATE_METRIC = "worker.workloads.byte.rate";
    @VisibleForTesting
    static final String WORKLOAD_MESSAGE_RATE_METRIC = "worker.workloads.message.rate";
    @VisibleForTesting
    static final String WORKLOAD_EMPTY_PARTITIONS_METRIC = "worker.workloads.empty.partitions";
    @VisibleForTesting
    static final String WORKLOAD_STALE_FLUSH_FAILURE_METRIC = "worker.workloads.stale.flush.failures";
    @VisibleForTesting
    static final String WORKLOAD_EMPTY_PUBLISH_FAILURE_METRIC = "worker.workloads.empty.publish.failures";

    private static final Logger log = LoggerFactory.getLogger(WorkloadTrackingTransformation.class);

    private final WorkloadStore workloadStore;
    private final String srcKafka;
    private final String dstKafka;
    private final Supplier<? extends Collection<TopicPartition>> assignedPartitions;
    private final Set<TopicPartition> emptyPartitions;
    private final WorkloadTracker topicWorkloadTracker;
    private final ScheduledExecutorService executor;
    private final Scope scope;
    private final Duration maxSegmentAge;

    public WorkloadTrackingProcessor(
            String srcKafka,
            String dstKafka,
            WorkloadStore workloadStore,
            Supplier<? extends Collection<TopicPartition>> assignedPartitions,
            Duration maxSegmentAge
    ) {
        this(
                srcKafka,
                dstKafka,
                assignedPartitions,
                MetricsUtils.INSTANCE.routeScope(srcKafka, dstKafka),
                workloadStore,
                new WorkloadTracker(maxSegmentAge),
                ExecutorUtils.singleThreadedScheduled("workload-tracking-message-processor-flush"),
                maxSegmentAge
        );
    }

    @VisibleForTesting
    WorkloadTrackingProcessor(
            String srcKafka,
            String dstKafka,
            Supplier<? extends Collection<TopicPartition>> assignedPartitions,
            Scope scope,
            WorkloadStore workloadStore,
            WorkloadTracker topicWorkloadTracker,
            ScheduledExecutorService executor,
            Duration maxSegmentAge
    ) {
        this.srcKafka = Objects.requireNonNull(
                srcKafka,
                "source Kafka cluster name may not be null"
        );
        this.dstKafka = Objects.requireNonNull(
                dstKafka,
                "target Kafka cluster name may not be null"
        );
        this.assignedPartitions = Objects.requireNonNull(
                assignedPartitions,
                "assigned topics supplier may not be null"
        );
        this.scope = Objects.requireNonNull(
                scope,
                "metrics scope may not be null"
        );
        this.workloadStore = Objects.requireNonNull(
                workloadStore,
                "workload store may not be nul"
        );
        this.topicWorkloadTracker = Objects.requireNonNull(
                topicWorkloadTracker,
                "topic workload tracker may not be null"
        );
        this.executor = Objects.requireNonNull(
                executor,
                "flush executor may not be null"
        );
        this.maxSegmentAge = Objects.requireNonNull(
                maxSegmentAge,
                "max segment age may not be null"
        );

        this.emptyPartitions = new HashSet<>();
    }

    public void start() {
        executor.scheduleAtFixedRate(
                this::handleStaleAndEmptyWorkloads,
                maxSegmentAge.toMillis(),
                maxSegmentAge.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    public void track(ConnectRecord<?> record) {
        track(record.topic(), record.kafkaPartition(), numRecordBytes(record), 1);
    }

    @VisibleForTesting
    void track(String topic, int partition, long bytes, long messages) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        synchronized (emptyPartitions) {
            emptyPartitions.remove(topicPartition);
        }
        topicWorkloadTracker.record(topicPartition, bytes, messages)
                .ifPresent(workload -> publishWorkload(topicPartition, workload));
    }

    @Override
    public void close() {
        ExecutorUtils.shutdown(
                executor,
                "executor for workload tracker",
                5,
                TimeUnit.SECONDS
        );

        // Final attempt to flush complete workloads
        flushStaleWorkloads();
    }

    @VisibleForTesting
    void handleStaleAndEmptyWorkloads() {
        try {
            flushStaleWorkloads();
        } catch (Throwable t) {
            log.error("Failed to flush stale workloads", t);
            scope.counter(WORKLOAD_STALE_FLUSH_FAILURE_METRIC).inc(1);
        }

        try {
            publishEmptyWorkloads();
        } catch (Throwable t) {
            log.error("Failed to publish empty workloads", t);
            scope.counter(WORKLOAD_EMPTY_PUBLISH_FAILURE_METRIC).inc(1);
        }
    }

    /**
     * Check for stale segments (i.e., ones that are old enough to complete, but for which no
     * throughput has been recorded after they became old enough), complete them, and flush them.
     */
    @VisibleForTesting
    void flushStaleWorkloads() {
        Map<TopicPartition, Workload> staleWorkloads = topicWorkloadTracker.collectStaleWorkloads();
        if (staleWorkloads.isEmpty()) {
            return;
        }

        synchronized (emptyPartitions) {
            emptyPartitions.removeAll(staleWorkloads.keySet());
        }

        log.debug(
                "Flushing {} stale workloads for topic partitions {}",
                staleWorkloads.size(),
                staleWorkloads.keySet()
        );
        staleWorkloads.forEach(this::publishWorkload);
    }

    /**
     * Check for empty topic partitions (i.e., ones that haven't received any traffic since the
     * last time we checked), and publish a zero-throughput workload for them.
     * <p>
     * This explicit step is necessary because all other workloads are only published after some
     * traffic is received (via one or more calls to {@link #track(String, int, long, long)});
     * for empty partitions, we never see traffic, and instead have to explicitly check for them
     * and publish workloads.
     */
    @VisibleForTesting
    void publishEmptyWorkloads() {
        Set<TopicPartition> partitions = new HashSet<>(assignedPartitions.get());

        // Take a snapshot of the current set of empty partitions so that we don't perform writes
        // to ZooKeeper while holding the lock
        Set<TopicPartition> emptyPartitionsSnapshot;
        synchronized (this.emptyPartitions) {
            emptyPartitionsSnapshot = new HashSet<>(this.emptyPartitions);

            // Assume every partition is empty until we see traffic for it
            this.emptyPartitions.clear();
            this.emptyPartitions.addAll(partitions);
        }

        // Don't publish empty workloads for partitions that are no longer assigned to us
        emptyPartitionsSnapshot.retainAll(partitions);

        scope.gauge(WORKLOAD_EMPTY_PARTITIONS_METRIC)
                .update(emptyPartitionsSnapshot.size());

        emptyPartitionsSnapshot.forEach(tp -> publishWorkload(tp, Workload.EMPTY));
    }

    private void publishWorkload(TopicPartition topicPartition, Workload workload) {
        log.trace(
                "Publishing workload of {} for topic partition {}",
                workload,
                topicPartition
        );

        workloadStore.store(srcKafka, dstKafka, topicPartition, workload);

        scope.tagged(MetricsUtils.INSTANCE.tags(topicPartition))
                .gauge(WORKLOAD_BYTE_RATE_METRIC)
                .update(workload.getBytesPerSecond());
        scope.tagged(MetricsUtils.INSTANCE.tags(topicPartition))
                .gauge(WORKLOAD_MESSAGE_RATE_METRIC)
                .update(workload.getMessagesPerSecond());
    }

    @VisibleForTesting
    static int numRecordBytes(ConnectRecord<?> record) {
        String topic = record.topic();
        int recordBytes = 0;

        if (record.key() instanceof byte[] keyBytes) {
            recordBytes += keyBytes.length;
        } else  {
            log.warn("Key for record in topic {} is not byte array; ignoring", topic);
        }

        if (record.value() instanceof byte[] valueBytes) {
            recordBytes += valueBytes.length;
        } else {
            log.warn("Value for record in topic {} is not byte array; ignoring", topic);
        }

        for (Header header : record.headers()) {
            recordBytes += header.key().length();

            if (header.value() instanceof String stringHeader) {
                recordBytes += stringHeader.length();
            } else if (header.value() instanceof byte[] bytesHeader) {
                recordBytes += bytesHeader.length;
            } else {
                log.debug("Value for header {} on record in topic {} is not byte array; ignoring", header.key(), topic);
            }
        }

        return recordBytes;
    }

}
