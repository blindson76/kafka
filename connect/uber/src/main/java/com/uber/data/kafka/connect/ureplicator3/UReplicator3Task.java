package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.uber.data.kafka.connect.aakafkaoffsetmgmt.OffsetSnapshotClient;
import com.uber.data.kafka.connect.aakafkaoffsetmgmt.OffsetSnapshotReporter;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignedPartition;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStore;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStores;
import com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProvider;
import com.uber.data.kafka.connect.workload.tracking.WorkloadTrackingProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class UReplicator3Task extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(UReplicator3Task.class);

    public static final String SOURCE_PARTITION_SOURCE_CLUSTER = "source.cluster";
    public static final String SOURCE_PARTITION_TARGET_CLUSTER = "target.cluster";
    public static final String SOURCE_PARTITION_KAFKA_TOPIC = "topic";
    public static final String SOURCE_PARTITION_KAFKA_PARTITION = "partition";
    public static final String SOURCE_OFFSET_KAFKA_OFFSET = "offset";

    private Time time;
    private UReplicator3TaskConfig config;
    private Consumer<byte[], byte[]> sourceConsumer;
    private WorkloadTrackingProcessor workloadTracker;
    private AssignmentStore partitionAssignmentStore;
    private volatile boolean assignmentChanged;
    private Map<TopicPartition, TopicPartition> overrides;
    private OffsetSnapshotReporter offsetSnapshotReporter;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting task");

        time = Time.SYSTEM;

        config = new UReplicator3TaskConfig(props);

        overrides = Map.of();

        partitionAssignmentStore = AssignmentStores.get();
        partitionAssignmentStore.addListener(config.connectorName(), config.taskId(), this::onAssignmentUpdate);

        sourceConsumer = config.sourceConsumer();

        // Force us to update the consumer assignment before polling it for the first time,
        // since right now it's not assigned anything
        assignmentChanged = true;

        initializeWorkloadTracker();
        initializeOffsetSnapshotReporter();

        log.info("Finished starting task");
    }

    @Override
    public List<SourceRecord> poll() {
        log.trace("Starting to poll for records"); // This gets called a lot; can't put it at INFO like the others

        // Consumer isn't thread-safe, so we make sure to update its partition assignment on the same thread
        // that we use to poll it for records
        if (assignmentChanged) {
            // Reset the flag before updating the assignment; that way, any subsequent assignment updates
            // (which will be picked up on a separate thread and may be observed concurrently with our consumer update)
            // won't be skipped
            assignmentChanged = false;
            updateAssignment();
        }

        if (sourceConsumer.assignment().isEmpty()) {
            log.trace("Task is not assigned any partitions; will idle and return empty list of records");
            // Idle here so that we don't enter a tight loop where no (real) work is done. We can't block indefinitely though,
            // since the task may be scheduled for shutdown and the same thread that calls this method is also responsible
            // for calling Task::stop
            // We may want to make this configurable in the future but for now this should be fine
            time.sleep(100);
            return List.of();
        }

        ConsumerRecords<byte[], byte[]> records = sourceConsumer.poll(config.consumerPollTimeout());

        List<SourceRecord> result = new ArrayList<>(records.count());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            SourceRecord sourceRecord = convertRecord(record);

            if (workloadTracker != null)
                workloadTracker.track(sourceRecord);

            result.add(sourceRecord);
        }

        log.trace("Finished polling for records");
        return result;
    }

    @Override
    public void stop() {
        log.info("Stopping task");
        Utils.closeQuietly(sourceConsumer, "source consumer");
        Utils.closeQuietly(workloadTracker, "workload tracker");
        if (offsetSnapshotReporter != null)
            Utils.closeQuietly(offsetSnapshotReporter::shutdown, "offset snapshot reporter");
        log.info("Finished stopping task");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (offsetSnapshotReporter != null) {
            TopicPartition dstTopicPartition = new TopicPartition(metadata.topic(), metadata.partition());
            if (!metadata.hasOffset()) {
                log.warn("Record from topic partition {} does not have offset; cannot report offsets for it", dstTopicPartition);
                return;
            }
            long dstOffset = metadata.offset();

            TopicPartition srcTopicPartition = decodeSourcePartition(record.sourcePartition());
            if (srcTopicPartition == null) {
                log.warn("Could not decode source partition for record sent to topic partition {}; cannot report offsets for it", dstTopicPartition);
                return;
            }

            Long srcOffset = decodeSourceOffset(srcTopicPartition, record.sourceOffset());
            if (srcOffset == null) {
                log.warn("Could not decode source offset for record sent to topic partition {}; cannot report offsets for it", dstTopicPartition);
                return;
            }

            offsetSnapshotReporter.record(srcTopicPartition.topic(), srcTopicPartition.partition(), srcOffset, dstTopicPartition.partition(), dstOffset);
        }
    }

    private Collection<AssignedPartition> assignedPartitions() {
        return partitionAssignmentStore.getAssignment(
                config.connectorName(),
                config.taskId(),
                config.sourceCluster(),
                config.targetCluster()
        );
    }

    private static Set<TopicPartition> sourcePartitions(Collection<AssignedPartition> partitions) {
        return partitions.stream().map(AssignedPartition::source).collect(Collectors.toSet());
    }

    private void updateAssignment() {
        log.debug("Updating consumer assignment");

        Set<TopicPartition> existingAssignment = sourceConsumer.assignment();
        Collection<AssignedPartition> newAssignment = assignedPartitions();
        Set<TopicPartition> sourceAssignment = sourcePartitions(newAssignment);
        sourceConsumer.assign(sourceAssignment);

        List<TopicPartition> newPartitions = sourceAssignment.stream()
            .filter(Predicate.not(existingAssignment::contains))
            .toList();
        List<Map<String, Object>> newSourcePartitions = newPartitions.stream()
            .map(tp -> encodeSourcePartition(tp.topic(), tp.partition()))
            .toList();

        if (!newPartitions.isEmpty()) {
            log.debug("Assigned new partition(s): {}", newPartitions);
        } else {
            log.debug("Assigned no new partitions; revocation or topic/partition override may have taken place");
        }

        context.offsetStorageReader().offsets(newSourcePartitions).forEach((sourcePartition, sourceOffset) -> {
            TopicPartition topicPartition = decodeSourcePartition(sourcePartition);
            if (topicPartition == null) {
                return;
            }

            Long offset = decodeSourceOffset(topicPartition, sourceOffset);
            if (offset == null) {
                log.trace("Found no committed offset for newly-assigned partition {}", topicPartition);
                return;
            }

            log.trace("Found committed offset {} for newly-assigned partition {}", offset, topicPartition);
            // Stored offset is what we were last able to replicate; we should seek to one beyond that
            sourceConsumer.seek(topicPartition, offset + 1);
        });

        overrides = newAssignment.stream()
            .filter(AssignedPartition::hasOverride)
            .collect(Collectors.toMap(
                AssignedPartition::source,
                AssignedPartition::target
            ));
    }

    private void initializeWorkloadTracker() {
        Duration workloadReportInterval = config.workloadReportInterval();
        if (workloadReportInterval == null) {
            log.warn("Workload tracking is disabled");
            return;
        }

        log.debug("Initializing workload tracker");
        workloadTracker = new WorkloadTrackingProcessor(
                config.sourceCluster(),
                config.targetCluster(),
                // TODO: Enable workload tracking to be performed even if the workload provider isn't set up on this worker
                KafkaBasedWorkloadProvider::publishWorkload,
                () -> sourcePartitions(assignedPartitions()),
                workloadReportInterval
        );
        workloadTracker.start();
        log.debug("Finished initializing workload tracker");
    }

    private void initializeOffsetSnapshotReporter() {
        if (!config.offsetReporterEnabled()) {
            log.warn("Offset reporter is disabled");
            return;
        }

        log.debug("Initializing offset reporter");
        // We may want to make some of these values configurable in the future, but for now
        // this is fine since they're the values used across the entire uReplicator2 fleet
        int offsetReportIntervalMs = 600_000;
        String offsetSnapshotPostUrl = "http://127.0.0.1:18668/snapshot";
        int offsetSnapshotRequestTimeoutMs = 10_000;
        OffsetSnapshotClient offsetSnapshotClient = new OffsetSnapshotClient(offsetSnapshotPostUrl, offsetSnapshotRequestTimeoutMs);

        // 🤷 Seems as reasonable as anything I guess
        String routeId = config.connectorName() + "-" + config.sourceCluster() + "-" + config.targetCluster();
        offsetSnapshotReporter = new OffsetSnapshotReporter(
                config.sourceCluster(),
                config.targetCluster(),
                offsetReportIntervalMs,
                offsetSnapshotClient,
                routeId
        );
        offsetSnapshotReporter.start();
        log.debug("Initialized offset reporter");
    }

    private SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition source = new TopicPartition(record.topic(), record.partition());
        TopicPartition target = overrides.getOrDefault(source, source);

        return new SourceRecord(
                encodeSourcePartition(source.topic(), source.partition()),
                encodeSourceOffset(record),
                target.topic(),
                target.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA,
                record.key(),
                Schema.OPTIONAL_BYTES_SCHEMA,
                record.value(),
                record.timestamp(),
                convertHeaders(record.headers())
        );
    }

    private Map<String, Object> encodeSourcePartition(String topic, int partition) {
        return encodeSourcePartition(config.sourceCluster(), config.targetCluster(), topic, partition);
    }

    public static Map<String, Object> encodeSourcePartition(String srcKafka, String dstKafka, String topic, int partition) {
        // LinkedHashMap to guarantee order of key/value pairs across serialization
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(SOURCE_PARTITION_SOURCE_CLUSTER, srcKafka);
        result.put(SOURCE_PARTITION_TARGET_CLUSTER, dstKafka);
        result.put(SOURCE_PARTITION_KAFKA_TOPIC, topic);
        result.put(SOURCE_PARTITION_KAFKA_PARTITION, partition);
        return result;
    }

    private Map<String, Object> encodeSourceOffset(ConsumerRecord<?, ?> record) {
        return encodeSourceOffset(record.offset());
    }

    public static Map<String, Object> encodeSourceOffset(long offset) {
        return Map.of(SOURCE_OFFSET_KAFKA_OFFSET, offset);
    }

    private TopicPartition decodeSourcePartition(Map<String, ?> sourcePartition) {
        Object rawTopic = sourcePartition.get(SOURCE_PARTITION_KAFKA_TOPIC);
        if (rawTopic == null) {
            log.warn("Ignoring invalid source partition {}; missing {} field", sourcePartition, SOURCE_PARTITION_KAFKA_TOPIC);
            return null;
        }
        if (!(rawTopic instanceof String)) {
            log.warn("Ignoring invalid source partition {}; field {} is not a string", sourcePartition, SOURCE_PARTITION_KAFKA_TOPIC);
            return null;
        }

        Object rawPartition = sourcePartition.get(SOURCE_PARTITION_KAFKA_PARTITION);
        if (rawPartition == null) {
            log.warn("Ignoring invalid source partition {}; missing {} field", sourcePartition, SOURCE_PARTITION_KAFKA_PARTITION);
            return null;
        }
        if (!(rawPartition instanceof Number)) {
            log.warn("Ignoring invalid source partition {}; field {} is not a number", sourcePartition, SOURCE_PARTITION_KAFKA_PARTITION);
            return null;
        }

        String topic = (String) rawTopic;
        int partition = ((Number) rawPartition).intValue();
        return new TopicPartition(topic, partition);
    }

    private Long decodeSourceOffset(TopicPartition topicPartition, Map<String, ?> sourceOffset) {
        if (sourceOffset == null) {
            log.debug("No source offset found for {}; this partition is probably being replicated for the first time", topicPartition);
            return null;
        }

        Object rawOffset = sourceOffset.get(SOURCE_OFFSET_KAFKA_OFFSET);
        if (rawOffset == null) {
            log.warn("Ignoring invalid source offset {}; missing {} field", sourceOffset, SOURCE_OFFSET_KAFKA_OFFSET);
            return null;
        }
        if (!(rawOffset instanceof Number)) {
            log.warn("Ignoring invalid source offset {}; field {} is not a number", sourceOffset, SOURCE_OFFSET_KAFKA_OFFSET);
            return null;
        }

        return ((Number) rawOffset).longValue();
    }

    private static Headers convertHeaders(org.apache.kafka.common.header.Headers headers) {
        ConnectHeaders result = new ConnectHeaders();
        for (Header header : headers) {
            result.addBytes(header.key(), header.value());
        }
        return result;
    }

    private void onAssignmentUpdate() {
        this.assignmentChanged = true;
    }

}
