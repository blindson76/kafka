package com.uber.data.kafka.connect.ureplicator3.coordination;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.KafkaCompletableFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.uber.data.kafka.connect.utils.SerializationUtils.parseMapField;
import static com.uber.data.kafka.connect.utils.SerializationUtils.parseNumberField;
import static com.uber.data.kafka.connect.utils.SerializationUtils.parseOptionalNumberField;
import static com.uber.data.kafka.connect.utils.SerializationUtils.parseOptionalStringField;
import static com.uber.data.kafka.connect.utils.SerializationUtils.parseStringField;

// This class is thread-safe
public class AssignmentStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AssignmentStore.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String ASSIGNMENT_FIELD = "assignment-v1";
    private static final String CONNECTOR_FIELD = "connector";
    private static final String SRC_KAFKA_FIELD = "src";
    private static final String DST_KAFKA_FIELD = "dst";
    private static final String TOPIC_FIELD = "topic";
    private static final String PARTITION_FIELD = "partition";

    private static final String TASK_FIELD = "task";
    private static final String DST_TOPIC_FIELD = "dst-topic";
    private static final String DST_PARTITION_FIELD = "dst-partition";

    private final String topic;
    private final int topicPartitions;
    private final short topicReplicationFactor;
    private final Map<String, Object> topicSettings;
    private final Map<String, Object> producerProps;
    private final Map<String, Object> consumerProps;
    private final Map<String, Object> adminProps;
    private final Map<String, Map<ReplicatedTopicPartition, AssignmentValue>> taskPartitionAssignments;
    private final Map<String, Map<Integer, AssignmentListener>> assignmentListeners;

    private TopicAdmin topicAdmin;
    private KafkaBasedLog<byte[], byte[]> kafkaLog;

    public AssignmentStore(AssignmentStoreConfig config) {
        this.topic = config.storageTopic();
        this.topicPartitions = config.storageTopicPartitions();
        this.topicReplicationFactor = config.storageTopicReplicationFactor();
        this.topicSettings = config.storageTopicSettings();
        this.producerProps = producerProps(config.producerConfig());
        this.consumerProps = consumerProps(config.consumerConfig());
        this.adminProps = config.adminConfig();
        this.taskPartitionAssignments = new HashMap<>();
        this.assignmentListeners = new HashMap<>();
    }

    public void start() {
        this.topicAdmin = new TopicAdmin(adminProps);
        this.kafkaLog = new KafkaBasedLog<>(
                topic,
                producerProps,
                consumerProps,
                () -> topicAdmin,
                this::onConsume,
                Time.SYSTEM,
                this::createAndValidateTopic
        );
        kafkaLog.start();
    }

    private static Map<String, Object> producerProps(Map<String, Object> baseProps) {
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        baseProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        return baseProps;
    }

    private static Map<String, Object> consumerProps(Map<String, Object> baseProps) {
        baseProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        baseProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return baseProps;
    }

    private void createAndValidateTopic(TopicAdmin topicAdmin) {
        NewTopic newTopic = TopicAdmin.defineTopic(topic)
                .config(topicSettings)
                .compacted()
                .partitions(topicPartitions)
                .replicationFactor(topicReplicationFactor)
                .build();
        boolean created = topicAdmin.createTopic(newTopic);
        if (!created) {
            // Ensure existing topic is compacted
            boolean verified = topicAdmin.verifyTopicCleanupPolicyOnlyCompact(
                    topic,
                    AssignmentStoreConfig.STORAGE_TOPIC_CONFIG,
                    "task partition assignments"
            );
            // Should never, NEVER happen, but just in case
            if (!verified) {
                throw new KafkaException("Failed to verify that task partition assignment storage topic " + topic + " is compacted");
            }
        }
    }

    @Override
    public void close() {
        if (kafkaLog != null) {
            Utils.closeQuietly(kafkaLog::stop, "Kafka-based log");
        }
        Utils.closeQuietly(topicAdmin, "topic admin");
    }

    public Future<?> assignPartition(String connector, ReplicatedTopicPartition replicatedTopicPartition, int taskId, String dstTopic, Integer dstPartition) {
        return publishAssignment(connector, replicatedTopicPartition, dstTopic, dstPartition, taskId);
    }

    public Future<?> revokePartition(String connector, ReplicatedTopicPartition replicatedTopicPartition) {
        return publishAssignment(connector, replicatedTopicPartition, null, null, null);
    }

    public Map<Integer, ? extends Collection<AssignedPartition>> getAssignments(String connector, String srcKafka, String dstKafka) {
        Objects.requireNonNull(connector);
        Objects.requireNonNull(srcKafka);
        Objects.requireNonNull(dstKafka);

        Map<ReplicatedTopicPartition, AssignmentValue> connectorSnapshot;
        synchronized (this) {
            connectorSnapshot = new HashMap<>(taskPartitionAssignments.getOrDefault(connector, Map.of()));
        }

        return connectorSnapshot.entrySet().stream()
            .filter(e -> e.getKey().matchesPipeline(srcKafka, dstKafka))
            .collect(Collectors.groupingBy(
                e -> e.getValue().taskId(),
                Collectors.mapping(AssignmentStore::assignedPartition, Collectors.toSet())
            ));
    }

    public Collection<AssignedPartition> getAssignment(String connector, int taskId, String srcKafka, String dstKafka) {
        synchronized (this) {
            return taskPartitionAssignments.getOrDefault(connector, Map.of()).entrySet().stream()
                .filter(e -> e.getKey().matchesPipeline(srcKafka, dstKafka))
                .filter(e -> taskId == e.getValue().taskId())
                .map(AssignmentStore::assignedPartition)
                .collect(Collectors.toSet());
        }
    }

    // TODO: Add an API to remove listeners as well (have to be careful not to overwrite listeners on slow task shutdown, though)
    public void addListener(String connector, int taskId, AssignmentListener listener) {
        synchronized (this) {
            assignmentListeners.computeIfAbsent(connector, k -> new HashMap<>())
                .put(taskId, listener);
        }
    }

    public void refresh() {
        try {
            kafkaLog.readToEnd().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException("Failed to read to end of task partition assignment log", e);
        }
    }

    // For testing only
    public void clear() {
        taskPartitionAssignments.clear();
    }

    private Future<?> publishAssignment(String connector, ReplicatedTopicPartition replicatedTopicPartition, String dstTopic, Integer dstPartition, Integer taskId) {
        if (connector == null) {
            log.error("Failed to serialize assignment key with null connector name");
            return KafkaCompletableFuture.failedFuture(new NullPointerException("Connector may not be null"));
        }
        if (replicatedTopicPartition == null) {
            log.error("Failed to serialize assignment key with null replicated topic partition");
            return KafkaCompletableFuture.failedFuture(new NullPointerException("Replicated topic partition may not be null"));
        }

        byte[] key;
        try {
            key = serializeAssignmentKey(connector, replicatedTopicPartition);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize assignment key for connector {} and replicated topic partition {} as JSON", connector, replicatedTopicPartition, e);
            return KafkaCompletableFuture.failedFuture(e);
        }

        byte[] value;
        if (taskId == null) {
            value = null;
        } else {
            try {
                value = serializeAssignmentValue(taskId, dstTopic, dstPartition);
            } catch (JsonProcessingException e) {
                log.error(
                    "Failed to serialize assignment value (task={}, dstTopic={}, dstPartition={}) for connector {} and replicated topic partition {} as JSON",
                    taskId, dstTopic, dstPartition, connector, replicatedTopicPartition, e);
                return KafkaCompletableFuture.failedFuture(e);
            }
        }

        // TODO: Error handling (probably just log and emit a metric)
        return kafkaLog.sendWithReceipt(key, value);
    }

    private void onConsume(Throwable error, ConsumerRecord<byte[], byte[]> record) {
        if (error != null) {
            log.error("Error occurred while consuming from task partition assignment topic", error);
            return;
        }

        int recordPartition = record.partition();
        long recordOffset = record.offset();

        if (record.key() == null) {
            log.error("Ignoring record with null key {} in partition {} at offset {}", record.key(), recordPartition, recordOffset);
            return;
        }

        AssignmentKey assignmentKey;
        try {
            assignmentKey = deserializeAssignmentKey(record.key());
        } catch (Exception e) {
            log.error("Failed to deserialize task partition assignment key in partition {} at offset {} as JSON", recordPartition, recordOffset, e);
            return;
        }

        AssignmentValue assignmentValue;
        try {
            assignmentValue = deserializeAssignmentValue(record.value());
        } catch (IOException e) {
            log.error("Failed to deserialize task partition assignment value in partition {} at offset {} for assignment key {} as JSON", recordPartition, recordOffset, assignmentKey, e);
            return;
        }

        // TODO: Notify listeners
        String connector = assignmentKey.connector();
        ReplicatedTopicPartition replicatedTopicPartition = assignmentKey.replicatedTopicPartition();
        if (assignmentValue == null) {
            log.trace("Picked up revocation of {} for connector {}", replicatedTopicPartition, connector);
            synchronized (this) {
                Map<ReplicatedTopicPartition, AssignmentValue> connectorAssignments = taskPartitionAssignments.get(connector);
                if (connectorAssignments == null) {
                    log.debug("No cached assignments for connector {}", connector);
                } else {
                    AssignmentValue priorAssignment = connectorAssignments.remove(replicatedTopicPartition);
                    if (priorAssignment != null) {
                        notifyListener(connector, priorAssignment.taskId());
                    }
                }
            }
        } else {
            log.trace("Picked up assignment of {} with destination topic {} and destination partition {} to task {} for connector {}",
                replicatedTopicPartition, assignmentValue.dstTopic(), assignmentValue.dstPartition(), assignmentValue.taskId(), connector);
            synchronized (this) {
                Map<ReplicatedTopicPartition, AssignmentValue> connectorAssignments = taskPartitionAssignments.computeIfAbsent(connector, k -> new HashMap<>());
                AssignmentValue priorAssignment = connectorAssignments.put(replicatedTopicPartition, assignmentValue);
                if (priorAssignment != null) {
                    notifyListener(connector, priorAssignment.taskId());
                }
                notifyListener(connector, assignmentValue.taskId());
            }
        }
    }

    private byte[] serializeAssignmentKey(String connector, ReplicatedTopicPartition replicatedTopicPartition) throws JsonProcessingException  {
        // LinkedHashMap preserves order and allows us to deterministically generate a serialized key from a replicated topic partition
        Map<String, Object> innerMap = new LinkedHashMap<>();
        innerMap.put(CONNECTOR_FIELD, connector);
        innerMap.put(SRC_KAFKA_FIELD, replicatedTopicPartition.srcKafka());
        innerMap.put(DST_KAFKA_FIELD, replicatedTopicPartition.dstKafka());
        innerMap.put(TOPIC_FIELD, replicatedTopicPartition.topicPartition().topic());
        innerMap.put(PARTITION_FIELD, replicatedTopicPartition.topicPartition().partition());
        Map<String, Object> wrapped = Map.of(ASSIGNMENT_FIELD, innerMap);
        return OBJECT_MAPPER.writeValueAsBytes(wrapped);
    }

    private byte[] serializeAssignmentValue(int taskId, String dstTopic, Integer dstPartition) throws JsonProcessingException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(TASK_FIELD, taskId);
        if (dstTopic != null)
            map.put(DST_TOPIC_FIELD, dstTopic);
        if (dstPartition != null)
            map.put(DST_PARTITION_FIELD, dstPartition);
        return OBJECT_MAPPER.writeValueAsBytes(map);
    }

    private AssignmentKey deserializeAssignmentKey(byte[] key) throws IOException {
        if (key == null)
            throw new IOException("Key should not be null");

        Map<String, Object> map = OBJECT_MAPPER.readValue(key, new TypeReference<>() { });
        Map<?, ?> wrapped = parseMapField(true, map, ASSIGNMENT_FIELD);

        String connector = parseStringField(true, wrapped, CONNECTOR_FIELD);
        String srcKafka = parseStringField(true, wrapped, SRC_KAFKA_FIELD);
        String dstKafka = parseStringField(true, wrapped, DST_KAFKA_FIELD);
        String topic = parseStringField(true, wrapped, TOPIC_FIELD);
        Number partition = parseNumberField(true, wrapped, PARTITION_FIELD);
        ReplicatedTopicPartition replicatedTopicPartition = new ReplicatedTopicPartition(srcKafka, dstKafka, topic, partition.intValue());

        return new AssignmentKey(connector, replicatedTopicPartition);
    }

    private AssignmentValue deserializeAssignmentValue(byte[] value) throws IOException {
        // Permit null values to signify revocation of the partition
        if (value == null)
            return null;

        Map<String, Object> map = OBJECT_MAPPER.readValue(value, new TypeReference<>() { });
        int taskId = parseNumberField(false, map, TASK_FIELD).intValue();
        String dstTopic = parseOptionalStringField(false, map, DST_TOPIC_FIELD);
        Number rawDstPartition = parseOptionalNumberField(false, map, DST_PARTITION_FIELD);
        Integer dstPartition = rawDstPartition != null ? rawDstPartition.intValue() : null;
        return new AssignmentValue(taskId, dstTopic, dstPartition);
    }

    private void notifyListener(String connector, int taskId) {
        AssignmentListener listener = assignmentListeners.getOrDefault(connector, Map.of())
            .get(taskId);

        if (listener == null)
            return;

        try {
            listener.onUpdate();
        } catch (Throwable t) {
            log.error("Listener for {}-{} threw an exception", connector, taskId, t);
        }
    }

    private static AssignedPartition assignedPartition(Map.Entry<ReplicatedTopicPartition, AssignmentValue> entry) {
        return new AssignedPartition(entry.getKey().topic(), entry.getKey().partition(), entry.getValue().dstTopic(), entry.getValue().dstPartition);
    }

    private record AssignmentKey(String connector, ReplicatedTopicPartition replicatedTopicPartition) {
    }

    private record AssignmentValue(int taskId, String dstTopic, Integer dstPartition) {
    }
}
