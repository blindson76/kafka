package com.uber.data.kafka.connect.workload.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
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
import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.utils.UberConnectUtils;
import com.uber.data.kafka.connect.workload.common.CachedWorkload;
import com.uber.data.kafka.connect.workload.common.Workload;
import com.uber.data.kafka.connect.workload.common.WorkloadCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.uber.data.kafka.connect.utils.SerializationUtils.parseMapField;
import static com.uber.data.kafka.connect.utils.SerializationUtils.parseNumberField;
import static com.uber.data.kafka.connect.utils.SerializationUtils.parseStringField;

class KafkaBasedWorkloadStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaBasedWorkloadStore.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @VisibleForTesting
    static final long MAX_VALID_TIME_WINDOW_IN_MS = TimeUnit.HOURS.toMillis(25);
    @VisibleForTesting
    static final long ESTIMATION_LOOKBACK_WINDOW_IN_MS = TimeUnit.HOURS.toMillis(1);

    private static final String WORKLOAD_FIELD = "workload-v1";
    private static final String SRC_KAFKA_FIELD = "src";
    private static final String DST_KAFKA_FIELD = "dst";
    private static final String TOPIC_FIELD = "topic";
    private static final String PARTITION_FIELD = "partition";
    private static final String BYTE_RATE_FIELD = "bytes";
    private static final String MESSAGE_RATE_FIELD = "messages";

    private final String topic;
    private final int topicPartitions;
    private final short topicReplicationFactor;
    private final Map<String, Object> topicSettings;
    private final Map<String, Object> producerProps;
    private final Map<String, Object> consumerProps;
    private final Map<String, Object> adminProps;
    private final WorkloadCache workloadsCache;
    private final Time time;

    private TopicAdmin topicAdmin;
    private KafkaBasedLog<byte[], byte[]> kafkaLog;

    public KafkaBasedWorkloadStore(KafkaBasedWorkloadProviderConfig config) {
        this(config, Time.SYSTEM);
    }

    @VisibleForTesting
    KafkaBasedWorkloadStore(KafkaBasedWorkloadProviderConfig config, Time time) {
        this.topic = config.storageTopic();
        this.topicPartitions = config.storageTopicPartitions();
        this.topicReplicationFactor = config.storageTopicReplicationFactor();
        this.topicSettings = config.storageTopicSettings();
        this.producerProps = producerProps(config.producerConfig());
        this.consumerProps = consumerProps(config.consumerConfig());
        this.adminProps = config.adminConfig();
        this.workloadsCache = new WorkloadCache(MAX_VALID_TIME_WINDOW_IN_MS, ESTIMATION_LOOKBACK_WINDOW_IN_MS);
        this.time = time;
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
                    KafkaBasedWorkloadProviderConfig.STORAGE_TOPIC_CONFIG,
                    "workloads"
            );
            // Should never, NEVER happen, but just in case
            if (!verified) {
                throw new KafkaException("Failed to verify that workload storage topic " + topic + " is compacted");
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

    public Future<?> putWorkload(String srcKafka, String dstKafka, TopicPartition topicPartition, Workload workload) {
        ReplicatedTopicPartition replicatedTopicPartition = new ReplicatedTopicPartition(srcKafka, dstKafka, topicPartition);

        byte[] key;
        try {
            key = serializeWorkloadKey(replicatedTopicPartition);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize workload key for replicated topic partition {} as JSON", replicatedTopicPartition, e);
            return KafkaCompletableFuture.failedFuture(e);
        }

        byte[] value;
        try {
            value = serializeWorkloadValue(workload);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize workload value {} for replicated topic partition {} as JSON", replicatedTopicPartition, e);
            return KafkaCompletableFuture.failedFuture(e);
        }

        // TODO: Error handling (probably just log and emit a metric)
        return kafkaLog.sendWithReceipt(key, value);
    }

    public Workload getWorkload(String srcKafka, String dstKafka, TopicPartition topicPartition) {
        ReplicatedTopicPartition replicatedTopicPartition = new ReplicatedTopicPartition(srcKafka, dstKafka, topicPartition);
        return workloadsCache.get(replicatedTopicPartition).orElse(null);
    }

    public Map<ReplicatedTopicPartition, Workload> getAllWorkloads() {
        return workloadsCache.getAll();
    }

    public List<CachedWorkload> getHistoricalWorkloads(String srcKafka, String dstKafka, TopicPartition topicPartition) {
        ReplicatedTopicPartition replicatedTopicPartition = new ReplicatedTopicPartition(srcKafka, dstKafka, topicPartition);
        return workloadsCache.getHistorical(replicatedTopicPartition);
    }

    public void refresh() {
        try {
            kafkaLog.readToEnd().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException("Failed to read to end of workload log", e);
        }
    }

    public void clear() {
        workloadsCache.clear();
    }

    private void onConsume(Throwable error, ConsumerRecord<byte[], byte[]> record) {
        if (error != null) {
            log.error("Error occurred while consuming from workload topic", error);
            return;
        }

        int recordPartition = record.partition();
        long recordOffset = record.offset();

        ReplicatedTopicPartition replicatedTopicPartition;
        try {
            replicatedTopicPartition = deserializeWorkloadKey(record.key());
        } catch (IOException e) {
            log.error("Failed to deserialize replicated topic partition in partition {} at offset {} as JSON", recordPartition, recordOffset, e);
            return;
        }

        Workload workload;
        try {
            workload = deserializeWorkloadValue(record.value());
        } catch (IOException e) {
            log.error("Failed to deserialize workload in partition {} at offset {} for replicated topic partition {} as JSON", recordPartition, recordOffset, replicatedTopicPartition, e);
            return;
        }

        if (workload != null) {
            long timestamp = UberConnectUtils.extractTimestamp(record)
                    .orElseGet(() -> {
                        log.warn("No timestamp found for record in partition {} at offset {}; using current wall-clock time instead", recordPartition, recordOffset);
                        return time.milliseconds();
                    });

            workloadsCache.put(replicatedTopicPartition, workload, timestamp);
        } else {
            workloadsCache.remove(replicatedTopicPartition);
        }
    }

    private byte[] serializeWorkloadKey(ReplicatedTopicPartition replicatedTopicPartition) throws JsonProcessingException  {
        // LinkedHashMap preserves order and allows us to deterministically generate a serialized key from a replicated topic partition
        Map<String, Object> innerMap = new LinkedHashMap<>();
        innerMap.put(SRC_KAFKA_FIELD, replicatedTopicPartition.srcKafka());
        innerMap.put(DST_KAFKA_FIELD, replicatedTopicPartition.dstKafka());
        innerMap.put(TOPIC_FIELD, replicatedTopicPartition.topicPartition().topic());
        innerMap.put(PARTITION_FIELD, replicatedTopicPartition.topicPartition().partition());
        Map<String, Object> wrapped = Map.of(WORKLOAD_FIELD, innerMap);
        return OBJECT_MAPPER.writeValueAsBytes(wrapped);
    }

    private ReplicatedTopicPartition deserializeWorkloadKey(byte[] key) throws IOException {
        if (key == null)
            throw new IOException("record has no key");

        Map<String, Object> map = OBJECT_MAPPER.readValue(key, new TypeReference<>() { });
        Map<?, ?> wrapped = parseMapField(true, map, WORKLOAD_FIELD);

        String srcKafka = parseStringField(true, wrapped, SRC_KAFKA_FIELD);
        String dstKafka = parseStringField(true, wrapped, DST_KAFKA_FIELD);
        String topic = parseStringField(true, wrapped, TOPIC_FIELD);
        Number partition = parseNumberField(true, wrapped, PARTITION_FIELD);

        return new ReplicatedTopicPartition(srcKafka, dstKafka, topic, partition.intValue());
    }

    private byte[] serializeWorkloadValue(Workload workload) throws JsonProcessingException  {
        // This allows us to write tombstone records for replicated topic partitions
        if (workload == null)
            return null;

        Map<String, Object> map = Map.of(
                BYTE_RATE_FIELD, workload.getBytesPerSecond(),
                MESSAGE_RATE_FIELD, workload.getMessagesPerSecond()
        );
        return OBJECT_MAPPER.writeValueAsBytes(map);
    }

    private Workload deserializeWorkloadValue(byte[] value) throws IOException {
        // Permit null values (tombstones) that indicate we should clear the cache for this replicated topic partition
        if (value == null)
            return null;

        Map<String, Object> map = OBJECT_MAPPER.readValue(value, new TypeReference<>() { });

        Number byteRate = parseNumberField(false, map, BYTE_RATE_FIELD);
        Number messageRate = parseNumberField(false, map, MESSAGE_RATE_FIELD);

        return new Workload(byteRate.doubleValue(), messageRate.doubleValue());
    }

}
