package com.uber.data.kafka.connect.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStore;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;

public final class UberConnectUtils {

    private static final Logger log = LoggerFactory.getLogger(UberConnectUtils.class);


    @SafeVarargs
    public static <E> Set<E> diff(Collection<E> base, Collection<E>... toSubtract) {
        Set<E> result = new HashSet<>(base);
        for (Collection<E> subtracted : toSubtract) {
            if (subtracted != null)
                result.removeAll(subtracted);
        }
        return result;
    }

    public static Set<String> listTopics(
            Admin admin, long timeout, TimeUnit timeUnit
    ) throws InterruptedException, TimeoutException, ExecutionException {
        return admin.listTopics().listings().get(timeout, timeUnit).stream()
                .filter(Predicate.not(TopicListing::isInternal))
                .map(TopicListing::name)
                .collect(Collectors.toSet());
    }

    public static Map<String, Integer> listPartitions(Collection<String> topics, Admin admin, long timeout, TimeUnit timeUnit) {
        int timeoutMs = (int) Math.max(Integer.MAX_VALUE, timeUnit.toMillis(timeout));
        DescribeTopicsOptions options = new DescribeTopicsOptions()
                .includeAuthorizedOperations(false) // Don't care about ACLS
                .timeoutMs(timeoutMs);

        Map<String, Integer> result = new HashMap<>();
        admin.describeTopics(topics, options).topicNameValues().forEach((topic, topicDescriptionFuture) -> {
            TopicDescription topicDescription;
            try {
                topicDescription = topicDescriptionFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                log.warn("Unable to describe topic {}", topic, e);
                return;
            }

            result.put(topic, topicDescription.partitions().size());
        });

        return result;
    }

    /**
     * Extract and return a record's {@link ConsumerRecord#timestamp() timestamp}, if it has one
     * @param record the record; may not be null
     * @return an {@link Optional} containing the record's timestamp, or {@link Optional#empty()}
     * if the record does not have a timestamp
     */
    public static Optional<Long> extractTimestamp(ConsumerRecord<?, ?> record) {
        return Optional.of(record)
                .filter(r -> !TimestampType.NO_TIMESTAMP_TYPE.equals(record.timestampType()))
                .map(ConsumerRecord::timestamp)
                .filter(ts -> ts >= 0);
    }

    public static String createErrorMessage(String base, Throwable t) {
        String separator = ": ";
        String result = base;
        while (t != null) {
            if (t.getMessage() != null)
                result += separator +  t.getMessage();
            t = t.getCause();
        }

        return result;
    }

    public static void addError(Config configValidationResult, String prop, Object value, String baseErrorMessage, Throwable error) {
        addError(configValidationResult, prop, value, createErrorMessage(baseErrorMessage, error));
    }

    public static void addError(Config configValidationResult, String prop, Object value, String errorMessage) {
        findOrCreate(configValidationResult, prop, value)
                .addErrorMessage(errorMessage);
    }

    public static ConfigValue findOrCreate(Config configValidationResult, String prop, Object value) {
        for (ConfigValue definedValue : configValidationResult.configValues()) {
            if (Objects.equals(prop, definedValue.name())) {
                return definedValue;
            }
        }

        ConfigValue newValue = value == null ? new ConfigValue(prop) : new ConfigValue(prop, value, new ArrayList<>(), new ArrayList<>());
        configValidationResult.configValues().add(newValue);
        return newValue;
    }

    public static Set<ReplicatedTopicPartition> assignedPartitions(String connector, int taskId, Map<String, String> connectorConfig) {
        String connectorClass = connectorConfig.get(CONNECTOR_CLASS_CONFIG);
        if (connectorClass == null) {
            log.warn("No connector class found for connector {}", connector);
            return null;
        }

        if (!isURep3(connectorClass)) {
            log.warn("Ignoring task {}-{} with unsupported connector type {}", connector, taskId, connectorClass);
            return null;
        }

        String srcKafka = connectorConfig.get(UReplicator3ConnectorConfig.SOURCE_CLUSTER_CONFIG);
        String dstKafka = connectorConfig.get(UReplicator3ConnectorConfig.TARGET_CLUSTER_CONFIG);
        return uRep3Assignment(connector, taskId, srcKafka, dstKafka);
    }

    public static boolean isURep3(Map<String, String> connectorConfig) {
        String connectorClass = connectorConfig.get(CONNECTOR_CLASS_CONFIG);
        if (connectorClass == null) {
            log.warn("No connector class found for connector");
            return false;
        }

        return isURep3(connectorClass);
    }

    public static boolean isURep3(String connectorClass) {
        return Set.of(
            "com.uber.data.kafka.connect.ureplicator3.UReplicator3Connector",
            "UReplicator3Connector",
            "UReplicator3"
        ).contains(connectorClass);
    }

    public static Set<ReplicatedTopicPartition> uRep3Assignment(
        String connector,
        int taskId,
        String srcKafka,
        String dstKafka
    ) {
        AssignmentStore store = AssignmentStores.get();
        return store.getAssignment(connector, taskId, srcKafka, dstKafka)
            .stream()
            .map(ap -> new ReplicatedTopicPartition(srcKafka, dstKafka, ap.source()))
            .collect(Collectors.toSet());
    }

    public static void awaitAll(
        Collection<? extends Future<?>> futures, Duration duration
    ) throws InterruptedException, TimeoutException, ExecutionException {
        Timer timer = Time.SYSTEM.timer(duration);
        for (Future<?> future : futures) {
            timer.update();
            long remaining = Math.max(1, timer.remainingMs());
            future.get(remaining, TimeUnit.MILLISECONDS);
        }
    }

    public static int compareLexicographically(TopicPartition first, TopicPartition second) {
        if (first == null)
            return second == null ? 0 : -1;

        if (second == null)
            return 1;

        int result = first.topic().compareTo(second.topic());
        if (result != 0)
            return result;

        return Integer.compare(first.partition(), second.partition());
    }
}
