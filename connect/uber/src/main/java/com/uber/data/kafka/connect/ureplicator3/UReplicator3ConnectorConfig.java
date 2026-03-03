package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.uber.data.kafka.connect.ureplicator3.UReplicator3TaskConfig.TASK_ID_CONFIG;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.addError;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.diff;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.listTopics;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

public class UReplicator3ConnectorConfig extends UReplicator3Config {

    private static final Logger log = LoggerFactory.getLogger(UReplicator3ConnectorConfig.class);

    public static final String TOPICS_CONFIG = "topics";
    private static final String TOPICS_DOC = "Comma-separated list of topics to replicate";

    public static final String WORKER_TASK_LIMIT_CONFIG = "worker.task.limit";
    public static final int WORKER_TASK_LIMIT_DEFAULT = 2;
    private static final String WORKER_TASK_LIMIT_DOC = "Maximum number of tasks to generate per worker. "
            + "To allow any number of tasks per worker, set to a non-positive value";

    public static final String TASK_BYTE_RATE_THRESHOLD_CONFIG = "task.byte.rate.threshold";
    public static final double TASK_BYTE_RATE_THRESHOLD_DEFAULT = 70 * 1024 * 1024;
    private static final String TASK_BYTE_RATE_THRESHOLD_DOC = "Maximum total byte rate for partitions assigned "
            + "to each task before attempting to revoke partitions and redistribute to other tasks";

    public static final String TASK_MESSAGE_RATE_THRESHOLD_CONFIG = "task.message.rate.threshold";
    public static final double TASK_MESSAGE_RATE_THRESHOLD_DEFAULT = 70_000;
    private static final String TASK_MESSAGE_RATE_THRESHOLD_DOC = "Maximum total message rate for partitions assigned "
            + "to each task before attempting to revoke partitions and redistribute to other workers";

    public static final String REFRESH_INTERVAL_MS = "refresh.interval.ms";
    public static final long REFRESH_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
    private static final String REFRESH_INTERVAL_MS_DOC = "How often to check for changes in the partition counts of source and target topics, "
            + "the size of the Kafka Connect cluster, and imbalances in the partitions assigned to each task, "
            + "possibly generating new task configs and/or reassigning partitions in response";

    public static final String CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_CONFIG = "cluster.size.reconfiguration.threshold";
    public static final double CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_DEFAULT = 10;
    private static final String CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_DOC = "Minimum percentage difference "
            + "in cluster size (i.e., number of workers) before new task configs are generated";

    public static final String TOPIC_RENAME_PREFIX = "topic.rename.";

    private final Map<String, String> topicRenames;
    private final Set<String> sourceTopics;
    private final Set<String> targetTopics;

    @SuppressWarnings("this-escape")
    public UReplicator3ConnectorConfig(Map<String, ?> props) {
        super(config(), props);
        this.topicRenames = Collections.unmodifiableMap(parseTopicRenames(originalsStrings(), null));
        this.sourceTopics = Set.copyOf(getList(TOPICS_CONFIG));
        this.targetTopics = Collections.unmodifiableSet(computeTargetTopics(sourceTopics, topicRenames));
    }

    protected static ConfigDef config() {
        return baseConfig()
                .define(
                        TOPICS_CONFIG,
                        Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.ValidList.anyNonDuplicateValues(false, false),
                        Importance.HIGH,
                        TOPICS_DOC
                ).define(
                        WORKER_TASK_LIMIT_CONFIG,
                        Type.INT,
                        WORKER_TASK_LIMIT_DEFAULT,
                        Importance.MEDIUM,
                        WORKER_TASK_LIMIT_DOC
                ).define(
                        TASK_BYTE_RATE_THRESHOLD_CONFIG,
                        Type.DOUBLE,
                        TASK_BYTE_RATE_THRESHOLD_DEFAULT,
                        atLeast(1),
                        Importance.MEDIUM,
                        TASK_BYTE_RATE_THRESHOLD_DOC
                ).define(
                        TASK_MESSAGE_RATE_THRESHOLD_CONFIG,
                        Type.DOUBLE,
                        TASK_MESSAGE_RATE_THRESHOLD_DEFAULT,
                        atLeast(1),
                        Importance.MEDIUM,
                        TASK_MESSAGE_RATE_THRESHOLD_DOC
                ).define(
                        REFRESH_INTERVAL_MS,
                        Type.LONG,
                        REFRESH_INTERVAL_MS_DEFAULT,
                        atLeast(1),
                        Importance.MEDIUM,
                        REFRESH_INTERVAL_MS_DOC
                ).define(
                        CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_CONFIG,
                        Type.DOUBLE,
                        CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_DEFAULT,
                        atLeast(1),
                        Importance.LOW,
                        CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_DOC
                );
    }

    @Override
    protected String role() {
        return "connector-" + connectorName();
    }

    public void validate(Config configValidationResult) {
        parseTopicRenames(originalsStrings(), configValidationResult);

        if (!validateBootstrapServersOverrides(configValidationResult)) {
            // Don't try to validate more since we might end up contacting the wrong Kafka clusters
            return;
        }

        validateCluster(configValidationResult, true);
        validateCluster(configValidationResult, false);

        validateWorkloadPublishing(configValidationResult);
    }

    public Set<String> sourceTopics() {
        return sourceTopics;
    }

    public Set<String> targetTopics() {
        return targetTopics;
    }

    public String targetTopic(String sourceTopic) {
        return topicRenames.getOrDefault(sourceTopic, sourceTopic);
    }

    public int workerTaskLimit() {
        return getInt(WORKER_TASK_LIMIT_CONFIG);
    }

    public double taskByteRateThreshold() {
        return getDouble(TASK_BYTE_RATE_THRESHOLD_CONFIG);
    }

    public double taskMessageRateThreshold() {
        return getDouble(TASK_MESSAGE_RATE_THRESHOLD_CONFIG);
    }

    public long refreshIntervalMs() {
        return getLong(REFRESH_INTERVAL_MS);
    }

    public double clusterSizeReconfigurationThreshold() {
        return getDouble(CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_CONFIG);
    }

    public Map<String, String> taskConfig(int taskId) {
        Map<String, String> result = new HashMap<>(originalsStrings());

        // If we don't do this, every time a topic is added or removed, a task reconfiguration will be triggered
        result.remove(TOPICS_CONFIG);
        // Same for topic renames
        result.keySet().removeIf(key -> key.startsWith(TOPIC_RENAME_PREFIX));
        // Same for horizontal scaling properties that are only used by the connector and not its tasks
        result.remove(TASKS_MAX_CONFIG);
        result.remove(WORKER_TASK_LIMIT_CONFIG);
        result.remove(CLUSTER_SIZE_RECONFIGURATION_THRESHOLD_CONFIG);
        // And other properties that are also not used by tasks
        result.remove(REFRESH_INTERVAL_MS);
        result.remove(TASK_BYTE_RATE_THRESHOLD_CONFIG);
        result.remove(TASK_MESSAGE_RATE_THRESHOLD_CONFIG);

        result.put(TASK_ID_CONFIG, Integer.toString(taskId));

        return result;
    }

    private boolean validateBootstrapServersOverrides(Config configValidationResult) {
        AtomicBoolean result = new AtomicBoolean(true);
        Stream.of(SOURCE_CONSUMER_PREFIX, SOURCE_ADMIN_PREFIX, TARGET_ADMIN_PREFIX)
                .map(prefix -> prefix + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                .forEach(prop -> {
                    Object forbiddenOverride = originals().get(prop);
                    if (forbiddenOverride != null) {
                        String errorMessage = "cannot override bootstrap servers for individual clients; instead, use the "
                                + SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG + " and/or "
                                + TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG + " properties";
                        addError(configValidationResult, prop, forbiddenOverride, errorMessage);
                        result.set(false);
                    }
                });
        return result.get();
    }

    private void validateCluster(Config configValidationResult, boolean source) {
        String sourceTarget = source ? "source" : "target";
        String bootstrapOverride = source ? sourceBootstrapOverride() : targetBootstrapOverride();

        String prop;
        String value;
        if (bootstrapOverride != null) {
            prop = source ? SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG : TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG;
            value = bootstrapOverride;
        } else {
            String clusterName = source ? sourceCluster() : targetCluster();
            prop = source ? SOURCE_CLUSTER_CONFIG : TARGET_CLUSTER_CONFIG;
            try {
                resolveBootstrap(prop, clusterName);
            } catch (ConfigException e) {
                addError(configValidationResult, prop, clusterName, "failed to resolve bootstrap servers for " + sourceTarget + " cluster", e);
                return;
            }
            value = clusterName;
        }

        boolean clusterReachable;
        Set<String> topics = source ? sourceTopics() : targetTopics();
        try (Admin admin = source ? sourceAdmin() : targetAdmin()) {
            clusterReachable = true;
            validateTopics(configValidationResult, topics, source, admin);
        } catch (Exception e) {
            clusterReachable = false;
            log.error("Failed to create admin client for {} cluster", sourceTarget, e);
            addError(configValidationResult, prop, value, "failed to create admin client for " + sourceTarget + " cluster", e);
        }

        // Only bother validating the source consumer if the Kafka cluster can actually be reached
        if (clusterReachable && source) {
            validateSourceConsumer(configValidationResult);
        }
    }

    private void validateTopics(Config configValidationResult, Set<String> configuredTopics, boolean source, Admin admin) {
        String sourceTarget = source ? "source" : "target";

        Set<String> presentTopics;
        try {
            presentTopics = listTopics(admin, 1, TimeUnit.MINUTES);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.error("Failed to list topics on {} cluster", sourceTarget, e);

            String clusterProp = source ? SOURCE_CLUSTER_CONFIG : TARGET_CLUSTER_CONFIG;
            addError(configValidationResult, clusterProp, null, "failed to list topics on " + sourceTarget + " cluster", e);

            return;
        }

        Set<String> missingTopics = diff(configuredTopics, presentTopics);

        if (!missingTopics.isEmpty()) {
            if (source) {
                String errorMessage = "the following topics are missing on the source cluster: " + String.join(", ", missingTopics);
                addError(configValidationResult, TOPICS_CONFIG, null, errorMessage);
            } else {
                Map<String, Set<String>> reversedTopicRenames = topicRenames.entrySet().stream()
                        .collect(Collectors.groupingBy(
                                Map.Entry::getValue,
                                Collectors.mapping(Map.Entry::getKey, Collectors.toSet()
                        )));
                Set<String> missingNonRenamedTopics = new HashSet<>();
                for (String targetTopic : missingTopics) {
                    Set<String> renamedSourceTopics = reversedTopicRenames.get(targetTopic);
                    if (renamedSourceTopics != null) {
                        renamedSourceTopics.forEach(sourceTopic -> {
                            // If a missing target topic is specified via a topic rename, we should raise that to the user by
                            // attaching an error message to the topic.rename.<source_topic> property
                            addError(
                                    configValidationResult,
                                    TOPIC_RENAME_PREFIX + sourceTopic,
                                    targetTopic,
                                    "renamed topic is missing on the target cluster"
                            );
                        });
                    }

                    if (sourceTopics.contains(targetTopic) && !topicRenames.containsKey(targetTopic))
                        missingNonRenamedTopics.add(targetTopic);
                }
                if (!missingNonRenamedTopics.isEmpty()) {
                    String errorMessage = "the following topics are missing on the target cluster: " + String.join(", ", missingNonRenamedTopics);
                    addError(configValidationResult, TOPICS_CONFIG, null, errorMessage);
                }
            }
        }
    }

    private void validateSourceConsumer(Config configValidationResult) {
        try (Consumer<?, ?> consumer = sourceConsumer()) {
        } catch (Exception e) {
            // This error could be caused by any number of custom consumer properties and we don't really
            // have a way of knowing which one(s) caused the issue, so we just report the error with a detailed
            // message on the source cluster property and hope that there's enough info there for the user to
            // pinpoint the problem
            addError(configValidationResult, SOURCE_CLUSTER_CONFIG, sourceCluster(), "failed to create consumer for source cluster", e);
        }
    }

    private void validateWorkloadPublishing(Config configValidationResult) {
        if (workloadReportInterval() == null) {
            log.debug("Workload publishing is disabled; skipping validation");
            return;
        }

        try {
            KafkaBasedWorkloadProvider.assertStoreInitialized();
        } catch (IllegalStateException e) {
            log.debug("Kafka workload store does not appear to be initialized", e);
            String errorMessage = "This worker does not appear to be configured to use the Kafka-based workload provider, "
                    + "which means that uReplicator3's workload publishing feature must be disabled. Set the "
                    + WORKLOAD_REPORT_INTERVAL_MS_CONFIG + " property to zero or a negative value to disable, or reconfigure "
                    + "the workers in your Kafka Connect cluster to use the Kafka-based workload provider";

            addError(configValidationResult, WORKLOAD_REPORT_INTERVAL_MS_CONFIG, null, errorMessage);
        }
    }

    @VisibleForTesting
    static Map<String, String> parseTopicRenames(Map<String, String> originals, Config configValidationResult) {
        Map<String, String> result = new HashMap<>();
        originals.forEach((key, value) -> {
            if (!key.startsWith(TOPIC_RENAME_PREFIX))
                return;

            String sourceTopic = key.substring(TOPIC_RENAME_PREFIX.length());

            if (sourceTopic.isBlank()) {
                if (configValidationResult != null) {
                    addError(configValidationResult, key, value, "source topic is empty or blank");
                } else {
                    log.warn("Ignoring invalid property {} with value {}", key, value);
                }
                return;
            }

            if (value == null || value.isBlank()) {
                if (configValidationResult != null) {
                    addError(configValidationResult, key, value, "target topic is null, empty, or blank");
                } else {
                    log.warn("Ignoring property {} with invalid value {}", key, value);
                }
                return;
            }

            result.putIfAbsent(sourceTopic, value);
        });

        return result;
    }

    private static Set<String> computeTargetTopics(Set<String> sourceTopics, Map<String, String> topicRenames) {
        return sourceTopics.stream()
                .map(t -> topicRenames.getOrDefault(t, t))
                .collect(Collectors.toSet());
    }

    // Run with `./gradlew :connect:uber:genUReplicator3Config`
    public static void main(String[] args) {
        System.out.println(config().toHtml());
    }

}
