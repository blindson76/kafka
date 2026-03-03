package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.distributed.assignment.AbstractAssignor;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignedPartition;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStore;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStores;
import com.uber.data.kafka.connect.utils.ExecutorUtils;
import com.uber.data.kafka.connect.utils.UberConnectUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.uber.data.kafka.connect.utils.UberConnectUtils.awaitAll;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.diff;

public class UReplicator3Connector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(UReplicator3Connector.class);

    public static final String SKIP_CONFIG_VALIDATION = "skip.config.validation";

    private UReplicator3ConnectorConfig config;
    private AbstractAssignor<TaskPartitions, TopicPartition> assignor;
    private ScheduledExecutorService executor;
    private AssignmentStore assignmentStore;

    // The values for these maps may be changed at any time, but their contents of any single map will never be mutated
    private volatile Map<String, Integer> sourcePartitionCounts;
    private volatile Map<String, Integer> targetPartitionCounts;
    private volatile int clusterSize;

    @Override
    public ConfigDef config() {
        return UReplicator3ConnectorConfig.config();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return UReplicator3Task.class;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting connector");

        config = new UReplicator3ConnectorConfig(props);

        assignmentStore = AssignmentStores.get();

        assignor = new TaskPartitionAssignor(
                config.sourceCluster(),
                config.targetCluster(),
                0.1, // This should never really be used so we hardcode it for now
                config.taskByteRateThreshold(),
                config.taskMessageRateThreshold()
        );

        // Synchronously collect partition counts the first time around
        // to make sure they're available before we generate task configs
        doRefreshClusterMetadata();

        long refreshIntervalMs = config.refreshIntervalMs();
        executor = ExecutorUtils.singleThreadedScheduled("executor-" + config.connectorName());
        executor.scheduleWithFixedDelay(this::refreshClusterMetadata, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
        // TODO: Should be two separate intervals for these operations
        executor.scheduleWithFixedDelay(this::refreshAssignments, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);

        log.info("Finished starting connector");
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Generating task configs");

        Map<String, Integer> sourcePartitionCountsSnapshot = sourcePartitionCounts;
        Map<String, Integer> targetPartitionCountsSnapshot = targetPartitionCounts;

        List<TopicPartition> topicPartitions = replicatedPartitions(sourcePartitionCountsSnapshot, targetPartitionCountsSnapshot);

        clusterSize = context.uberClusterSize();
        int numTasks = numTasks(maxTasks, topicPartitions.size(), clusterSize);

        List<Map<String, String>> result = IntStream.range(0, numTasks)
                .mapToObj(i -> config.taskConfig(i))
                .collect(Collectors.toList());

        // Make sure partition assignments are updated in line with the expected number of tasks
        // Run on a separate thread since we really, really don't want to block too long in this method
        // (it's invoked from the Connect worker's herder thread, which is basically the central nervous
        // system of the control plane)
        executor.execute(() -> refreshAssignments(numTasks));

        log.info("Finished generating task configs");

        return result;
    }

    @Override
    public void stop() {
        log.info("Stopping connector");
        ExecutorUtils.shutdown(executor, "connector executor", 2, TimeUnit.SECONDS);
        log.info("Finished stopping connector");
    }

    @Override
    public Config validate(Map<String, String> props) {
        // Breakglass logic: allow the connector config to be updated even if it's failing
        // Should only be used in emergencies
        String skipValidationValue = props.get(SKIP_CONFIG_VALIDATION);
        if (Boolean.parseBoolean(skipValidationValue)) {
            log.warn("Skipping config validation since {} is set to {}", SKIP_CONFIG_VALIDATION, skipValidationValue);
            return new Config(List.of());
        }

        Config result = super.validate(props);
        UReplicator3ConnectorConfig config;

        try {
            config = new UReplicator3ConnectorConfig(props);
        } catch (Exception e) {
            boolean alreadyHasErrors = result.configValues().stream()
                    .anyMatch(configValue -> !configValue.errorMessages().isEmpty());

            if (alreadyHasErrors) {
                // We can just log the error; preflight validation will fail without any intervention on our part
                log.debug("Unable to fully validate connector config", e);
                return result;
            } else {
                // This shouldn't really happen, make sure to log at error level if it does so we can investigate
                log.error("Unable to fully validate connector config", e);
                // Force preflight validation to fail
                throw new ConnectException("Unable to fully validate connector config", e);
            }
        }

        config.validate(result);

        return result;
    }

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
        return true;
    }

    private void refreshClusterMetadata() {
        log.info("Refreshing cluster metadata");
        try {
            doRefreshClusterMetadata();
            log.info("Finished refreshing cluster metadata");
        } catch (Exception e) {
            // TODO: Emit metric
            log.error("Failed to refresh cluster metadata", e);
        }
    }

    private void doRefreshClusterMetadata() {
        Map<String, Integer> oldSourcePartitionCounts = sourcePartitionCounts;
        try (Admin sourceAdmin = config.sourceAdmin()) {
            sourcePartitionCounts = partitionCounts(sourceAdmin, config.sourceTopics(), "source");
        }

        Map<String, Integer> oldTargetPartitionCounts = targetPartitionCounts;
        try (Admin targetAdmin = config.targetAdmin()) {
            targetPartitionCounts = partitionCounts(targetAdmin, config.targetTopics(), "target");
        }

        if (oldSourcePartitionCounts != null && !oldSourcePartitionCounts.equals(sourcePartitionCounts)) {
            context.requestTaskReconfiguration();
        }
        if (oldTargetPartitionCounts != null && !oldTargetPartitionCounts.equals(targetPartitionCounts)) {
            context.requestTaskReconfiguration();
        }

        int currentClusterSize = context.uberClusterSize();
        if (currentClusterSize > 0 && clusterSize > 0) {
            double clusterSizeRatio = 100.0 * Math.abs(clusterSize - currentClusterSize) / clusterSize;
            log.trace("Current cluster size: {}; last-used cluster size: {}", currentClusterSize, clusterSize);
            double reconfigurationThreshold = config.clusterSizeReconfigurationThreshold();
            if (clusterSizeRatio >= reconfigurationThreshold) {
                log.debug(
                    "Ratio of current cluster size to last-used cluster size exceeds reconfiguration threshold ({}); generating new task configs",
                    reconfigurationThreshold
                );
                context.requestTaskReconfiguration();
            } else if (currentClusterSize == clusterSize) {
                log.trace("No change in cluster size");
            } else {
                log.trace("Ratio of current cluster size to last-used cluster size is within reconfiguration threshold; will not generate new task configs");
            }
        }
    }

    private static Map<String, Integer> partitionCounts(Admin admin, Set<String> topics, String clusterType) {
        try {
            return UberConnectUtils.listPartitions(topics, admin, 1, TimeUnit.MINUTES);
        } catch (Exception  e) {
            throw new ConnectException("Failed to list partition counts for topics on " + clusterType + " cluster", e);
        }
    }

    private List<TopicPartition> replicatedPartitions(
            Map<String, Integer> sourcePartitionCountsSnapshot,
            Map<String, Integer> targetPartitionCountsSnapshot
    ) {
        Comparator<Map.Entry<String, Integer>> topicPartitionCountEntryComparator =
                Map.Entry.<String, Integer>comparingByValue()
                        .reversed()
                        .thenComparing(
                                Map.Entry.comparingByKey()
                        );

        return sourcePartitionCountsSnapshot.entrySet().stream()
                .filter(e -> {
                    String sourceTopic = e.getKey();
                    String targetTopic = config.targetTopic(sourceTopic);

                    if (targetPartitionCountsSnapshot.containsKey(targetTopic)) {
                        return true;
                    } else {
                        log.warn(
                                "Not assigning topic {} to tasks since its target partition count is unknown "
                                        + "(topic may have been deleted)",
                                sourceTopic);
                        return false;
                    }
                })
                .sorted(topicPartitionCountEntryComparator)
                .flatMap(e -> IntStream.range(0, e.getValue())
                        .mapToObj(partition -> new TopicPartition(e.getKey(), partition))
                )
                .toList();
    }

    private int numTasks(int maxTasks, int numTopicPartitions, int clusterSize) {
        int numTasks = Math.min(maxTasks, numTopicPartitions);
        if (clusterSize > 0) {
            int workerTaskThreshold = config.workerTaskLimit();
            if (workerTaskThreshold > 0) {
                int maxWorkerTasks = workerTaskThreshold * clusterSize;
                log.debug("Total cluster-based task limit: {} (cluster size: {}, tasks per worker: {})",
                        maxWorkerTasks, clusterSize, workerTaskThreshold);
                numTasks = Math.min(maxWorkerTasks, numTasks);
            } else {
                log.debug("Ignoring cluster size since worker task limit is disabled");
            }
        } else {
            log.warn("Cluster size not available; will generate at most {} tasks", numTasks);
        }
        return numTasks;
    }

    private void refreshAssignments() {
        refreshAssignments(context.uberTaskConfigs().size());
    }

    private void refreshAssignments(int numTasks) {
        log.info("Refreshing task partition assignments");
        try {
            doRefreshAssignments(numTasks);
            log.info("Finished refreshing task partition assignments");
        } catch (Exception e) {
            // TODO: Emit metric
            log.error("Failed to refresh task partition assignments", e);
        }
    }

    private void doRefreshAssignments(int numTasks) {
        Map<String, Integer> sourcePartitionCountsSnapshot = sourcePartitionCounts;
        Map<String, Integer> targetPartitionCountsSnapshot = targetPartitionCounts;

        List<TopicPartition> topicPartitions = replicatedPartitions(sourcePartitionCountsSnapshot, targetPartitionCountsSnapshot);

        Map<Integer, ? extends Collection<AssignedPartition>> existingStoredAssignments = assignmentStore.getAssignments(
                config.connectorName(),
                config.sourceCluster(),
                config.targetCluster()
        );

        List<TaskPartitions> existingAssignments = existingAssignments(existingStoredAssignments);
        Collection<TopicPartition> unassignedPartitions = unassignedPartitions(numTasks, existingAssignments, topicPartitions);

        List<TaskPartitions> newAssignments = IntStream.range(0, numTasks)
                .mapToObj(i -> i < existingAssignments.size()
                        ? existingAssignments.get(i).copy()
                        : new TaskPartitions(config.connectorName(), i, new HashSet<>())
                ).toList();
        assignor.assign(newAssignments, unassignedPartitions);

        Map<Integer, Collection<AssignedPartition>> newStoredAssignments = newStoredAssignments(
                targetPartitionCountsSnapshot,
                newAssignments
        );

        Collection<Future<?>> storeFutures = new ArrayList<>();

        Set<TopicPartition> toExplicitlyRevoke = existingAssignments.stream()
                .map(TaskPartitions::partitions)
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(HashSet::new));
        toExplicitlyRevoke.removeAll(new HashSet<>(topicPartitions));

        if (!toExplicitlyRevoke.isEmpty()) {
            log.debug("Found {} partitions to explicitly revoke", toExplicitlyRevoke.size());
        } else {
            log.trace("No partitions to explicitly revoke");
        }

        toExplicitlyRevoke.stream()
                .map(this::replicatedTopicPartition)
                .map(rtp -> assignmentStore.revokePartition(config.connectorName(), rtp))
                .forEach(storeFutures::add);

        int tasksWithNewPartitions = 0;
        for (int i = 0; i < newAssignments.size(); i++) {
            final int taskId = i;
            Collection<AssignedPartition> existingPartitions = existingStoredAssignments.get(taskId);
            Collection<AssignedPartition> newPartitions = newStoredAssignments.get(taskId);

            Set<AssignedPartition> added = newPartitions != null ? diff(newPartitions, existingPartitions) : Set.of();

            if (!added.isEmpty()) {
                log.debug("Found {} partitions to add for task {}-{}", added.size(), config.connectorName(), taskId);
                tasksWithNewPartitions++;
            } else {
                log.trace("No partitions to add for task {}-{}", config.connectorName(), taskId);
            }

            added.stream()
                    .map(ap -> {
                        ReplicatedTopicPartition rtp = replicatedTopicPartition(ap.source());
                        return assignmentStore.assignPartition(config.connectorName(), rtp, taskId, ap.dstTopic(), ap.dstPartition());
                    })
                    .forEach(storeFutures::add);
        }

        if (tasksWithNewPartitions > 0) {
            log.debug("Assigned new partitions to {} total tasks", tasksWithNewPartitions);
        } else {
            log.debug("Assigned no new partitions to any tasks");
        }

        // We may want to make this duration configurable in the future
        // TODO: Should also add metrics on write failures
        try {
            awaitAll(storeFutures, Duration.ofMinutes(1));
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.error("Failed to publish task partition assignment updates", e);
        }
    }

    private List<TaskPartitions> existingAssignments(Map<Integer, ? extends Collection<AssignedPartition>> storedAssignments) {
        if (storedAssignments.isEmpty())
            return List.of();

        int highestTask = Collections.max(storedAssignments.keySet());
        return IntStream.rangeClosed(0, highestTask) // rangeClosed because tasks are zero-indexed
                .mapToObj(i -> {
                    Collection<AssignedPartition> partitions = storedAssignments.get(i);
                    Set<TopicPartition> partitionsSet = new HashSet<>();
                    if (partitions != null) {
                        partitions.stream()
                                .map(AssignedPartition::source)
                                .forEach(partitionsSet::add);
                    }
                    return new TaskPartitions(config.connectorName(), i, partitionsSet);
                }).toList();
    }

    private Set<TopicPartition> unassignedPartitions(int numTasks, List<TaskPartitions> taskPartitions, Collection<TopicPartition> topicPartitions) {
        Set<TopicPartition> result = new LinkedHashSet<>(topicPartitions);

        for (int i = 0; i < numTasks && i < taskPartitions.size(); i++) {
            result.removeAll(taskPartitions.get(i).partitions());
        }

        return result;
    }

    private ReplicatedTopicPartition replicatedTopicPartition(TopicPartition tp) {
        return new ReplicatedTopicPartition(config.sourceCluster(), config.targetCluster(), tp);
    }

    private Map<Integer, Collection<AssignedPartition>> newStoredAssignments(
            Map<String, Integer> targetPartitionCounts,
            List<TaskPartitions> newAssignments
    ) {
        Map<Integer, Collection<AssignedPartition>> result = new HashMap<>();
        for (int i = 0; i < newAssignments.size(); i++) {
            TaskPartitions newAssignment = newAssignments.get(i);
            Collection<AssignedPartition> assignment = new HashSet<>();
            for (TopicPartition source : newAssignment.partitions()) {
                String dstTopic = config.targetTopic(source.topic());
                Integer targetPartitionCount = targetPartitionCounts.get(dstTopic);
                if (targetPartitionCount == null) {
                    log.warn("Could not find partition count for topic {} on target cluster; will not replicate topic partition {} (this should never happen)",
                            dstTopic, source);
                    continue;
                }
                int dstPartition = source.partition() % targetPartitionCount;
                AssignedPartition assignedPartition = new AssignedPartition(
                        source.topic(),
                        source.partition(),
                        !Objects.equals(dstTopic, source.topic()) ? dstTopic : null,
                        dstPartition != source.partition() ? dstPartition : null
                );
                assignment.add(assignedPartition);
            }
            result.put(i, assignment);
        }
        return result;
    }

}
