package com.uber.data.kafka.connect.distributed.assignment;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.runtime.distributed.UberWorkerLoad;
import org.apache.kafka.connect.util.ConnectUtils;

import com.uber.data.kafka.connect.distributed.ClusterAssignment;
import com.uber.data.kafka.connect.distributed.ClusterAssignor;
import com.uber.data.kafka.connect.distributed.ClusterConfigState;
import com.uber.data.kafka.connect.distributed.ConnectorTaskId;
import com.uber.data.kafka.connect.distributed.ConnectorsAndTasks;
import com.uber.data.kafka.connect.workload.common.WorkloadProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class UberClusterAssignor implements ClusterAssignor {

    private static final Logger log = LoggerFactory.getLogger(UberClusterAssignor.class);

    private WorkloadProvider workloadProvider;
    private double workerLoadThreshold;
    private double defaultTaskLoad;
    private int periodicRebalanceIntervalMs;
    private int workerTaskLimit;

    @Override
    public void configure(Map<String, ?> props) {
        UberClusterAssignorConfig config = new UberClusterAssignorConfig(props);
        this.workloadProvider = config.workloadProvider();
        this.workerLoadThreshold = config.uberWorkerLoadThreshold();
        this.defaultTaskLoad = config.uberDefaultTaskLoad();
        this.periodicRebalanceIntervalMs = config.uberPeriodicRebalanceIntervalMs();
        this.workerTaskLimit = 10; // TODO: Don't even bother making this configurable, just put it into Flipr with a sane default
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void close() throws Exception {
        closeQuietly(workloadProvider, "Workload provider for Uber cluster assignor");
    }

    @Override
    public ClusterAssignment assign(ClusterConfigState clusterConfigState, Map<String, ConnectorsAndTasks> currentAssignment) {
        Set<String> configuredConnectors = new TreeSet<>(clusterConfigState.connectors());
        Set<ConnectorTaskId> configuredTasks = combineCollections(configuredConnectors, clusterConfigState::tasks, Collectors.toSet());

        // The connectors and tasks that should be running on the cluster, based on the contents of the
        // config topic (which is the source of truth for the cluster's intended connectors and tasks)
        ConnectorsAndTasks configured = ConnectorsAndTasks.of(configuredConnectors, configuredTasks);

        // The connectors and tasks currently running on each member that's participating in this rebalance
        ConnectorsAndTasks activeAssignments = assignment(currentAssignment);
        log.debug("Active assignments: {}", activeAssignments);

        // The complete set of connectors and tasks that should be newly-assigned during this round,
        // either because they were just created, or the worker they were previously assigned to
        // is no longer running
        ConnectorsAndTasks toAssign = diff(configured, activeAssignments);
        log.debug("Unassigned: {}", toAssign);

        // The set of deleted connectors and tasks that members of the cluster are currently running
        // and which should now be explicitly revoked
        ConnectorsAndTasks deleted = diff(activeAssignments, configured);
        log.debug("Deleted assignments: {}", deleted);

        // The connectors and tasks that are currently running on more than one worker each
        ConnectorsAndTasks duplicated = duplicatedAssignments(currentAssignment);
        log.debug("Duplicated assignments: {}", duplicated);

        // The connectors and tasks that need to be completely revoked from all workers
        // (A follow-up rebalance is automatically triggered whenever a revocation takes place,
        // during which the duplicated connectors and tasks revoked in this round can be safely
        // reassigned)
        ConnectorsAndTasks toRemove = deleted.toBuilder()
                .add(duplicated)
                .build();

        Collection<UberWorkerLoad> workerLoads = workerLoads(currentAssignment);

        revokeAll(workerLoads, toRemove);

        int connectorsPerWorker = (int) Math.ceil(configured.connectors().size() / (double) workerLoads.size());
        AbstractAssignor<UberWorkerLoad, String> connectorAssignor = new WorkerConnectorAssignor(connectorsPerWorker);
        connectorAssignor.assign(workerLoads, toAssign.connectors());

        AbstractAssignor<UberWorkerLoad, ConnectorTaskId> taskAssignor =
                new WorkerTaskAssignor(workloadProvider, clusterConfigState, workerLoadThreshold, defaultTaskLoad, workerTaskLimit);
        taskAssignor.assign(workerLoads, toAssign.tasks());

        Map<String, ConnectorsAndTasks.Builder> resultBuilder = new HashMap<>();
        workerLoads.forEach(worker ->
                resultBuilder.computeIfAbsent(worker.worker(), w -> ConnectorsAndTasks.builder())
                        .addConnectors(worker.connectors())
                        .addTasks(worker.tasks())
        );

        Map<String, ConnectorsAndTasks> workerAssignments = transformValues(resultBuilder, ConnectorsAndTasks.Builder::build);
        return new ClusterAssignment(workerAssignments, periodicRebalanceIntervalMs);
    }

    private ConnectorsAndTasks assignment(Map<String, ConnectorsAndTasks> memberAssignments) {
        log.debug("Received assignments: {}", memberAssignments);
        return ConnectorsAndTasks.of(
                ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::connectors),
                ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::tasks)
        );
    }

    private ConnectorsAndTasks duplicatedAssignments(Map<String, ConnectorsAndTasks> memberAssignments) {
        Set<String> duplicatedConnectors = duplicatedElements(memberAssignments, ConnectorsAndTasks::connectors);
        Set<ConnectorTaskId> duplicatedTasks = duplicatedElements(memberAssignments, ConnectorsAndTasks::tasks);

        return ConnectorsAndTasks.of(duplicatedConnectors, duplicatedTasks);
    }

    private static <E> Set<E> duplicatedElements(
            Map<String, ConnectorsAndTasks> memberAssignments,
            Function<ConnectorsAndTasks, Collection<E>> assignmentExtractor
    ) {
        Map<E, Long> elementCounts = combineCollections(
                memberAssignments.values(),
                assignmentExtractor,
                Collectors.groupingBy(Function.identity(), Collectors.counting())
        );
        return elementCounts
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1L)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Revoke all of the specified connectors and tasks from all workers
     * @param workerLoads the workers; may be empty, but not null
     * @param toRevoke the connectors and tasks to revoke; may be empty, but not null
     */
    private void revokeAll(Collection<UberWorkerLoad> workerLoads, ConnectorsAndTasks toRevoke) {
        workerLoads.forEach(w -> {
            w.connectors().removeAll(toRevoke.connectors());
            w.tasks().removeAll(toRevoke.tasks());
        });
    }

    private static List<UberWorkerLoad> workerLoads(Map<String, ConnectorsAndTasks> memberAssignments) {
        return memberAssignments.entrySet().stream()
                .map(e -> new UberWorkerLoad(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private static <K, I, O> Map<K, O> transformValues(Map<K, I> map, Function<I, O> transformation) {
        return map.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                transformation.compose(Map.Entry::getValue)
        ));
    }

    private static ConnectorsAndTasks diff(ConnectorsAndTasks base, ConnectorsAndTasks... toSubtract) {
        Set<String> connectors = new TreeSet<>(base.connectors());
        Set<ConnectorTaskId> tasks = new TreeSet<>(base.tasks());
        for (ConnectorsAndTasks sub : toSubtract) {
            connectors.removeAll(sub.connectors());
            tasks.removeAll(sub.tasks());
        }
        return new ConnectorsAndTasks(connectors, tasks);
    }

    private static <I, T, C> C combineCollections(
            Collection<I> collection,
            Function<I, Collection<T>> extractCollection,
            Collector<T, ?, C> collector
    ) {
        return collection.stream()
                .map(extractCollection)
                .flatMap(Collection::stream)
                .collect(collector);
    }
}
