/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.connect.storage.ClusterConfigState;

import com.uber.data.kafka.connect.distributed.ClusterAssignor;
import com.uber.data.kafka.connect.distributed.ConnectorTaskId;
import com.uber.data.kafka.connect.distributed.ConnectorsAndTasks;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;
import static org.apache.kafka.connect.util.ConnectUtils.combineCollections;
import static org.apache.kafka.connect.util.ConnectUtils.transformValues;

/**
 * An assignor that provides a pluggable interface layer for users to customize the logic for
 * assigning connectors and tasks to workers, possibly taking into account workload information
 */
public class UberAssignor implements ConnectAssignor {

    private final Logger log;
    private final ClusterAssignor assignor;

    public UberAssignor(
            LogContext logContext,
            ClusterAssignor assignor
    ) {
        this.log = logContext.logger(UberAssignor.class);
        this.assignor = assignor;
    }

    @Override
    public Map<String, ByteBuffer> performAssignment(String leaderId, ConnectProtocolCompatibility protocol,
                                                     List<JoinGroupResponseMember> allMemberMetadata,
                                                     WorkerCoordinator coordinator) {
        log.debug("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseMember member : allMemberMetadata) {
            memberConfigs.put(
                    member.memberId(),
                    IncrementalCooperativeConnectProtocol.deserializeMetadata(ByteBuffer.wrap(member.metadata())));
        }
        log.debug("Member configs: {}", memberConfigs);

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, coordinator.configSnapshot().offset());

        short protocolVersion = protocol.protocolVersion();

        ClusterConfigState leaderSnapshot = ensureLeaderConfig(maxOffset, coordinator);
        if (leaderSnapshot == null) {
            Map<String, ExtendedAssignment> assignments = fillAssignments(
                    memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    ClusterAssignment.EMPTY, protocolVersion);
            return serializeAssignments(assignments);
        }
        return performTaskAssignment(leaderId, leaderSnapshot, memberConfigs, coordinator, protocolVersion);
    }

    private ClusterConfigState ensureLeaderConfig(long maxOffset, WorkerCoordinator coordinator) {
        // If this leader is behind some other members, we can't do assignment
        if (coordinator.configSnapshot().offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = coordinator.configFreshSnapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. "
                         + "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                coordinator.configSnapshot(updatedSnapshot);
                return updatedSnapshot;
            }
        }
        return coordinator.configSnapshot();
    }

    protected Map<String, ByteBuffer> performTaskAssignment(String leaderId, ClusterConfigState configSnapshot,
                                                            Map<String, ExtendedWorkerState> memberConfigs,
                                                            WorkerCoordinator coordinator, short protocolVersion) {
        log.debug("Performing task assignment during generation: {} with memberId: {}",
                coordinator.generationId(), coordinator.memberId());
        Map<String, ConnectorsAndTasks> memberAssignments = transformValues(
                memberConfigs,
                memberConfig -> ConnectorsAndTasks.of(
                        memberConfig.assignment().connectors(),
                        memberConfig.assignment().tasks().stream()
                                .map(org.apache.kafka.connect.util.ConnectorTaskId::uberToPublicApi)
                                .toList()
                )
        );
        ClusterAssignment clusterAssignment = performTaskAssignment(
                configSnapshot,
                memberAssignments
        );

        coordinator.leaderState(new LeaderState(memberConfigs, clusterAssignment.allAssignedConnectors(), clusterAssignment.allAssignedTasks()));

        Map<String, ExtendedAssignment> assignments =
                fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId,
                        memberConfigs.get(leaderId).url(), configSnapshot.offset(),
                        clusterAssignment,
                        protocolVersion);

        log.debug("Actual assignments: {}", assignments);
        return serializeAssignments(assignments);
    }

    // Visible for testing
    ClusterAssignment performTaskAssignment(
            ClusterConfigState configSnapshot,
            Map<String, ConnectorsAndTasks> currentAssignments
    ) {
        log.debug("Current assignments: {}", currentAssignments);

        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = combineCollections(
                configuredConnectors,
                connector -> configSnapshot.tasks(connector).stream().map(org.apache.kafka.connect.util.ConnectorTaskId::uberToPublicApi).toList(),
                Collectors.toSet()
        );

        // The connectors and tasks that should be running on the cluster, based on the contents of the
        // config topic (which is the source of truth for the cluster's intended connectors and tasks)
        ConnectorsAndTasks configured = ConnectorsAndTasks.of(configuredConnectors, configuredTasks);
        log.debug("Configured assignments: {}", configured);

        com.uber.data.kafka.connect.distributed.ClusterAssignment nextClusterAssignment = assignor.assign(new ClusterConfigStateImpl(configSnapshot), currentAssignments);
        Map<String, ConnectorsAndTasks> nextAssignments = nextClusterAssignment.workerAssignments();
        log.debug("Next assignments: {}", nextAssignments);

        Map<String, Collection<String>> nextConnectorAssignments = transformValues(nextAssignments, ConnectorsAndTasks::connectors);
        Map<String, Collection<ConnectorTaskId>> nextTaskAssignments = transformValues(nextAssignments, ConnectorsAndTasks::tasks);

        Map<String, Collection<String>> currentConnectorAssignments = transformValues(currentAssignments, ConnectorsAndTasks::connectors);
        Map<String, Collection<ConnectorTaskId>> currentTaskAssignments = transformValues(currentAssignments, ConnectorsAndTasks::tasks);

        // Example:
        // Current: {C1, C2, C3}
        // Next: {C2, C3, C4}
        // Added: {C2, C3, C4} - {C1, C2, C3} = {C4}
        Map<String, Collection<String>> addedConnectors = diff(nextConnectorAssignments, currentConnectorAssignments);
        Map<String, Collection<ConnectorTaskId>> addedTasks = diff(nextTaskAssignments, currentTaskAssignments);
        log.debug("Incremental connector assignments: {}", addedConnectors);
        log.debug("Incremental task assignments: {}", addedTasks);

        // Example:
        // Current: {C1, C2, C3}
        // Next: {C2, C3, C4}
        // Revoked: {C1, C2, C3} - {C2, C3, C4} = {C1}
        Map<String, Collection<String>> revokedConnectors = diff(currentConnectorAssignments, nextConnectorAssignments);
        Map<String, Collection<ConnectorTaskId>> revokedTasks = diff(currentTaskAssignments, nextTaskAssignments);
        log.debug("Revoked connector assignments: {}", revokedConnectors);
        log.debug("Revoked task assignments: {}", revokedTasks);

        return new ClusterAssignment(
                addedConnectors,
                toInternal(addedTasks),
                revokedConnectors,
                toInternal(revokedTasks),
                nextConnectorAssignments,
                toInternal(nextTaskAssignments),
                nextClusterAssignment.scheduledRebalanceDelayMs()
        );
    }

    private Map<String, ExtendedAssignment> fillAssignments(Collection<String> members, short error,
                                                            String leaderId, String leaderUrl, long maxOffset,
                                                            ClusterAssignment clusterAssignment,
                                                            short protocolVersion) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectorsToStart = clusterAssignment.newlyAssignedConnectors(member);
            Collection<org.apache.kafka.connect.util.ConnectorTaskId> tasksToStart = clusterAssignment.newlyAssignedTasks(member);
            Collection<String> connectorsToStop = clusterAssignment.newlyRevokedConnectors(member);
            Collection<org.apache.kafka.connect.util.ConnectorTaskId> tasksToStop = clusterAssignment.newlyRevokedTasks(member);
            ExtendedAssignment assignment =
                    new ExtendedAssignment(protocolVersion, error, leaderId, leaderUrl, maxOffset,
                            connectorsToStart, tasksToStart, connectorsToStop, tasksToStop, clusterAssignment.uberScheduledRebalanceDelayMs());
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    /**
     * From a map of workers to assignment object generate the equivalent map of workers to byte
     * buffers of serialized assignments.
     *
     * @param assignments the map of worker assignments
     * @return the serialized map of assignments to workers
     */
    protected Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments) {
        int clusterSize = assignments.size();
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> {
                        UberAssignmentV1 uberAssignment = UberAssignmentV1.fromExtended(e.getValue(), clusterSize);
                        return UberConnectProtocol.serializeAssignment(uberAssignment);
                    }));
    }

    private static <T> Map<String, Collection<T>> diff(Map<String, Collection<T>> base,
                                                       Map<String, Collection<T>> toSubtract) {
        Map<String, Collection<T>> incremental = new HashMap<>();
        for (Map.Entry<String, Collection<T>> entry : base.entrySet()) {
            List<T> values = new ArrayList<>(entry.getValue());
            values.removeAll(toSubtract.getOrDefault(entry.getKey(), Set.of()));
            incremental.put(entry.getKey(), values);
        }
        return incremental;
    }

    private static Map<String, Collection<org.apache.kafka.connect.util.ConnectorTaskId>> toInternal(
            Map<String, Collection<ConnectorTaskId>> publicApi
    ) {
        return publicApi.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> toInternal(e.getValue())
                ));
    }

    private static Collection<org.apache.kafka.connect.util.ConnectorTaskId> toInternal(Collection<ConnectorTaskId> publicApi) {
        return publicApi.stream()
                .map(org.apache.kafka.connect.util.ConnectorTaskId::uberFromPublicApi)
                .toList();
    }

    static class ClusterAssignment extends IncrementalCooperativeAssignor.ClusterAssignment {

        public static final ClusterAssignment EMPTY = new ClusterAssignment(
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                0
        );

        private final int uberScheduledRebalanceDelayMs;

        public ClusterAssignment(
                Map<String, Collection<String>> newlyAssignedConnectors,
                Map<String, Collection<org.apache.kafka.connect.util.ConnectorTaskId>> newlyAssignedTasks,
                Map<String, Collection<String>> newlyRevokedConnectors,
                Map<String, Collection<org.apache.kafka.connect.util.ConnectorTaskId>> newlyRevokedTasks,
                Map<String, Collection<String>> allAssignedConnectors,
                Map<String, Collection<org.apache.kafka.connect.util.ConnectorTaskId>> allAssignedTasks,
                int uberScheduledRebalanceDelayMs
        ) {
            super(
                    newlyAssignedConnectors,
                    newlyAssignedTasks,
                    newlyRevokedConnectors,
                    newlyRevokedTasks,
                    allAssignedConnectors,
                    allAssignedTasks
            );
            this.uberScheduledRebalanceDelayMs = uberScheduledRebalanceDelayMs;
        }

        public int uberScheduledRebalanceDelayMs() {
            return uberScheduledRebalanceDelayMs;
        }

        @Override
        public String toString() {
            return "ClusterAssignment{"
                    + "newlyAssignedConnectors=" + newlyAssignedConnectors()
                    + ", newlyAssignedTasks=" + newlyAssignedTasks()
                    + ", newlyRevokedConnectors=" + newlyRevokedConnectors()
                    + ", newlyRevokedTasks=" + newlyRevokedTasks()
                    + ", allAssignedConnectors=" + allAssignedConnectors()
                    + ", allAssignedTasks=" + allAssignedTasks()
                    + ", allWorkers=" + allWorkers()
                    + ", uberScheduledRebalanceDelayMs=" + uberScheduledRebalanceDelayMs
                    + '}';
        }
    }

}
