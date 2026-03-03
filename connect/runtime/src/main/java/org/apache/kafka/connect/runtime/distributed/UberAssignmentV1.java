package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ASSIGNMENT_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_OFFSET_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ERROR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.REVOKED_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.SCHEDULED_DELAY_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.UberConnectProtocol.UBER_ASSIGNMENT_V1;
import static org.apache.kafka.connect.runtime.distributed.UberConnectProtocol.UBER_CLUSTER_SIZE_KEY_NAME;

public class UberAssignmentV1 extends ExtendedAssignment {

    private static final UberAssignmentV1 EMPTY = fromExtended(ExtendedAssignment.empty(), 0);

    private final int clusterSize;

    public UberAssignmentV1(
            short version,
            short error,
            String leader,
            String leaderUrl,
            long configOffset,
            Collection<String> connectorIds,
            Collection<ConnectorTaskId> taskIds,
            Collection<String> revokedConnectorIds,
            Collection<ConnectorTaskId> revokedTaskIds,
            int delay,
            int clusterSize) {
        super(version, error, leader, leaderUrl, configOffset, connectorIds, taskIds, revokedConnectorIds, revokedTaskIds, delay);
        this.clusterSize = clusterSize;
    }

    public int clusterSize() {
        return clusterSize;
    }

    /**
     * Return the {@code Struct} that corresponds to this assignment.
     *
     * @return the assignment struct
     */
    public Struct toUberStruct() {
        Struct superStruct = super.toStruct();
        return new Struct(UBER_ASSIGNMENT_V1)
                .set(ERROR_KEY_NAME, error())
                .set(LEADER_KEY_NAME, leader())
                .set(LEADER_URL_KEY_NAME, leaderUrl())
                .set(CONFIG_OFFSET_KEY_NAME, offset())
                .set(ASSIGNMENT_KEY_NAME, superStruct.get(ASSIGNMENT_KEY_NAME))
                .set(REVOKED_KEY_NAME, superStruct.get(REVOKED_KEY_NAME))
                .set(SCHEDULED_DELAY_KEY_NAME, superStruct.get(SCHEDULED_DELAY_KEY_NAME))
                .set(UBER_CLUSTER_SIZE_KEY_NAME, clusterSize);
    }

    // We don't override toStruct; it's only used when serializing assignments to send to the leader when
    // joining a group during rebalance, and the cluster size field isn't used there

    public static UberAssignmentV1 fromExtended(ExtendedAssignment baseAssignment, int clusterSize) {
        return new UberAssignmentV1(
                baseAssignment.version(),
                baseAssignment.error(),
                baseAssignment.leader(),
                baseAssignment.leaderUrl(),
                baseAssignment.offset(),
                baseAssignment.connectors(),
                baseAssignment.tasks(),
                baseAssignment.revokedConnectors(),
                baseAssignment.revokedTasks(),
                baseAssignment.delay(),
                clusterSize
        );
    }

    /**
     * Given a {@code Struct} that encodes an assignment return the assignment object.
     *
     * @param struct a struct representing an assignment
     * @return the assignment
     */
    public static UberAssignmentV1 fromStruct(short version, Struct struct) {
        ExtendedAssignment extendedAssignment = ExtendedAssignment.fromStruct(version, struct);
        Integer clusterSize = struct.getInt(UBER_CLUSTER_SIZE_KEY_NAME);
        if (clusterSize == null) {
            clusterSize = 0;
        }
        return fromExtended(extendedAssignment, clusterSize);
    }

    public static UberAssignmentV1 empty() {
        return EMPTY;
    }
}
