package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ASSIGNMENT_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_OFFSET_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_HEADER_SCHEMA;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ERROR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.VERSION_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.SESSIONED;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.UBER_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ALLOCATION_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ALLOCATION_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONFIG_STATE_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECTOR_ASSIGNMENT_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.REVOKED_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.SCHEDULED_DELAY_KEY_NAME;

public class UberConnectProtocol {

    public static final String UBER_CLUSTER_SIZE_KEY_NAME = "uberClusterSize";
    public static final short UBER_CONNECT_PROTOCOL_V1 = 1001;
    public static final boolean TOLERATE_MISSING_FIELDS_WITH_DEFAULTS = true;

    /**
     * Uber Connect Protocol Header V3:
     * <pre>
     *   Version            => Int16
     * </pre>
     * The Uber V3 protocol is identical to Connect V2, <em>except</em> that it also includes the total cluster size
     * in each member's assignment
     */
    private static final Struct UBER_CONNECT_PROTOCOL_HEADER_V1 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, UBER_CONNECT_PROTOCOL_V1);

    /**
     * Raw (non versioned) Uber assignment V1:
     * <pre>
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     *   Revoked            => [Connector Assignment]
     *   ScheduledDelay     => Int32
     *   UberClusterSize    => Int32
     * </pre>
     */
    public static final Schema UBER_ASSIGNMENT_V1 = new Schema(
            TOLERATE_MISSING_FIELDS_WITH_DEFAULTS,
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, ArrayOf.nullable(CONNECTOR_ASSIGNMENT_V1), null, true, null),
            new Field(REVOKED_KEY_NAME, ArrayOf.nullable(CONNECTOR_ASSIGNMENT_V1), null, true, null),
            new Field(SCHEDULED_DELAY_KEY_NAME, Type.INT32, null, 0),
            new Field(UBER_CLUSTER_SIZE_KEY_NAME, Type.INT32, null, 0));

    /**
     * The fields are serialized in sequence as follows:
     * Subscription V1:
     * <pre>
     *   Version            => Int16
     *   Url                => [String]
     *   ConfigOffset       => Int64
     *   Current Assignment => [Byte]
     * </pre>
     */
    public static ByteBuffer serializeMetadata(UberWorkerState workerState) {
        Struct configState = new Struct(CONFIG_STATE_V1)
                .set(URL_KEY_NAME, workerState.url())
                .set(CONFIG_OFFSET_KEY_NAME, workerState.offset());
        // Not a big issue if we embed the protocol version with the assignment in the metadata
        Struct allocation = new Struct(ALLOCATION_V1)
                .set(ALLOCATION_KEY_NAME, serializeAssignment(workerState.assignment()));
        Struct connectProtocolHeader = UBER_CONNECT_PROTOCOL_HEADER_V1;
        ByteBuffer buffer = ByteBuffer.allocate(connectProtocolHeader.sizeOf()
                + CONFIG_STATE_V1.sizeOf(configState)
                + ALLOCATION_V1.sizeOf(allocation));
        connectProtocolHeader.writeTo(buffer);
        CONFIG_STATE_V1.write(buffer, configState);
        ALLOCATION_V1.write(buffer, allocation);
        buffer.flip();
        return buffer;
    }

    /**
     * Returns the collection of Connect protocols that are supported by this version along
     * with their serialized metadata. The protocols are ordered by preference.
     *
     * @param workerState the current state of the worker metadata
     * @return the collection of Connect protocol metadata
     */
    public static JoinGroupRequestProtocolCollection metadataRequest(UberWorkerState workerState) {
        // Order matters in terms of protocol preference
        List<JoinGroupRequestProtocol> joinGroupRequestProtocols = new ArrayList<>();

        // Metadata for our custom protocol
        joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                .setName(UBER_V1.protocol())
                .setMetadata(serializeMetadata(workerState).array())
        );
        // Metadata for OSS Connect protocols
        joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                .setName(SESSIONED.protocol())
                .setMetadata(IncrementalCooperativeConnectProtocol.serializeMetadata(workerState, true).array())
        );
        joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                .setName(COMPATIBLE.protocol())
                .setMetadata(IncrementalCooperativeConnectProtocol.serializeMetadata(workerState, false).array())
        );
        joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                .setName(EAGER.protocol())
                .setMetadata(ConnectProtocol.serializeMetadata(workerState).array())
        );
        return new JoinGroupRequestProtocolCollection(joinGroupRequestProtocols.iterator());
    }

    /**
     * The fields are serialized in sequence as follows:
     * Complete Assignment V1:
     * <pre>
     *   Version            => Int16
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     *   Revoked            => [Connector Assignment]
     *   ScheduledDelay     => Int32
     *   UberClusterSize    => Int32
     * </pre>
     */
    public static ByteBuffer serializeAssignment(UberAssignmentV1 assignment) {
        // comparison depends on reference equality for now
        if (assignment == null || UberAssignmentV1.empty().equals(assignment)) {
            return null;
        }
        Struct struct = assignment.toUberStruct();
        Struct protocolHeader = UBER_CONNECT_PROTOCOL_HEADER_V1;
        ByteBuffer buffer = ByteBuffer.allocate(protocolHeader.sizeOf()
                + UBER_ASSIGNMENT_V1.sizeOf(struct));
        protocolHeader.writeTo(buffer);
        UBER_ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    /**
     * Given a byte buffer that contains an assignment as defined by this protocol, return the
     * deserialized form of the assignment.
     *
     * @param buffer the buffer containing a serialized assignment
     * @return the deserialized assignment
     * @throws SchemaException on incompatible Connect protocol version
     */
    public static UberAssignmentV1 deserializeAssignment(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = UBER_ASSIGNMENT_V1.read(buffer);
        return UberAssignmentV1.fromStruct(version, struct);
    }

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONNECT_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed
    }
}
