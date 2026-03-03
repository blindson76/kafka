package com.uber.data.kafka.connect.distributed;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.components.Versioned;

import java.util.Map;

public interface ClusterAssignor extends Versioned, Configurable, AutoCloseable {

    ClusterAssignment assign(ClusterConfigState clusterConfigState, Map<String, ConnectorsAndTasks> currentAssignments);

}
