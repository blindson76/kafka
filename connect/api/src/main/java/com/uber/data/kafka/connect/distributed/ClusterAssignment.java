package com.uber.data.kafka.connect.distributed;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record ClusterAssignment(Map<String, ConnectorsAndTasks> workerAssignments, int scheduledRebalanceDelayMs) {

    public ClusterAssignment(Map<String, ConnectorsAndTasks> workerAssignments, int scheduledRebalanceDelayMs) {
        this.workerAssignments = Collections.unmodifiableMap(new LinkedHashMap<>(workerAssignments));
        this.scheduledRebalanceDelayMs = scheduledRebalanceDelayMs;
    }

}
