package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public record TaskPartitions(String connectorName, int taskId, Set<TopicPartition> partitions) {

    @Override
    public String toString() {
        return connectorName + "-" + taskId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorName, taskId);
    }

    public TaskPartitions copy() {
        return new TaskPartitions(connectorName, taskId, new HashSet<>(partitions));
    }

}
