package com.uber.data.kafka.connect.rest.entities;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignedPartition;

import java.util.Collection;
import java.util.List;

import static com.uber.data.kafka.connect.utils.UberConnectUtils.compareLexicographically;

public record TaskAssignmentEntity(
    @JsonProperty String connector,
    @JsonProperty int task,
    @JsonProperty List<AssignedPartitionEntity> partitions
) {

  public static TaskAssignmentEntity of(String connector, int task, Collection<AssignedPartition> partitions) {
    List<AssignedPartitionEntity> sortedPartitions = partitions.stream()
        .sorted((ap1, ap2) -> compareLexicographically(ap1.source(), ap2.source()))
        .map(AssignedPartitionEntity::of)
        .toList();

    return new TaskAssignmentEntity(connector, task, sortedPartitions);
  }
}
