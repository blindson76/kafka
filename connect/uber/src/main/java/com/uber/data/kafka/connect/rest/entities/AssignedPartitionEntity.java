package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignedPartition;

@JsonInclude(Include.NON_NULL)
public record AssignedPartitionEntity(
    @JsonProperty String topic,
    @JsonProperty int partition,
    @JsonProperty String dstTopic,
    @JsonProperty Integer dstPartition
) {

  public static AssignedPartitionEntity of(AssignedPartition assignedPartition) {
    return new AssignedPartitionEntity(
        assignedPartition.srcTopic(),
        assignedPartition.srcPartition(),
        assignedPartition.dstTopic(),
        assignedPartition.dstPartition()
    );
  }

  public AssignedPartition assignedPartition() {
    return new AssignedPartition(topic, partition, dstTopic, dstPartition);
  }

}
