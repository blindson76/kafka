package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;

public record ReplicatedTopicPartitionEntity(
        @JsonProperty("src_kafka") String sourceKafka,
        @JsonProperty("dst_kafka") String targetKafka,
        @JsonProperty("topic") String topic,
        @JsonProperty("partition") int partition
) {

    public static ReplicatedTopicPartitionEntity of(ReplicatedTopicPartition replicatedTopicPartition) {
        return new ReplicatedTopicPartitionEntity(
                replicatedTopicPartition.srcKafka(),
                replicatedTopicPartition.dstKafka(),
                replicatedTopicPartition.topic(),
                replicatedTopicPartition.partition()
        );
    }

}
