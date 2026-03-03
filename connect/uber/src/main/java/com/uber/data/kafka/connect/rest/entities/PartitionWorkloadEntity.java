package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.workload.common.Workload;

import java.util.Comparator;

// TODO: Property name overrides aren't being used in REST responses for some reason
public record PartitionWorkloadEntity(
        @JsonProperty("replicated_partition") ReplicatedTopicPartitionEntity replicatedPartition,
        @JsonProperty("bytes_per_second") double bytesPerSecond,
        @JsonProperty("messages_per_second") double messagesPerSecond
) implements Comparable<PartitionWorkloadEntity> {

    public static PartitionWorkloadEntity of(ReplicatedTopicPartition replicatedPartition, Workload workload) {
        return new PartitionWorkloadEntity(
                ReplicatedTopicPartitionEntity.of(replicatedPartition),
                workload.getBytesPerSecond(),
                workload.getMessagesPerSecond()
        );
    }

    public static Comparator<PartitionWorkloadEntity> lexicographicComparator() {
        return Comparator.<PartitionWorkloadEntity, String>comparing(w -> w.replicatedPartition().sourceKafka())
                .thenComparing(w -> w.replicatedPartition().targetKafka())
                .thenComparing(w -> w.replicatedPartition().topic())
                .thenComparing(w -> w.replicatedPartition().partition())
                .thenComparing(Comparator.naturalOrder());
    }

    public Workload workload() {
        return new Workload(bytesPerSecond, messagesPerSecond);
    }

    public ReplicatedTopicPartition replicatedTopicPartition() {
        return new ReplicatedTopicPartition(
                replicatedPartition.sourceKafka(),
                replicatedPartition.targetKafka(),
                replicatedPartition.topic(),
                replicatedPartition.partition()
        );
    }

    @Override
    public int compareTo(PartitionWorkloadEntity that) {
        if (that == null)
            return 1;

        if (this == that)
            return 0;

        int result = Double.compare(this.bytesPerSecond, that.bytesPerSecond);
        if (result != 0)
            return result;

        return Double.compare(this.messagesPerSecond, that.messagesPerSecond);
    }
}
