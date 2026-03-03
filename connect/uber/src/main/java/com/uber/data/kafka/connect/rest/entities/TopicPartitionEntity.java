package com.uber.data.kafka.connect.rest.entities;

import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TopicPartitionEntity(
    @JsonProperty String topic,
    @JsonProperty int partition
) {

  public static TopicPartitionEntity of(TopicPartition topicPartition) {
    return new TopicPartitionEntity(topicPartition.topic(), topicPartition.partition());
  }

  public TopicPartition topicPartition() {
    return new TopicPartition(topic, partition);
  }

}
