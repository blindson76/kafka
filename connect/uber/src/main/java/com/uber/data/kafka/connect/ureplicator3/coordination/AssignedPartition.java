package com.uber.data.kafka.connect.ureplicator3.coordination;

import org.apache.kafka.common.TopicPartition;

public record AssignedPartition(
    String srcTopic,
    int srcPartition,
    String dstTopic,
    Integer dstPartition
) {

  public TopicPartition source() {
    return new TopicPartition(srcTopic, srcPartition);
  }

  public TopicPartition target() {
    return new TopicPartition(
            dstTopic != null ? dstTopic : srcTopic,
            dstPartition != null ? dstPartition : srcPartition
    );
  }

  public boolean hasOverride() {
    return !source().equals(target());
  }

}
