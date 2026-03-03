package com.uber.data.kafka.connect.workload.tracking;

import org.apache.kafka.common.TopicPartition;

import com.uber.data.kafka.connect.workload.common.Workload;

@FunctionalInterface
public interface WorkloadStore {

    void store(String srcKafka, String dstKafka, TopicPartition topicPartition, Workload workload);

}
