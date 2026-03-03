package com.uber.data.kafka.connect.common;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * POJO containing a topic partition and a source and destination Kafka cluster
 */
public class ReplicatedTopicPartition {

    private final String srcKafka;
    private final String dstKafka;
    private final String topic;
    private final int partition;

    public ReplicatedTopicPartition(String srcKafka, String dstKafka, String topic, int partition) {
        this.srcKafka = srcKafka;
        this.dstKafka = dstKafka;
        this.topic = topic;
        this.partition = partition;
    }

    public ReplicatedTopicPartition(String srcKafka, String dstKafka, TopicPartition topicPartition) {
        this(srcKafka, dstKafka, topicPartition.topic(), topicPartition.partition());
    }

    public String srcKafka() {
        return srcKafka;
    }

    public String dstKafka() {
        return dstKafka;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    public boolean matchesPipeline(String srcKafka, String dstKafka) {
        return Objects.equals(srcKafka, this.srcKafka)
            && Objects.equals(dstKafka, this.dstKafka);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReplicatedTopicPartition that = (ReplicatedTopicPartition) o;
        return partition == that.partition
                && Objects.equals(srcKafka, that.srcKafka)
                && Objects.equals(dstKafka, that.dstKafka)
                && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcKafka, dstKafka, topic, partition);
    }

    @Override
    public String toString() {
        return "@" + srcKafka + "@" + dstKafka + "@" + topicPartition();
    }
}
