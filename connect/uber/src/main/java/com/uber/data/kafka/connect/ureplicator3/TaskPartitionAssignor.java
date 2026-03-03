package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.common.TopicPartition;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.distributed.assignment.AbstractAssignor;
import com.uber.data.kafka.connect.workload.common.Workload;
import com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskPartitionAssignor extends AbstractAssignor<TaskPartitions, TopicPartition> {

    private static final Logger log = LoggerFactory.getLogger(TaskPartitionAssignor.class);

    private final Map<TopicPartition, Double> partitionLoads;

    public TaskPartitionAssignor(
            String srcKafka,
            String dstKafka,
            double defaultPartitionLoad,
            double taskByteRateThreshold,
            double taskMessageRateThreshold
    ) {
        // TODO: Should we have a limit of partitions per task?
        this(
                defaultPartitionLoad,
                retrievePartitionLoads(srcKafka, dstKafka, taskByteRateThreshold, taskMessageRateThreshold)
        );
    }

    @VisibleForTesting
    TaskPartitionAssignor(
            double defaultPartitionLoad,
            Map<TopicPartition, Double> partitionLoads
    ) {
        super(1, defaultPartitionLoad, 0);
        this.partitionLoads = partitionLoads;
    }

    @Override
    protected Double jobLoad(TopicPartition topicPartition) {
        return partitionLoads.get(topicPartition);
    }

    @Override
    protected void assign(TaskPartitions task, TopicPartition topicPartition) {
        task.partitions().add(topicPartition);
    }

    @Override
    protected void revoke(TaskPartitions task, TopicPartition topicPartition) {
        task.partitions().remove(topicPartition);
    }

    @Override
    protected Collection<TopicPartition> jobs(TaskPartitions task) {
        return task.partitions();
    }

    @Override
    protected String groupType() {
        return "task";
    }

    @Override
    protected String jobType() {
        return "partition";
    }

    private static Map<TopicPartition, Double> retrievePartitionLoads(
            String srcKafka,
            String dstKafka,
            double byteRateThreshold,
            double messageRateThreshold
    ) {
        log.debug("Retrieving partition loads in preparation for task partition assignment");

        Map<ReplicatedTopicPartition, Workload> allWorkloads;
        try {
            allWorkloads = KafkaBasedWorkloadProvider.getAllWorkloads();
        } catch (IllegalStateException e) {
            log.warn(
                    "Worker does not appear to be configured with Kafka-based workload provider; "
                            + "will not be able to balance task assignment based on partition workloads"
            );
            return Map.of();
        }

        Map<TopicPartition, Double> result = allWorkloads.entrySet().stream()
                .filter(e -> srcKafka.equals(e.getKey().srcKafka()))
                .filter(e -> dstKafka.equals(e.getKey().dstKafka()))
                .collect(Collectors.toMap(
                        e -> e.getKey().topicPartition(),
                        e -> {
                            Workload workload = e.getValue();
                            double byteRateScale = workload.getBytesPerSecond() / byteRateThreshold;
                            double messageRateScale = workload.getMessagesPerSecond() / messageRateThreshold;
                            return Math.max(byteRateScale, messageRateScale);
                        }
                ));

        log.debug("Finished retrieving partition loads in preparation for task partition assignment");
        return result;
    }
}
