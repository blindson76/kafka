package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskPartitionAssignorTest {

    private static final Logger log = LoggerFactory.getLogger(TaskPartitionAssignorTest.class);

    private static final String CONNECTOR = "connector";
    private static final String TOPIC = "topic";

    @ParameterizedTest
    @CsvSource({
            "1,100_000", "2,100_000", "5,100_000", "50,100_000", "500,100_000", "5_000,100_000", "50_000,100_000", "100_000,100_000", "200_000,100_000",
            "1,1_000_000", "2,1_000_000", "5,1_000_000", "50,1_000_000", "500,1_000_000", "5_000,1_000_000", "50_000,1_000_000", "500_000,1_000_000", "1_000_000,1_000_000"
    })
    public void testInitialAssignment(int numTasks, int numPartitions) {
        Collection<TaskPartitions> taskPartitions = IntStream.range(0, numTasks)
                .mapToObj(i -> new TaskPartitions(CONNECTOR, i, new HashSet<>()))
                .toList();

        Map<TopicPartition, Double> partitionLoads = IntStream.range(0, numPartitions)
                .mapToObj(i -> new TopicPartition(TOPIC, i))
                .collect(Collectors.toMap(
                        Function.identity(),
                        tp -> 0.1 * (tp.partition() % 100)
                ));

        Set<TopicPartition> toAssign = partitionLoads.keySet();

        TaskPartitionAssignor assignor = new TaskPartitionAssignor(0.1, partitionLoads);

        log.info("Beginning initial assignment");
        assignor.assign(taskPartitions, toAssign);
        log.info("Finished initial assignment");
    }
}
