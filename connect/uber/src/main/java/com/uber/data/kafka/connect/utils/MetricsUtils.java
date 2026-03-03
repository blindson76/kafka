package com.uber.data.kafka.connect.utils;

import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.ImmutableMap;
import com.uber.m3.tally.Scope;

import java.util.Map;

public class MetricsUtils {

    public static final MetricsUtils INSTANCE = new MetricsUtils();

    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String SRC_CLUSTER = "srcCluster";
    private static final String DST_CLUSTER = "dstCluster";

    private MetricsUtils() {
    }

    public Scope routeScope(String srcKafka, String dstKafka) {
        return M3ReporterProvider.SCOPE
                .tagged(ImmutableMap.of(
                        SRC_CLUSTER, srcKafka,
                        DST_CLUSTER, dstKafka
                ));
    }

    public Map<String, String> tags(TopicPartition topicPartition) {
        return ImmutableMap.of(
                TOPIC, topicPartition.topic(),
                PARTITION, Integer.toString(topicPartition.partition())
        );
    }

}
