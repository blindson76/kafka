package com.uber.data.kafka.connect.workload.transformation;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.time.Duration;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class WorkloadTrackingTransformationConfig extends AbstractConfig {

    public static final String SRC_CLUSTER_NAME_CONFIG = "source.cluster.name";
    private static final String SRC_CLUSTER_NAME_DOC = "Name of the source Kafka cluster";

    public static final String DST_CLUSTER_NAME_CONFIG = "target.cluster.name";
    private static final String DST_CLUSTER_NAME_DOC = "Name of the target Kafka cluster";

    public static final String REPORT_INTERVAL_MS_CONFIG = "report.interval.ms";
    private static final String REPORT_INTERVAL_MS_DOC = "How long to record traffic for in between reporting recorded values";

    public WorkloadTrackingTransformationConfig(Map<String, ?> props) {
        super(config(), props);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(
                        SRC_CLUSTER_NAME_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        SRC_CLUSTER_NAME_DOC
                ).define(
                        DST_CLUSTER_NAME_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        DST_CLUSTER_NAME_DOC
                ).define(
                        REPORT_INTERVAL_MS_CONFIG,
                        Type.LONG,
                        Duration.ofMinutes(10).toMillis(),
                        atLeast(Duration.ofSeconds(1).toMillis()),
                        Importance.LOW,
                        REPORT_INTERVAL_MS_DOC
                );
    }

    public String srcKafkaName() {
        return getString(SRC_CLUSTER_NAME_CONFIG);
    }

    public String dstKafkaName() {
        return getString(DST_CLUSTER_NAME_CONFIG);
    }

    public Duration reportInterval() {
        return Duration.ofMillis(getLong(REPORT_INTERVAL_MS_CONFIG));
    }

}
