package com.uber.data.kafka.connect.workload.transformation;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import com.uber.data.kafka.connect.workload.tracking.WorkloadStore;
import com.uber.data.kafka.connect.workload.tracking.WorkloadTrackingProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * {@link org.apache.kafka.connect.transforms.Transformation} implementation that tracks workloads on a per-topic-partition basis as
 * records are consumed from Kafka, and reports those workloads periodically to a pluggable {@link WorkloadStore}
 */
public abstract class WorkloadTrackingTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(WorkloadTrackingTransformation.class);

    private WorkloadTrackingProcessor processor;

    protected abstract WorkloadStore createStore();

    @Override
    public ConfigDef config() {
        return WorkloadTrackingTransformationConfig.config();
    }

    @Override
    public void configure(Map<String, ?> props) {
        WorkloadTrackingTransformationConfig config = new WorkloadTrackingTransformationConfig(props);

        this.processor = new WorkloadTrackingProcessor(
                config.srcKafkaName(),
                config.dstKafkaName(),
                createStore(),
                Set::of, // TODO: Is there any non-insane way to get the set of topic partitions assigned to the task here?
                config.reportInterval()
        );

        this.processor.start();

        log.info("Topic workload tracking is now enabled");
    }

    @Override
    public R apply(R record) {
        processor.track(record);
        return record;
    }

    @Override
    public void close() {
        Utils.closeQuietly(processor, "workload processor");
    }

}
