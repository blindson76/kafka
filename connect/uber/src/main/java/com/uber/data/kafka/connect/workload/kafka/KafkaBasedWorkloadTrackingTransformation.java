package com.uber.data.kafka.connect.workload.kafka;

import org.apache.kafka.connect.connector.ConnectRecord;

import com.uber.data.kafka.connect.workload.tracking.WorkloadStore;
import com.uber.data.kafka.connect.workload.transformation.WorkloadTrackingTransformation;

public class KafkaBasedWorkloadTrackingTransformation<R extends ConnectRecord<R>> extends WorkloadTrackingTransformation<R> {

    @Override
    protected WorkloadStore createStore() {
        return KafkaBasedWorkloadProvider::publishWorkload;
    }

}
