package com.uber.data.kafka.connect.distributed.assignment;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.components.Versioned;

import java.io.Closeable;
import java.util.Map;

public interface WorkloadProvider extends Versioned, Configurable, Closeable {

    /**
     * Determine and return the load for the given task, if it is known
     * @return the load for the task, or null if none is known
     */
    Double taskLoad(String connector, int taskId, Map<String, String> connectorConfig, Map<String, String> taskConfig);

    static com.uber.data.kafka.connect.workload.common.WorkloadProvider noOp() {
        return new com.uber.data.kafka.connect.workload.common.WorkloadProvider() {
            @Override
            public Double taskLoad(String connector, int taskId, Map<String, String> connectorConfig, Map<String, String> taskConfig) {
                return null;
            }

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> configs) {
            }

            @Override
            public String version() {
                return "no-op";
            }
        };
    }

}