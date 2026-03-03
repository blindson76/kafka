package com.uber.data.kafka.connect.distributed.assignment;

import org.apache.kafka.connect.runtime.distributed.UberWorkerLoad;

import com.uber.data.kafka.connect.distributed.ClusterConfigState;
import com.uber.data.kafka.connect.distributed.ConnectorTaskId;
import com.uber.data.kafka.connect.workload.common.WorkloadProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class WorkerTaskAssignor extends AbstractAssignor<UberWorkerLoad, ConnectorTaskId> {

    private static final Logger log = LoggerFactory.getLogger(WorkerTaskAssignor.class);

    private final WorkloadProvider workloadProvider;
    private final ClusterConfigState clusterConfigState;

    public WorkerTaskAssignor(
            WorkloadProvider workloadProvider,
            ClusterConfigState clusterConfigState,
            double workerLoadThreshold,
            double defaultTaskLoad,
            int workerTaskLimit
    ) {
        super(workerLoadThreshold, defaultTaskLoad, workerTaskLimit);
        this.workloadProvider = workloadProvider;
        this.clusterConfigState = clusterConfigState;
    }

    @Override
    protected Double jobLoad(ConnectorTaskId task) {
        Map<String, String> taskConfig = clusterConfigState.taskConfig(task);
        if (taskConfig == null) {
            log.warn("Unable to find config for task {}", task);
            return null;
        }

        String connector = task.connector();
        Map<String, String> connectorConfig = clusterConfigState.connectorConfig(connector);
        if (connectorConfig == null) {
            log.warn("Unable to find config for connector {}", connector);
            return null;
        }

        return workloadProvider.taskLoad(connector, task.task(), connectorConfig, taskConfig);
    }

    @Override
    protected void assign(UberWorkerLoad worker, ConnectorTaskId task) {
        worker.assign(task);
    }

    @Override
    protected void revoke(UberWorkerLoad worker, ConnectorTaskId task) {
        worker.tasks().remove(task);
    }

    @Override
    protected Collection<ConnectorTaskId> jobs(UberWorkerLoad worker) {
        return worker.tasks();
    }

    @Override
    protected String groupType() {
        return "worker";
    }

    @Override
    protected String jobType() {
        return "task";
    }

    @Override
    protected String groupName(UberWorkerLoad worker) {
        return worker.worker();
    }
}
