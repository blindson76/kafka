package com.uber.data.kafka.connect.distributed.assignment;

import org.apache.kafka.connect.runtime.distributed.UberWorkerLoad;

import java.util.Collection;

public class WorkerConnectorAssignor extends AbstractAssignor<UberWorkerLoad, String> {

    public WorkerConnectorAssignor(int connectorWorkerThreshold) {
        // Try to have at most one connector per worker, and permit at most 10 at a time
        super(connectorWorkerThreshold, 1, 10);
    }

    @Override
    protected Double jobLoad(String job) {
        return 1.0;
    }

    @Override
    protected void assign(UberWorkerLoad worker, String connector) {
        worker.assign(connector);
    }

    @Override
    protected void revoke(UberWorkerLoad worker, String connector) {
        worker.connectors().remove(connector);
    }

    @Override
    protected Collection<String> jobs(UberWorkerLoad worker) {
        return worker.connectors();
    }

    @Override
    protected String groupType() {
        return "worker";
    }

    @Override
    protected String jobType() {
        return "connector";
    }

    @Override
    protected String groupName(UberWorkerLoad worker) {
        return worker.worker();
    }
}
