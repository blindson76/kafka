package com.uber.data.kafka.connect.distributed.assignment;

import com.uber.data.kafka.connect.distributed.ConnectorTaskId;
import com.uber.data.kafka.connect.distributed.ConnectorsAndTasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public class UberWorkerLoad {

    private final String worker;
    private final Collection<String> connectors;
    private final Collection<ConnectorTaskId> tasks;

    public UberWorkerLoad(String worker, ConnectorsAndTasks connectorsAndTasks) {
        this(worker, connectorsAndTasks.connectors(), connectorsAndTasks.tasks());
    }

    public UberWorkerLoad(
            String worker,
            Collection<String> connectors,
            Collection<ConnectorTaskId> tasks
    ) {
        this.worker = worker;
        this.connectors = connectors != null ? new ArrayList<>(connectors) : new ArrayList<>();
        this.tasks = tasks != null ? new ArrayList<>(tasks) : new ArrayList<>();
    }

    public String worker() {
        return worker;
    }

    public Collection<String> connectors() {
        return connectors;
    }

    public Collection<ConnectorTaskId> tasks() {
        return tasks;
    }

    public void assign(String connector) {
        connectors.add(connector);
    }

    public void assign(ConnectorTaskId task) {
        tasks.add(task);
    }

    @Override
    public String toString() {
        return "UberWorkerLoad{ worker=" + worker + ", connectorIds=" + connectors + ", taskIds=" + tasks + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UberWorkerLoad that)) {
            return false;
        }
        return Objects.equals(this.worker, that.worker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(worker);
    }
}
