package org.apache.kafka.connect.runtime.distributed;

import com.uber.data.kafka.connect.distributed.ConnectorTaskId;
import com.uber.data.kafka.connect.distributed.ConnectorsAndTasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public record UberWorkerLoad(String worker, Collection<String> connectors, Collection<ConnectorTaskId> tasks) {

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

    /**
     * <b>IMPORTANT:</b> we do not hash on the sets of connectors or tasks, for two vital reasons:
     * <ul>
     *     <li>These collections are mutable and expected to change over the lifetime of the object, causing its hash code to change</li>
     *     <li>These collections may grow fairly large and the time to compute their hash code should remain as low as possible since they
     *     may be used as keys in maps for, e.g., memoization of total worker load</li>
     * </ul>
     * @return the hash code of the worker load, derived solely from the worker ID
     */
    @Override
    public int hashCode() {
        return Objects.hash(worker);
    }
}
