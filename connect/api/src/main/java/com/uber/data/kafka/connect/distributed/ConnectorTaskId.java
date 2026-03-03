package com.uber.data.kafka.connect.distributed;

public record ConnectorTaskId(String connector, int task) implements Comparable<ConnectorTaskId> {

    @Override
    public int compareTo(ConnectorTaskId o) {
        int connectorCmp = connector.compareTo(o.connector);
        if (connectorCmp != 0)
            return connectorCmp;
        return Integer.compare(task, o.task);
    }

}
