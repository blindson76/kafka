package org.apache.kafka.connect.runtime.distributed;

import com.uber.data.kafka.connect.distributed.ClusterConfigState;
import com.uber.data.kafka.connect.distributed.ConnectorTaskId;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterConfigStateImpl implements ClusterConfigState {

    private final org.apache.kafka.connect.storage.ClusterConfigState configSnapshot;

    public ClusterConfigStateImpl(org.apache.kafka.connect.storage.ClusterConfigState configSnapshot) {
        this.configSnapshot = configSnapshot;
    }

    @Override
    public Set<String> connectors() {
        return configSnapshot.connectors();
    }

    @Override
    public List<ConnectorTaskId> tasks(String connector) {
        return configSnapshot.tasks(connector).stream()
                .map(org.apache.kafka.connect.util.ConnectorTaskId::uberToPublicApi)
                .toList();
    }

    @Override
    public Map<String, String> connectorConfig(String connector) {
        return configSnapshot.connectorConfig(connector);
    }

    @Override
    public Map<String, String> taskConfig(ConnectorTaskId task) {
        return configSnapshot.taskConfig(new org.apache.kafka.connect.util.ConnectorTaskId(task.connector(), task.task()));
    }
}
