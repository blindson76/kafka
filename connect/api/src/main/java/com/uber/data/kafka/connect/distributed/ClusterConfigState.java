package com.uber.data.kafka.connect.distributed;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ClusterConfigState {

    Set<String> connectors();

    List<ConnectorTaskId> tasks(String connector);

    Map<String, String> connectorConfig(String connector);

    Map<String, String> taskConfig(ConnectorTaskId task);

}
