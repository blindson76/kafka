/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
