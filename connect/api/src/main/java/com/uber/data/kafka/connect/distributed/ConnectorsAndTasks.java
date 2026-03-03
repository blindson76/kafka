package com.uber.data.kafka.connect.distributed;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public record ConnectorsAndTasks(Set<String> connectors, Set<ConnectorTaskId> tasks) {

    public ConnectorsAndTasks(Set<String> connectors, Set<ConnectorTaskId> tasks) {
        this.connectors = Collections.unmodifiableSet(new LinkedHashSet<>(connectors));
        this.tasks = Collections.unmodifiableSet(new LinkedHashSet<>(tasks));
    }

    public Builder toBuilder() {
        return builder()
                .add(this);
    }

    public static ConnectorsAndTasks of(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
        return new ConnectorsAndTasks(Set.copyOf(connectors), Set.copyOf(tasks));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
        return builder()
                .addConnectors(connectors)
                .addTasks(tasks);
    }

    public static class Builder {

        private final Set<String> connectors;
        private final Set<ConnectorTaskId> tasks;

        public Builder() {
            this.connectors = new LinkedHashSet<>();
            this.tasks = new LinkedHashSet<>();
        }

        public Builder addConnectors(Collection<String> connectors) {
            this.connectors.addAll(connectors);
            return this;
        }

        public Builder addTasks(Collection<ConnectorTaskId> tasks) {
            this.tasks.addAll(tasks);
            return this;
        }

        public Builder add(ConnectorsAndTasks connectorsAndTasks) {
            return this
                    .addConnectors(connectorsAndTasks.connectors())
                    .addTasks(connectorsAndTasks.tasks());
        }

        public Builder removeConnectors(Collection<String> connectors) {
            this.connectors.removeAll(connectors);
            return this;
        }

        public Builder removeTasks(Collection<ConnectorTaskId> tasks) {
            this.tasks.removeAll(tasks);
            return this;
        }

        public Builder remove(ConnectorsAndTasks connectorsAndTasks) {
            return this
                    .removeConnectors(connectorsAndTasks.connectors())
                    .removeTasks(connectorsAndTasks.tasks());
        }

        public ConnectorsAndTasks build() {
            return new ConnectorsAndTasks(connectors, tasks);
        }

    }

}
