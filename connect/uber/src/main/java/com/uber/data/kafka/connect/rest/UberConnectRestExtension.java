package com.uber.data.kafka.connect.rest;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import com.uber.data.kafka.connect.rest.filter.ContentTypeHeaderFilter;
import com.uber.data.kafka.connect.rest.interceptor.ConnectorConfigInterceptor;
import com.uber.data.kafka.connect.rest.interceptor.ConnectorOffsetsInterceptor;
import com.uber.data.kafka.connect.rest.resources.AssignmentsResource;
import com.uber.data.kafka.connect.rest.resources.WorkloadsResource;

import org.glassfish.jersey.internal.inject.AbstractBinder;

import java.util.Map;

import jakarta.ws.rs.core.Configurable;

public class UberConnectRestExtension implements ConnectRestExtension {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        Configurable<?> configurable = restPluginContext.configurable();
        configurable.register(ContentTypeHeaderFilter.class);
        configurable.register(new Binder(restPluginContext.clusterState()));
        configurable.register(ConnectorConfigInterceptor.class);
        configurable.register(ConnectorOffsetsInterceptor.class);
        configurable.register(WorkloadsResource.class);
        configurable.register(AssignmentsResource.class);
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    private static class Binder extends AbstractBinder {
        private final ConnectClusterState clusterState;

        public Binder(ConnectClusterState clusterState) {
            this.clusterState = clusterState;
        }

        @Override
        protected void configure() {
            bind(clusterState).to(ConnectClusterState.class);
        }
    }


}
