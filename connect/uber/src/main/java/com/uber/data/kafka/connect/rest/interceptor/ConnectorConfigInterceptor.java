package com.uber.data.kafka.connect.rest.interceptor;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;

import com.uber.data.kafka.connect.utils.BrokerResolver;
import com.uber.data.kafka.exception.ResolverException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ReaderInterceptor;
import jakarta.ws.rs.ext.ReaderInterceptorContext;

import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.CLIENTS_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_CLIENTS_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_CLUSTER_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_PRODUCER_PREFIX;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.isURep3;

public class ConnectorConfigInterceptor implements ReaderInterceptor {

    public static final String SKIP_QUERY_PARAM = "uberSkipInterception";

    private static final Logger log = LoggerFactory.getLogger(ConnectorConfigInterceptor.class);

    private static final String PRODUCER_OVERRIDE_PREFIX = "producer.override.";
    private static final String PRODUCER_OVERRIDE_BOOTSTRAP_CONFIG =
            PRODUCER_OVERRIDE_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    private final ConnectClusterState clusterState;

    @Inject
    public ConnectorConfigInterceptor(ConnectClusterState clusterState) {
        this.clusterState = clusterState;
    }

    @Context
    private UriInfo uriInfo;

    @Context
    private ResourceInfo resourceInfo;

    @Context
    private HttpServletRequest httpRequest;

    @Override
    public Object aroundReadFrom(ReaderInterceptorContext readerInterceptorContext) throws IOException, WebApplicationException {
        Object entity = readerInterceptorContext.proceed();
        if (shouldSkip()) {
            return entity;
        } else if (entity == null) {
            log.debug("Request has no body; skipping interception");
            return null;
        }

        if (isConnectorPostRequest()) {
            if (entity instanceof CreateConnectorRequest createRequest) {
                return interceptConnectorConfig(
                        createRequest.config(),
                        createRequest,
                        CreateConnectorRequest::config,
                        intercepted -> new CreateConnectorRequest(createRequest.name(), intercepted, createRequest.initialState())
                );
            } else {
                log.error("Unable to intercept connector create request");
                return entity;
            }
        } else if (isConnectorConfigPutRequest()) {
            if (entity instanceof Map<?, ?> rawConfig) {
                @SuppressWarnings("unchecked")
                Map<String, String> originalConfig = (Map<String, String>) rawConfig;
                return interceptConnectorConfig(
                        originalConfig,
                        originalConfig,
                        Function.identity(),
                        Function.identity()
                );
            } else {
                log.error("Unable to intercept connector config put request");
                return entity;
            }
        } else if (isConnectorConfigPatchRequest()) {
            String path = uriInfo.getPath();
            Matcher m = Pattern.compile("/*connectors/+([^/]+)/+config").matcher(path);
            if (!m.matches()) {
                log.error("Unable to intercept connector config patch request: unrecognized URL path");
                return entity;
            }
            String connectorName = m.group(1);
            if (connectorName == null) {
                log.error("Unable to intercept connector config patch request: could not deduce connector name from URL path");
                return entity;
            }
            // This is technically prone to race conditions since the connector config may be
            // modified in between us reading it and when we allow the patch request to go through
            // Changing a connector's type is highly unusual and doing it with the kind of frequency
            // necessary to make that kind of race condition actually happen is not a usage pattern
            // worth accommodating right now
            Map<String, String> connectorConfig = clusterState.connectorConfig(connectorName);
            if (connectorConfig == null) {
                return entity;
            }
            if (entity instanceof Map<?, ?> rawPatch) {
                @SuppressWarnings("unchecked")
                Map<String, String> originalPatch = (Map<String, String>) rawPatch;
                return interceptConnectorConfig(
                        connectorConfig,
                        originalPatch,
                        Function.identity(),
                        Function.identity()
                );
            } else {
                log.error("Unable to intercept connector config patch request: unrecognized request body type {}", entity);
                return entity;
            }
        } else {
            return entity;
        }
    }

    private boolean shouldSkip() {
        if(httpRequest.getParameter(SKIP_QUERY_PARAM) != null) {
            log.debug("Skipping interception (requested by {} URL query parameter)", SKIP_QUERY_PARAM);
            return true;
        }
        return false;
    }

    private boolean isConnectorPostRequest() {
        return isConnectorConfigRequest("createConnector", HttpMethod.POST);
    }

    private boolean isConnectorConfigPutRequest() {
        return isConnectorConfigRequest("putConnectorConfig", HttpMethod.PUT);
    }

    private boolean isConnectorConfigPatchRequest() {
        return isConnectorConfigRequest("patchConnectorConfig", HttpMethod.PATCH);
    }

    private boolean isConnectorConfigRequest(String resourceMethod, String httpMethod) {
        return isConnectorRequest()
                && httpMethod.equals(httpRequest.getMethod())
                && resourceMethod.equals(resourceInfo.getResourceMethod().getName());
    }

    private boolean isConnectorRequest() {
        return resourceInfo.getResourceClass().getSimpleName().equals("ConnectorsResource");
    }

    private <T> T interceptConnectorConfig(
            Map<String, String> completeCurrentConnectorConfig,
            T originalEntity,
            Function<T, Map<String, String>> configExtractor,
            Function<Map<String, String>, T> entityReconstructor
    ) {
        if (!isURep3(completeCurrentConnectorConfig)) {
            log.debug("Ignoring config for non-uReplicator3 connector");
            return originalEntity;
        }

        Map<String, String> originalConfig = configExtractor.apply(originalEntity);
        Map<String, String> interceptedConfig = interceptConnectorConfig(originalConfig);
        return entityReconstructor.apply(interceptedConfig);
    }

    private Map<String, String> interceptConnectorConfig(Map<String, String> connectorConfig) {
        Map<String, String> result = new HashMap<>(connectorConfig);

        String targetCluster = connectorConfig.get(TARGET_CLUSTER_CONFIG);
        if (targetCluster != null) {
            if (!connectorConfig.containsKey(PRODUCER_OVERRIDE_BOOTSTRAP_CONFIG)) {
                String targetBootstrap = connectorConfig.get(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG);
                if (targetBootstrap == null)
                    targetBootstrap = resolveBootstrap(targetCluster);
                result.put(PRODUCER_OVERRIDE_BOOTSTRAP_CONFIG, targetBootstrap);
            }
        }

        translatePrefix(connectorConfig, result, CLIENTS_PREFIX, PRODUCER_OVERRIDE_PREFIX);
        translatePrefix(connectorConfig, result, TARGET_CLIENTS_PREFIX, PRODUCER_OVERRIDE_PREFIX);
        translatePrefix(connectorConfig, result, TARGET_PRODUCER_PREFIX, PRODUCER_OVERRIDE_PREFIX);

        // TODO: Auto-inject default partition byte and message rates,
        //       and possibly max per-task byte and message rates

        return result;
    }

    private void translatePrefix(Map<String, String> from, Map<String, String> to, String fromPrefix, String toPrefix) {
        from.forEach((prefixedProp, value) -> {
            if (prefixedProp.startsWith(fromPrefix)) {
                String strippedProp = prefixedProp.substring(fromPrefix.length());
                if (strippedProp.isBlank()) {
                    log.warn("Ignoring property {} with value {}", prefixedProp, value);
                    return;
                }

                to.put(toPrefix + strippedProp, value);
            }
        });
    }

    private String resolveBootstrap(String cluster) {
        try {
            return BrokerResolver.resolveBootstrap(cluster);
        } catch (ResolverException e) {
            throw new ConnectRestException(
                    Status.BAD_REQUEST,
                    "Failed to resolve bootstrap servers for cluster " + cluster,
                    e
            );
        }
    }

}