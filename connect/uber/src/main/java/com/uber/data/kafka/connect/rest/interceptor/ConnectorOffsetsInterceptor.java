package com.uber.data.kafka.connect.rest.interceptor;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ReaderInterceptor;
import jakarta.ws.rs.ext.ReaderInterceptorContext;

import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.TOPICS_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Task.SOURCE_PARTITION_KAFKA_TOPIC;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.isURep3;
import static org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource.UBER_FORCE_OFFSET_PATCH;

public class ConnectorOffsetsInterceptor implements ReaderInterceptor {

    public static final String SKIP_QUERY_PARAM = "uberSkipInterception";

    private static final Logger log = LoggerFactory.getLogger(ConnectorOffsetsInterceptor.class);

    private final ConnectClusterState clusterState;

    @Inject
    public ConnectorOffsetsInterceptor(ConnectClusterState clusterState) {
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
        } else if (!Boolean.parseBoolean(uriInfo.getQueryParameters().getFirst(UBER_FORCE_OFFSET_PATCH))) {
            log.debug("Request will not be forced if connector is not stopped; skipping interception since no safeguard is necessary");
            return entity;
        }

        if (!isConnectorOffsetPatchRequest()) {
            return entity;
        }

        String path = uriInfo.getPath();
        Matcher m = Pattern.compile("/*connectors/+([^/]+)/+offsets").matcher(path);
        if (!m.matches()) {
            log.error("Unable to intercept connector offset patch request: unrecognized URL path");
            return entity;
        }
        String connectorName = m.group(1);
        if (connectorName == null) {
            log.error("Unable to intercept connector offset patch request: could not deduce connector name from URL path");
            return entity;
        }

        Map<String, String> connectorConfig = clusterState.connectorConfig(connectorName);
        if (connectorConfig == null) {
            log.warn("Unable to locate config for connector {}; will not be able to safeguard offset patch request", connectorName);
            return entity;
        }

        if (!isURep3(connectorConfig)) {
            log.warn("Skipping interception for connector {} since it is not uReplicator3", connectorName);
            return entity;
        }

        if (!(entity instanceof ConnectorOffsets offsets)) {
            log.warn("Unrecognized entity type {} for connector {}; will not be able to safeguard offset patch request", entity.getClass(), connectorName);
            return entity;
        }

        String topicsValue = connectorConfig.get(TOPICS_CONFIG);
        @SuppressWarnings("unchecked")
        List<String> topicsList = (List<String>) ConfigDef.parseType(TOPICS_CONFIG, topicsValue, Type.LIST);
        if (topicsList == null) {
            log.warn("Unable to parse topics list in config for connector {}; will not be able to safeguard offset patch request", connectorName);
            return entity;
        }

        Set<String> invalidTopics = new TreeSet<>();
        Set<String> topicsSet = new HashSet<>(topicsList); // We could also potentially fetch this from the assignment store, which might be more accurate
        offsets.offsets().forEach(offset -> {
            Object rawTopic = offset.partition().get(SOURCE_PARTITION_KAFKA_TOPIC);
            // TODO: Technically this is too broad; we should also narrow down by src/dst Kafka cluster as well
            //       I'm too lazy to implement that right now and am currently betting it will not come back to bite me
            //       (if you are reading this I was probably wrong, sorry 🥲)
            if (rawTopic instanceof String topic && topicsSet.contains(topic))
                invalidTopics.add(topic);
        });

        if (!invalidTopics.isEmpty()) {
            String message = "Cannot forcibly alter offsets for connector " + connectorName + " since it is still configured "
                    + "to replicate these topics: " + invalidTopics
                    + "\nEither stop the connector before modifying its offsets, or reconfigure the connector to no longer "
                    + "replicate from these topics before trying again";
            throw new BadRequestException(message);
        }

        return entity;
    }

    private boolean shouldSkip() {
        if(httpRequest.getParameter(SKIP_QUERY_PARAM) != null) {
            log.debug("Skipping interception (requested by {} URL query parameter)", SKIP_QUERY_PARAM);
            return true;
        }
        return false;
    }

    private boolean isConnectorOffsetPatchRequest() {
        return "ConnectorsResource".equals(resourceInfo.getResourceClass().getSimpleName())
                && HttpMethod.PATCH.equals(httpRequest.getMethod())
                && "alterConnectorOffsets".equals(resourceInfo.getResourceMethod().getName());
    }

}