package com.uber.data.kafka.connect.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.distributed.assignment.UberClusterAssignor;
import com.uber.data.kafka.connect.rest.UberConnectRestExtension;
import com.uber.data.kafka.connect.rest.entities.AssignedPartitionEntity;
import com.uber.data.kafka.connect.rest.entities.PartitionWorkloadEntity;
import com.uber.data.kafka.connect.rest.entities.TaskAssignmentEntity;
import com.uber.data.kafka.connect.rest.entities.WorkerWorkloadEntity;
import com.uber.data.kafka.connect.rest.entities.WorkloadEntity;
import com.uber.data.kafka.connect.ureplicator3.UReplicator3Connector;
import com.uber.data.kafka.connect.ureplicator3.UReplicator3Task;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignedPartition;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStoreConfig;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStores;
import com.uber.data.kafka.connect.workload.common.Workload;
import com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProvider;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import static com.uber.data.kafka.connect.distributed.assignment.UberClusterAssignorConfig.UBER_WORKLOAD_PROVIDER_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_CLIENTS_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_PRODUCER_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.REFRESH_INTERVAL_MS;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.SOURCE_ADMIN_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.SOURCE_CLUSTER_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.SOURCE_CONSUMER_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.TARGET_ADMIN_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.TARGET_CLUSTER_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.TOPICS_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3ConnectorConfig.TOPIC_RENAME_PREFIX;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Task.SOURCE_OFFSET_KAFKA_OFFSET;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3TaskConfig.WORKLOAD_REPORT_INTERVAL_MS_CONFIG;
import static com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProviderConfig.STORAGE_TOPIC_CONFIG;
import static com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProviderConfig.STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.PLUGIN_DISCOVERY_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.UBER_CLUSTER_ASSIGNOR_CONFIG;
import static org.apache.kafka.connect.runtime.rest.RestServerConfig.REST_EXTENSION_CLASSES_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class ReplicationIntegrationTest {

    // TODO: These test cases are taking pretty long, even though we share embedded Kafka clusters across cases
    //       If we have time we should figure out what's taking so long and try to speed things up

    private static final Logger log = LoggerFactory.getLogger(ReplicationIntegrationTest.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

    private static final int NUM_WORKERS = 3;
    private static final String WORKLOAD_TOPIC = "uber-workloads";
    private static final String ASSIGNMENTS_TOPIC = "ureplicator3-assignments";
    private static final Map<String, EmbeddedKafkaCluster> EXTRA_KAFKA_CLUSTERS = new ConcurrentHashMap<>();

    private static EmbeddedConnectCluster connect;

    @BeforeAll
    public static void setup() throws Exception {
        Map<String, String> workerProps = new HashMap<>(); // Has to be mutable to work with the embedded Connect cluster API
        workerProps.put(UBER_CLUSTER_ASSIGNOR_CONFIG, UberClusterAssignor.class.getName());
        workerProps.put(UBER_WORKLOAD_PROVIDER_CONFIG, KafkaBasedWorkloadProvider.class.getName());
        workerProps.put(STORAGE_TOPIC_CONFIG, WORKLOAD_TOPIC);
        workerProps.put(STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG, "1"); // We run a single embedded broker
        workerProps.put(AssignmentStoreConfig.STORAGE_TOPIC_CONFIG, ASSIGNMENTS_TOPIC);
        workerProps.put(AssignmentStoreConfig.STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        workerProps.put(PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.SERVICE_LOAD.name()); // Fast startup time ⚡
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
        workerProps.put(REST_EXTENSION_CLASSES_CONFIG, UberConnectRestExtension.class.getName());
        workerProps.put(KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        workerProps.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());

        int numBrokers = 1;
        Properties brokerProps = new Properties();

        // build a Connect cluster backed by Kafka
        connect = new EmbeddedConnectCluster.Builder()
                .numWorkers(NUM_WORKERS)
                .workerProps(workerProps)
                .numBrokers(numBrokers)
                .brokerProps(brokerProps)
                .build();

        connect.start();
        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS, "Cluster did not start in time");
    }

    @AfterEach
    public void close() throws Exception {
        log.info("Cleaning up test case resources");

        // Wipe every connector's offsets and then delete it
        for (String connector : connect.connectors()) {
            connect.stopConnector(connector);
            connect.assertions().assertConnectorIsStopped(connector, "connector did not stop in time");

            connect.resetConnectorOffsets(connector);

            connect.deleteConnector(connector);
            connect.assertions().assertConnectorDoesNotExist(connector, "connector was not deleted in time");
        }

        // Delete testing topics from our extra Kafka clusters
        for (EmbeddedKafkaCluster kafkaCluster : EXTRA_KAFKA_CLUSTERS.values()) {
            try (Admin admin = kafkaCluster.createAdminClient()) {
                ListTopicsOptions listOptions = new ListTopicsOptions().listInternal(false);
                Set<String> topics = admin.listTopics(listOptions).names().get();

                admin.deleteTopics(topics);
            }
        }

        // Wipe out any workloads and assignments generated during this test case so they don't affect other cases
        KafkaBasedWorkloadProvider.clearCache();
        AssignmentStores.get().clear();

        log.info("Finished cleaning up test case resources");
    }

    @AfterAll
    public static void teardown() {
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();

        // stop the Connect cluster and its backing Kafka cluster.
        if (connect != null) {
            Utils.closeQuietly(connect::stop, "Embedded Connect cluster", shutdownFailure);
        }

        // Stop every extra Kafka cluster
        EXTRA_KAFKA_CLUSTERS.forEach((name, kafkaCluster) ->
                Utils.closeQuietly(kafkaCluster::stop, "Embedded Kafka cluster '" + name + "'", shutdownFailure)
        );

        if (shutdownFailure.get() != null) {
            throw new AssertionError("Failed to cleanly shut down testing resources", shutdownFailure.get());
        }
    }

    @Test
    public void testContentTypeHeaderFilter() throws Exception {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());

        String topic = "t1";
        srcCluster.createTopic(topic);
        dstCluster.createTopic(topic);

        // If your IDE tells you this should be closed, it's using a more up-to-date JDK version than the project builds with
        HttpClient httpClient = HttpClient.newHttpClient();
        String resource = "connector-plugins/" + UReplicator3Connector.class.getSimpleName() + "/config/validate";
        Map<String, String> connectorConfig = baseURep3Config(srcKafkaName, dstKafkaName, 3, Set.of(topic));
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(connectorConfig);

        HttpRequest request = HttpRequest.newBuilder(new URI(connect.endpointForResource(resource)))
                .PUT(BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        // 415 (unsupported media type) is what we'd see with the header filter disabled,
        // since we didn't specify a content-type header in our request
        assertEquals(200, response.statusCode());

        request = HttpRequest.newBuilder(new URI(connect.endpointForResource(resource)))
                .PUT(BodyPublishers.ofByteArray(body))
                .header(CONTENT_TYPE, APPLICATION_FORM_URLENCODED) // Make sure this stupid shit gets overridden too (curl, I'm not mad at you, I'm just disappointed)
                .build();
        response = httpClient.send(request, BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    public void testPublishWorkloadsManually() throws Exception {
        assertEquals(
                Map.of(),
                getAllWorkloads()
        );

        ReplicatedTopicPartition tp1 = new ReplicatedTopicPartition("src1", "dst1", "t", 0);
        Workload w1 = new Workload(3.0, 4.0);
        manuallyPublishWorkload(tp1, w1);

        assertEquals(
                Map.of(tp1, w1),
                getAllWorkloads()
        );

        ReplicatedTopicPartition tp2 = new ReplicatedTopicPartition("src2", "dst2", "t", 0);
        Workload w2 = new Workload(35.0, 14.0);
        manuallyPublishWorkload(tp2, w2);

        assertEquals(
                Map.of(tp1, w1, tp2, w2),
                getAllWorkloads()
        );

        manuallyDeleteWorkload(tp1);

        assertEquals(
                Map.of(tp2, w2),
                getAllWorkloads()
        );
    }

    @Test
    public void testNormalConnectors() throws Exception {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());

        int numProduced = 0;
        try (Producer<byte[], byte[]> srcProducer = srcCluster.createProducer(Map.of())) {
            for (int i = 1; i <= 6; i++) {
                String topic = "t" + i;
                srcCluster.createTopic(topic, i);
                dstCluster.createTopic(topic, i);

                for (int partition = 0; partition < i; partition++) {
                    produceTestRecord(srcProducer, topic, partition, 0);
                    numProduced++;
                }
            }

            srcProducer.flush();
        }

        String conn1 = "connector-one";
        String conn2 = "connector-two";

        startConnector(conn1, srcKafkaName, dstKafkaName, 3, Set.of("t1", "t2", "t3"), null, false);
        startConnector(conn2, srcKafkaName, dstKafkaName, 1, Set.of("t4", "t5", "t6"), null, false);

        dstCluster.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6");

        // Stop the connectors to force offset commits
        connect.stopConnector(conn1);
        connect.stopConnector(conn2);

        connect.assertions().assertConnectorIsStopped(conn1, "Connector and/or tasks did not stop in time");
        connect.assertions().assertConnectorIsStopped(conn2, "Connector and/or tasks did not stop in time");

        waitForCondition(() -> {
            Map<Map<String, ?>, Map<String, ?>> actualOffsets = connect.connectorOffsets(conn1).toMap();
            Map<Map<String, ?>, Map<String, ?>> expectedOffsets = IntStream.rangeClosed(1, 3)
                    .boxed()
                    .flatMap(t -> IntStream.range(0, t)
                            .mapToObj(p -> {
                                Map<String, Object> sourcePartition = UReplicator3Task.encodeSourcePartition(srcKafkaName, dstKafkaName, "t" + t, p);
                                // Can't use UReplicator3Task::encodeSourceOffset because it encodes as a long instead of an integer,
                                // and the response from the REST API above gets deserialized as an integer instead of a long
                                // I lost several hours debugging this fucking bullshit if you want to do better have at it
                                Map<String, Object> sourceOffset = Map.of(SOURCE_OFFSET_KAFKA_OFFSET, 0);
                                return new AbstractMap.SimpleEntry<>(sourcePartition, sourceOffset);
                            })
                    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertEquals(actualOffsets, expectedOffsets);

            actualOffsets = connect.connectorOffsets(conn2).toMap();
            expectedOffsets = IntStream.rangeClosed(4, 6)
                    .boxed()
                    .flatMap(t -> IntStream.range(0, t)
                            .mapToObj(p -> {
                                Map<String, Object> sourcePartition = UReplicator3Task.encodeSourcePartition(srcKafkaName, dstKafkaName, "t" + t, p);
                                Map<String, Object> sourceOffset = Map.of(SOURCE_OFFSET_KAFKA_OFFSET, 0);
                                return new AbstractMap.SimpleEntry<>(sourcePartition, sourceOffset);
                            })
                    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertEquals(actualOffsets, expectedOffsets);

            return true;
        }, 30_000, "Connector offsets were not updated in time");

        // Produce some more traffic
        try (Producer<byte[], byte[]> srcProducer = srcCluster.createProducer(Map.of())) {
            for (int i = 1; i <= 6; i++) {
                String topic = "t" + i;

                for (int partition = 0; partition < i; partition++) {
                    produceTestRecord(srcProducer, topic, partition, 1);
                    numProduced++;
                }
            }

            srcProducer.flush();
        }

        connect.resumeConnector(conn1);
        connect.resumeConnector(conn2);

        dstCluster.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6");
        int numConsumed = dstCluster.consumeAll(60_000, "t1", "t2", "t3", "t4", "t5", "t6").count();
        assertEquals(numProduced, numConsumed);

        // Reconfigure to stop replicating from t3
        startConnector(conn1, srcKafkaName, dstKafkaName, 2, Set.of("t1", "t2"), null, false);

        ConnectorOffset t1p0Offset = new ConnectorOffset(
                UReplicator3Task.encodeSourcePartition(srcKafkaName, dstKafkaName, "t1", 0),
                UReplicator3Task.encodeSourceOffset(-1)
        );
        ConnectorOffset t3p0Offset = new ConnectorOffset(
                UReplicator3Task.encodeSourcePartition(srcKafkaName, dstKafkaName, "t3", 0),
                UReplicator3Task.encodeSourceOffset(-1)
        );

        final ConnectorOffsets invalidOffsets = new ConnectorOffsets(List.of(t1p0Offset, t3p0Offset));
        // Should fail because the connector is still running
        ConnectRestException failure = assertThrows(ConnectRestException.class, () -> connect.alterConnectorOffsets(conn1, invalidOffsets));
        assertEquals(Status.BAD_REQUEST.getStatusCode(), failure.statusCode());

        // Should fail because the connector is still assigned one of the altered topics
        failure = assertThrows(ConnectRestException.class, () -> connect.alterConnectorOffsets(conn1, invalidOffsets, true));
        assertEquals(Status.BAD_REQUEST.getStatusCode(), failure.statusCode());

        ConnectorOffsets validOffsets = new ConnectorOffsets(List.of(t3p0Offset));
        // Should succeed
        connect.alterConnectorOffsets(conn1, validOffsets, true);

        // Reconfigure to resume replicating from t3
        startConnector(conn1, srcKafkaName, dstKafkaName, 1, Set.of("t1", "t2", "t3"), null, false);

        dstCluster.consume(4, TimeUnit.MINUTES.toMillis(1), "t3");
        List<ConsumerRecord<byte[], byte[]>> consumed = dstCluster.consumeAll(60_000, "t3")
                .records(new TopicPartition("t3", 0));
        // UReplicator3 currently only provides at-least-once semantics; it's possible there could be some
        // duplication for our reset partition (or any other replicated partition)
        assertTrue(consumed.size() >= 4);
        long replicatedFirstRecords = consumed.stream()
                .filter(record -> Arrays.equals(record.value(), "value 0".getBytes(StandardCharsets.UTF_8)))
                .count();
        assertTrue(replicatedFirstRecords >= 2);
        long replicatedSecondRecords = consumed.stream()
                .filter(record -> Arrays.equals(record.value(), "value 1".getBytes(StandardCharsets.UTF_8)))
                .count();
        assertTrue(replicatedSecondRecords >= 2);
    }

    @Test
    public void testConfigTranslation() throws Exception {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());

        String connector = "connector";

        String topic = "t1";
        srcCluster.createTopic(topic);
        dstCluster.createTopic(topic);

        String targetClientsReceiveBufferBytes = TARGET_CLIENTS_PREFIX + CommonClientConfigs.RECEIVE_BUFFER_CONFIG;
        String targetClientsSendBufferBytes = TARGET_CLIENTS_PREFIX + CommonClientConfigs.SEND_BUFFER_CONFIG;
        String targetProducerLingerMs = TARGET_PRODUCER_PREFIX + ProducerConfig.LINGER_MS_CONFIG;
        String targetProducerSendBufferBytes = TARGET_PRODUCER_PREFIX + CommonClientConfigs.SEND_BUFFER_CONFIG;

        Map<String, String> connectorConfig = baseURep3Config(srcKafkaName, dstKafkaName, 1, Set.of(topic));
        connectorConfig.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers());
        connectorConfig.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        connectorConfig.put(targetClientsReceiveBufferBytes, "33333");
        connectorConfig.put(targetClientsSendBufferBytes, "11111");
        connectorConfig.put(targetProducerLingerMs, "1000");
        connectorConfig.put(targetProducerSendBufferBytes, "22222");

        // Uses the POST endpoint
        // TODO: Flaky (sometimes metadata for the newly-created topic hasn't propagated to every broker)
        //       Unclear if we should handle this by adding retry logic to the test, or if we should add retry logic to the connector itself
        connect.configureConnector(new CreateConnectorRequest(connector, connectorConfig, null));

        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(connector, 1, "Connector and/or task did not start in time");

        Map<String, String> expectedTranslatedConfig = new HashMap<>(connectorConfig);
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                dstCluster.bootstrapServers()
        );
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                "33333"
        );
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + ProducerConfig.LINGER_MS_CONFIG,
                "1000"
        );
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.SEND_BUFFER_CONFIG,
                "22222" // Precedence is given to the target.producer properties over the target.kafka.clients properties
        );
        // Automatically injected by the Connect runtime
        expectedTranslatedConfig.put(NAME_CONFIG, connector);

        Map<String, String> actualTranslatedConfig = connect.connectorInfo(connector).config();
        assertEquals(expectedTranslatedConfig, actualTranslatedConfig);

        // Swap source and target clusters--inadvisable in production, perfectly fine in a tightly-controlled testing environment
        connectorConfig = baseURep3Config(srcKafkaName, dstKafkaName, 1, Set.of(topic));
        connectorConfig.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        connectorConfig.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers());
        connectorConfig.put(targetClientsReceiveBufferBytes, "66666");
        connectorConfig.put(targetClientsSendBufferBytes, "22222");
        connectorConfig.put(targetProducerLingerMs, "2000");
        connectorConfig.put(targetProducerSendBufferBytes, "44444");

        // Uses the PUT endpoint
        connect.configureConnector(connector, connectorConfig);

        expectedTranslatedConfig = new HashMap<>(connectorConfig);
        expectedTranslatedConfig.put(NAME_CONFIG, connector);
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                srcCluster.bootstrapServers()
        );
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                "66666"
        );
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + ProducerConfig.LINGER_MS_CONFIG,
                "2000"
        );
        expectedTranslatedConfig.put(
                CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.SEND_BUFFER_CONFIG,
                "44444"
        );

        actualTranslatedConfig = connect.connectorInfo(connector).config();
        assertEquals(expectedTranslatedConfig, actualTranslatedConfig);

        // Swap source and target back to the correct clusters
        Map<String, String> patch = new HashMap<>();
        patch.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers());
        patch.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        patch.put(targetClientsReceiveBufferBytes, "99999");
        patch.put(targetClientsSendBufferBytes, "33333");
        patch.put(targetProducerLingerMs, "3000");
        patch.put(targetProducerSendBufferBytes, "66666");

        connect.patchConnectorConfig(connector, patch);

        actualTranslatedConfig = connect.connectorInfo(connector).config();
        // Only assert that our patch has gone through
        assertEquals(
                dstCluster.bootstrapServers(),
                actualTranslatedConfig.get(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
        );
        assertEquals(
                srcCluster.bootstrapServers(),
                actualTranslatedConfig.get(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG)
        );
        assertEquals(
                dstCluster.bootstrapServers(),
                actualTranslatedConfig.get(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG)
        );
        assertEquals(
                "99999",
                actualTranslatedConfig.get(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.RECEIVE_BUFFER_CONFIG)
        );
        assertEquals(
                "3000",
                actualTranslatedConfig.get(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + ProducerConfig.LINGER_MS_CONFIG)
        );
        assertEquals(
                "66666",
                actualTranslatedConfig.get(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.SEND_BUFFER_CONFIG)
        );
    }

    @Test
    public void testTopicRenaming() throws Exception {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());

        int numProduced = 0;
        try (Producer<byte[], byte[]> srcProducer = srcCluster.createProducer(Map.of())) {
            for (int i = 1; i <= 6; i++) {
                String srcTopic = "s" + i;
                srcCluster.createTopic(srcTopic, i);
                String dstTopic = "t" + i;
                dstCluster.createTopic(dstTopic, i);

                for (int partition = 0; partition < i; partition++) {
                    produceTestRecord(srcProducer, srcTopic, partition, 0);
                    numProduced++;
                }
            }

            srcProducer.flush();
        }

        String conn1 = "connector-one";
        String conn2 = "connector-two";

        startConnector(conn1, srcKafkaName, dstKafkaName, 3, Set.of("s1", "s2", "s3"), Map.of("s1", "t1", "s2", "t2", "s3", "t3"), false);
        startConnector(conn2, srcKafkaName, dstKafkaName, 1, Set.of("s4", "s5", "s6"), Map.of("s4", "t4", "s5", "t5", "s6", "t6"), false);

        dstCluster.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6");
    }

    @Test
    public void testTasksMax() throws Exception {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());

        String t1 = "t1";
        srcCluster.createTopic(t1, 1);
        dstCluster.createTopic(t1, 1);

        String connector = "connector";

        Map<String, String> connectorConfig = baseURep3Config(srcKafkaName, dstKafkaName, 5, Set.of(t1));
        connectorConfig.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers());
        connectorConfig.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        connectorConfig.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        connect.configureConnector(connector, connectorConfig);
        // Only one partition to replicate; expect only one task, even though tasks.max is set to 5
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                connector, 1, "Connector and/or expected number of tasks did not start in time"
        );

        String t2 = "t2";
        srcCluster.createTopic(t2, 2);
        dstCluster.createTopic(t2, 2);

        connectorConfig.put(TOPICS_CONFIG, String.join(",", List.of(t1, t2)));
        connect.configureConnector(connector, connectorConfig);
        // Three partitions to replicate; expect three tasks, even though tasks.max is set to 5
        stopAndAssertResume(connector, 3);

        String t3 = "t3";
        srcCluster.createTopic(t3, 3);
        dstCluster.createTopic(t3, 3);

        connectorConfig.put(TOPICS_CONFIG, String.join(",", List.of(t1, t2, t3)));
        connect.configureConnector(connector, connectorConfig);
        // Six partitions to replicate; expect five tasks, since tasks.max is set to 5
        stopAndAssertResume(connector, 5);

        connectorConfig.put(TASKS_MAX_CONFIG, "10");
        connectorConfig.put(TOPICS_CONFIG, String.join(",", List.of(t1, t2, t3)));
        connect.configureConnector(connector, connectorConfig);
        // Six partitions to replicate; expect six tasks now
        stopAndAssertResume(connector, 6);

        String t4 = "t4";
        srcCluster.createTopic(t4, 10);
        dstCluster.createTopic(t4, 10);

        connectorConfig.put(TOPICS_CONFIG, String.join(",", List.of(t1, t2, t3, t4)));
        connect.configureConnector(connector, connectorConfig);
        // Ten partitions to replicate and tasks.max is 10; however, only three workers in the cluster--should still only be six tasks (two per worker)
        stopAndAssertResume(connector, 6);

        WorkerHandle newWorker1 = connect.addWorker();
        connect.assertions().assertExactlyNumWorkersAreUp(4, "Additional worker did not start in time");
        connect.restartConnector(connector);
        // Four workers in the cluster--should now be eight tasks (two per worker)
        stopAndAssertResume(connector, 8);

        WorkerHandle newWorker2 = connect.addWorker();
        connect.assertions().assertExactlyNumWorkersAreUp(5, "Additional worker did not start in time");
        connect.restartConnector(connector);
        // Five workers in the cluster--should now be ten tasks (two per worker)
        stopAndAssertResume(connector, 10);

        WorkerHandle newWorker3 = connect.addWorker();
        connect.assertions().assertExactlyNumWorkersAreUp(6, "Additional worker did not start in time");
        connect.restartConnector(connector);
        // Six workers in the cluster, but tasks.max is still only 10
        stopAndAssertResume(connector, 10);

        // Prevent the shutdown of our extra workers from killing the JVM-global workload store
        KafkaBasedWorkloadProvider.destroyOnClose(false);

        newWorker3.stop();
        newWorker2.stop();
        newWorker1.stop();

        KafkaBasedWorkloadProvider.destroyOnClose(true);
    }

    private void stopAndAssertResume(String connector, int expectedRunningTasks) throws InterruptedException {
        connect.stopConnector(connector);
        connect.assertions().assertConnectorIsStopped(connector, "Connector did not stop in time");
        connect.resumeConnector(connector);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(connector, expectedRunningTasks, "Connector and/or tasks did not start back up in time");
    }

    @Test
    public void testPartitionCountTracking() throws Exception {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());

        int numProduced = 0;
        try (Producer<byte[], byte[]> srcProducer = srcCluster.createProducer(Map.of())) {
            for (int i = 1; i <= 6; i++) {
                String topic = "t" + i;
                srcCluster.createTopic(topic, i);
                dstCluster.createTopic(topic, 7 - i);

                for (int partition = 0; partition < i; partition++) {
                    produceTestRecord(srcProducer, topic, partition, 0);
                    numProduced++;
                }
            }

            srcProducer.flush();
        }

        String conn1 = "connector-one";
        String conn2 = "connector-two";

        startURep3Connector(
                conn1,
                srcKafkaName,
                dstKafkaName,
                3,
                Set.of("t1", "t2", "t3"),
                null,
                REFRESH_INTERVAL_MS,
                "1000"
        );
        startURep3Connector(
                conn2,
                srcKafkaName,
                dstKafkaName,
                1,
                Set.of("t4", "t5", "t6"),
                null,
                REFRESH_INTERVAL_MS,
                "1000"
        );

        ConsumerRecords<byte[], byte[]> records = dstCluster.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6");
        // Ensure that every possible target partition was written
        Set<TopicPartition> expectedPartitions = IntStream.rangeClosed(1, 6).boxed().flatMap(i -> {
            String topic = "t" + i;
            int targetPartitions = 7 - i;
            return IntStream.range(0, Math.min(i, targetPartitions))
                    .mapToObj(targetPartition -> new TopicPartition(topic, targetPartition));
        }).collect(Collectors.toSet());
        Set<TopicPartition> actualPartitions = records.partitions();
        assertEquals(expectedPartitions, actualPartitions);

        // Change the partition counts of the source cluster and produce more test traffic
        final int newNumSrcPartitions = 10;
        Map<String, NewPartitions> newPartitions = IntStream.rangeClosed(1, 6)
                .mapToObj(i -> "t" + i)
                .collect(Collectors.toMap(
                        Function.identity(),
                        topic -> NewPartitions.increaseTo(newNumSrcPartitions)
                ));
        try (Admin srcAdmin = srcCluster.createAdminClient()) {
            srcAdmin.createPartitions(newPartitions);
        }
        try (Producer<byte[], byte[]> srcProducer = srcCluster.createProducer(Map.of())) {
            for (int i = 1; i <= 6; i++) {
                String topic = "t" + i;
                for (int partition = 0; partition < newNumSrcPartitions; partition++) {
                    produceTestRecord(srcProducer, topic, partition, 0);
                    numProduced++;
                }
            }

            srcProducer.flush();
        }
        // Make sure all the records we produced (including ones to the newly-created partitions) were replicated
        dstCluster.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6");

        // Change the partition counts--of the target cluster this time
        final int newNumDstPartitions = 8;
        newPartitions = IntStream.rangeClosed(1, 6)
                .mapToObj(i -> "t" + i)
                .collect(Collectors.toMap(
                        Function.identity(),
                        topic -> NewPartitions.increaseTo(newNumDstPartitions)
                ));
        try (Admin dstAdmin = dstCluster.createAdminClient()) {
            dstAdmin.createPartitions(newPartitions);
        }

        Thread.sleep(5_000);
        Set<AssignedPartition> expectedConn1Assignment = IntStream.rangeClosed(1, 3)
                .boxed()
                .flatMap(t -> IntStream.range(0, newNumSrcPartitions)
                        .mapToObj(p -> {
                            String topic = "t" + t;
                            int dstPartition = p % newNumDstPartitions;
                            return new AssignedPartition(
                                    topic,
                                    p,
                                    null,
                                    dstPartition != p ? dstPartition : null
                            );
                        })
                ).collect(Collectors.toSet());
        Set<AssignedPartition> expectedConn2Assignment = IntStream.rangeClosed(4, 6)
                .boxed()
                .flatMap(t -> IntStream.range(0, newNumSrcPartitions)
                        .mapToObj(p -> {
                            String topic = "t" + t;
                            int dstPartition = p % newNumDstPartitions;
                            return new AssignedPartition(
                                    topic,
                                    p,
                                    null,
                                    dstPartition != p ? dstPartition : null
                            );
                        })
                ).collect(Collectors.toSet());

        // Wait for the connectors to pick up on the partition count changes
        waitForCondition(() -> {
            Set<AssignedPartition> actualConn1Assignment = getAllAssignedPartitions(conn1);
            assertEquals(expectedConn1Assignment, actualConn1Assignment);

            Set<AssignedPartition> actualConn2Assignment = getAllAssignedPartitions(conn2);
            assertEquals(expectedConn2Assignment, actualConn2Assignment);
            return true;
        }, TimeUnit.MINUTES.toMillis(1), "Connector did not generate new assignments in response to target partition count change in time");

        // Force task restarts to guarantee that the new assignments have been picked up
        connect.restartConnectorAndTasks(conn1, false, true, false);
        connect.restartConnectorAndTasks(conn2, false, true, false);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(conn1, 3, "Connector and/or tasks did not restart in time");
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(conn2, 1, "Connector and/or tasks did not restart in time");

        // Produce even more test traffic
        try (Producer<byte[], byte[]> srcProducer = srcCluster.createProducer(Map.of())) {
            for (int i = 1; i <= 6; i++) {
                String topic = "t" + i;
                for (int partition = 0; partition < newNumSrcPartitions; partition++) {
                    produceTestRecord(srcProducer, topic, partition, 0);
                    numProduced++;
                }
            }

            srcProducer.flush();
        }

        records = dstCluster.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6");
        // Ensure that every possible target partition was written
        expectedPartitions = IntStream.rangeClosed(1, 6).boxed().flatMap(i -> {
            String topic = "t" + i;
            return IntStream.range(0, newNumSrcPartitions)
                    .mapToObj(targetPartition -> new TopicPartition(topic, targetPartition % newNumDstPartitions));
        }).collect(Collectors.toSet());
        actualPartitions = records.partitions();
        assertEquals(expectedPartitions, actualPartitions);
    }

    @Test
    public void testPreflightValidation() {
        String srcKafkaName = "src-kafka";
        String dstKafkaName = "dst-kafka";
        String srcTopic = "src-topic";
        String dstTopic = "dst-topic";

        Map<String, String> connectorProps = baseURep3Config(srcKafkaName, dstKafkaName, 1, Set.of(srcTopic));
        connectorProps.put(NAME_CONFIG, "connector-validation-test");
        // Simulate unreachable src/dst clusters
        connectorProps.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, "bad-host-src:9092");
        connectorProps.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, "bad-host-dst:9092");
        validateURep3Config(connectorProps,
                // Neither of the clusters is available
                SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG,
                TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG
        );

        EmbeddedKafkaCluster srcCluster = extraKafkaCluster(srcKafkaName, 1, new Properties());
        connectorProps.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers());
        validateURep3Config(connectorProps,
                // Only the source cluster is available; target cluster is still down
                TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG,
                // Topic hasn't been created on the source cluster yet
                TOPICS_CONFIG
        );

        srcCluster.createTopic(srcTopic);
        validateURep3Config(connectorProps,
                // Only the source cluster is available; target cluster is still down
                TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG
        );

        EmbeddedKafkaCluster dstCluster = extraKafkaCluster(dstKafkaName, 1, new Properties());
        connectorProps.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        validateURep3Config(connectorProps,
                // Both clusters available now, but the topic hasn't been created on the target cluster yet
                TOPICS_CONFIG
        );

        dstCluster.createTopic(srcTopic);
        validateURep3Config(connectorProps);

        String renamedTopicProp = TOPIC_RENAME_PREFIX + srcTopic;
        connectorProps.put(renamedTopicProp, dstTopic);
        validateURep3Config(connectorProps,
                // Renamed topic doesn't exist on target cluster yet
                renamedTopicProp
        );

        dstCluster.createTopic(dstTopic);
        validateURep3Config(connectorProps); // Everything should work now

        // Can't override individual client bootstrap servers
        String srcAdminBootstrapProp = SOURCE_ADMIN_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        connectorProps.put(srcAdminBootstrapProp, srcCluster.bootstrapServers());
        String dstAdminBootstrapProp = TARGET_ADMIN_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        connectorProps.put(dstAdminBootstrapProp, dstCluster.bootstrapServers());
        String srcConsumerBootstrapProp = SOURCE_CONSUMER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        connectorProps.put(srcConsumerBootstrapProp, srcCluster.bootstrapServers());
        validateURep3Config(connectorProps,
                srcAdminBootstrapProp,
                dstAdminBootstrapProp,
                srcConsumerBootstrapProp);

        connectorProps.keySet().removeAll(Set.of(srcAdminBootstrapProp, dstAdminBootstrapProp, srcConsumerBootstrapProp));
        String emptyRenamedSrcTopicProp = TOPIC_RENAME_PREFIX;
        connectorProps.put(emptyRenamedSrcTopicProp, "some-dst-topic");
        String blankRenamedSrcTopicProp = TOPIC_RENAME_PREFIX + "   ";
        connectorProps.put(blankRenamedSrcTopicProp, "some-dst-topic");
        String emptyRenamedDstTopicProp = TOPIC_RENAME_PREFIX + "some-src-topic-1";
        connectorProps.put(emptyRenamedDstTopicProp, "");
        String blankRenamedDstTopicProp = TOPIC_RENAME_PREFIX + "some-src-topic-2";
        connectorProps.put(blankRenamedDstTopicProp, "   ");
        validateURep3Config(connectorProps,
                emptyRenamedSrcTopicProp,
                blankRenamedSrcTopicProp,
                emptyRenamedDstTopicProp,
                blankRenamedDstTopicProp
        );
    }

    @Test
    public void testWorkloadTracking() throws Exception {
        String srcKafka1Name = "src-kafka";
        String srcKafka2Name = "src-kafka-2";
        String dstKafka1Name = "dst-kafka";
        String dstKafka2Name = "dst-kafka-2";

        EmbeddedKafkaCluster srcCluster1 = extraKafkaCluster(srcKafka1Name, 1, new Properties());
        EmbeddedKafkaCluster srcCluster2 = extraKafkaCluster(srcKafka2Name, 1, new Properties());
        EmbeddedKafkaCluster dstCluster1 = extraKafkaCluster(dstKafka1Name, 1, new Properties());
        EmbeddedKafkaCluster dstCluster2 = extraKafkaCluster(dstKafka2Name, 1, new Properties());

        IntStream.rangeClosed(1, 7).forEach(i -> {
            String topic = "t" + i;
            // i = number of partitions in each topic
            Stream.of(srcCluster1, srcCluster2, dstCluster1, dstCluster2).forEach(
                    cluster -> cluster.createTopic(topic, i)
            );
        });

        String conn1 = "connector-one";
        String conn2 = "connector-two";
        String conn3 = "connector-three";
        String conn4 = "connector-four";

        startConnector(conn1, srcKafka1Name, dstKafka1Name, 6, Set.of("t1", "t2", "t3", "t4", "t5", "t6", "t7"), null, true);
        startConnector(conn2, srcKafka1Name, dstKafka2Name, 6, Set.of("t1", "t2", "t3", "t4", "t5", "t6", "t7"), null, true);
        // Only replicate a subset of topics from src 2
        startConnector(conn3, srcKafka2Name, dstKafka1Name, 6, Set.of("t1", "t3", "t4", "t6", "t7"), null, true);
        startConnector(conn4, srcKafka2Name, dstKafka2Name, 6, Set.of("t1", "t3", "t4", "t6", "t7"), null, true);

        int numProduced = 0;
        try (Producer<byte[], byte[]> src1Producer = srcCluster1.createProducer(Map.of())) {
            for (int t = 1; t <= 3; t++) {
                String topic = "t" + t;
                for (int i = 0; i < 1000; i++) {
                    int partition = i % t;
                    produceTestRecord(src1Producer, topic, partition, i);
                    numProduced++;
                }
            }
            for (int t = 4; t <= 6; t++) {
                String topic = "t" + t;
                for (int i = 0; i < 100; i++) {
                    int partition = i % t;
                    produceTestRecord(src1Producer, topic, partition, i);
                    numProduced++;
                }
            }
            // Intentionally refrain from producing to t7 to verify that empty partitions are still tracked
            src1Producer.flush();
        }

        try (Producer<byte[], byte[]> src2Producer = srcCluster2.createProducer(Map.of())) {
            for (int t : Set.of(1, 3)) {
                String topic = "t" + t;
                for (int i = 0; i < 100; i++) {
                    int partition = i % t;
                    produceTestRecord(src2Producer, topic, partition, i);
                    numProduced++;
                }
            }
            for (int t : Set.of(4, 6)) {
                String topic = "t" + t;
                for (int i = 0; i < 1000; i++) {
                    int partition = i % t;
                    produceTestRecord(src2Producer, topic, partition, i);
                    numProduced++;
                }
            }
            // Intentionally refrain from producing to t7 to verify that empty partitions are still tracked
            src2Producer.flush();
        }

        // Ensure that the same number of records produced to both of the source clusters is available on both of the destination clusters
        dstCluster1.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6", "t7");
        dstCluster2.consume(numProduced, TimeUnit.MINUTES.toMillis(1), "t1", "t2", "t3", "t4", "t5", "t6", "t7");

        // Have to potentially retry here since tasks aren't guaranteed to immediately publish workloads
        waitForCondition(() -> {
            Map<ReplicatedTopicPartition, Workload> trackedWorkloads = getAllWorkloads();

            Set<ReplicatedTopicPartition> expectedReplicatedTopicPartitions = new HashSet<>();
            for (int i = 1; i <= 7; i++) {
                String topic = "t" + i;
                for (int partition = 0; partition < i; partition++) {
                    ReplicatedTopicPartition tpSrc1Dst1 = new ReplicatedTopicPartition(srcKafka1Name, dstKafka1Name, topic, partition);
                    expectedReplicatedTopicPartitions.add(tpSrc1Dst1);
                    ReplicatedTopicPartition tpSrc1Dst2 = new ReplicatedTopicPartition(srcKafka1Name, dstKafka2Name, topic, partition);
                    expectedReplicatedTopicPartitions.add(tpSrc1Dst2);

                    if (i != 2 && i != 5) {
                        ReplicatedTopicPartition tpSrc2Dst1 = new ReplicatedTopicPartition(srcKafka2Name, dstKafka1Name, topic, partition);
                        expectedReplicatedTopicPartitions.add(tpSrc2Dst1);
                        ReplicatedTopicPartition tpSrc2Dst2 = new ReplicatedTopicPartition(srcKafka2Name, dstKafka2Name, topic, partition);
                        expectedReplicatedTopicPartitions.add(tpSrc2Dst2);
                    }
                }
            }
            assertEquals(expectedReplicatedTopicPartitions, trackedWorkloads.keySet());

            trackedWorkloads.forEach((tp, workload) -> {
                if ("t7".equals(tp.topic())) {
                    assertEquals(0, workload.getMessagesPerSecond());
                    assertEquals(0, workload.getBytesPerSecond());
                } else {
                    assertNotEquals(0, workload.getMessagesPerSecond(), "empty message rate found for " + tp);

                    double expectedBytesPerSecond = workload.getMessagesPerSecond() * 10; // Values are always 7 bytes, keys are always 3
                    double delta = 0.1;
                    assertEquals(expectedBytesPerSecond, workload.getBytesPerSecond(), delta);
                }
            });

            Map<ReplicatedTopicPartition, Workload> heavyWorkloads = trackedWorkloads.entrySet().stream()
                    .filter(e -> {
                        String topic = e.getKey().topic();
                        String srcKafka = e.getKey().srcKafka();
                        return switch (topic) {
                            case "t1", "t2", "t3" -> srcKafka1Name.equals(srcKafka);
                            case "t4", "t6" -> srcKafka2Name.equals(srcKafka);
                            default -> false;
                        };
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Map<ReplicatedTopicPartition, Workload> lightWorkloads = trackedWorkloads.entrySet().stream()
                    .filter(e -> !heavyWorkloads.containsKey(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            heavyWorkloads.forEach((heavyTp, heavyLoad) ->
                    lightWorkloads.forEach((lightTp, lightLoad) -> {
                        assertTrue(heavyLoad.getBytesPerSecond() > lightLoad.getBytesPerSecond(), "workload for " + heavyTp + " should be larger than for " + lightTp);
                        assertTrue(heavyLoad.getMessagesPerSecond() > lightLoad.getMessagesPerSecond(), "workload for " + heavyTp + " should be larger than for " + lightTp);
                    })
            );
        return true;
        }, TimeUnit.SECONDS.toMillis(10), "");

        Map<String, Workload> workerWorkloads = getWorkerWorkloads();
        assertEquals(NUM_WORKERS, workerWorkloads.size());
        workerWorkloads.forEach((worker, workload) ->
            assertNotEquals(Workload.EMPTY, workload, "Worker " + worker + " has empty workload")
        );
    }

    private EmbeddedKafkaCluster extraKafkaCluster(String name, int numBrokers, Properties brokerProperties) {
        return EXTRA_KAFKA_CLUSTERS.computeIfAbsent(name, n -> {
            log.info("Starting embedded Kafka cluster {}", name);
            EmbeddedKafkaCluster result = new EmbeddedKafkaCluster(numBrokers, brokerProperties);
            result.start();
            log.info("Finished starting embedded Kafka cluster {}", name);
            return result;
        });
    }

    private void startConnector(
            String connectorName,
            String srcKafkaName,
            String dstKafkaName,
            int tasksMax,
            Set<String> topics,
            Map<String, String> topicRenames,
            boolean workloadTracking
    ) throws InterruptedException {
        if (workloadTracking)
            startURep3ConnectorWithWorkloadTracking(connectorName, srcKafkaName, dstKafkaName, tasksMax, topics, topicRenames);
        else
            startURep3Connector(connectorName, srcKafkaName, dstKafkaName, tasksMax, topics, topicRenames);
    }

    private void startURep3ConnectorWithWorkloadTracking(
            String connectorName,
            String srcKafkaName,
            String dstKafkaName,
            int tasksMax,
            Set<String> topics,
            Map<String, String> topicRenames
    ) throws InterruptedException {
        startURep3Connector(
                connectorName,
                srcKafkaName,
                dstKafkaName,
                tasksMax,
                topics,
                topicRenames,
                WORKLOAD_REPORT_INTERVAL_MS_CONFIG, "1000" // Force rapid reporting so that we don't have to wait forever to verify workloads
        );
    }

    private void startURep3Connector(
            String connectorName,
            String srcKafkaName,
            String dstKafkaName,
            int tasksMax,
            Set<String> topics,
            Map<String, String> topicRenames,
            String... extraProps
    ) throws InterruptedException {
        assertEquals(0, extraProps.length % 2, "Extra props should be given as a series of key and value pairs");
        EmbeddedKafkaCluster srcCluster = EXTRA_KAFKA_CLUSTERS.get(srcKafkaName);
        assertNotNull(srcCluster, "No Kafka cluster found with name '" + srcKafkaName + "'");

        EmbeddedKafkaCluster dstCluster = EXTRA_KAFKA_CLUSTERS.get(dstKafkaName);
        assertNotNull(dstCluster, "No Kafka cluster found with name '" + dstKafkaName + "'");

        Map<String, String> connectorProps = baseURep3Config(srcKafkaName, dstKafkaName, tasksMax, topics);
        connectorProps.put(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers());
        connectorProps.put(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());
        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers());

        if (topicRenames != null) {
            topicRenames.forEach((srcTopic, dstTopic) ->
                    connectorProps.put(TOPIC_RENAME_PREFIX + srcTopic, dstTopic)
            );
        }

        for (int i = 0; i < extraProps.length; i += 2)
            connectorProps.put(extraProps[i], extraProps[i + 1]);

        log.info("Starting connector {}", connectorName);
        connect.configureConnector(connectorName, connectorProps);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(connectorName, tasksMax, "Connector and/or tasks did not start in time");
        log.info("Finished starting connector {}", connectorName);
    }

    private Map<String, String> baseURep3Config(String srcKafkaName, String dstKafkaName, int tasksMax, Set<String> topics) {
        Map<String, String> result = new HashMap<>();
        result.put(CONNECTOR_CLASS_CONFIG, UReplicator3Connector.class.getName());
        result.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));
        result.put(SOURCE_CLUSTER_CONFIG, srcKafkaName);
        result.put(TARGET_CLUSTER_CONFIG, dstKafkaName);
        result.put(TOPICS_CONFIG, String.join(",", topics));
        result.put(WORKLOAD_REPORT_INTERVAL_MS_CONFIG, "-1"); // Disable workload publishing by default
        return result;
    }

    private void validateURep3Config(Map<String, String> config, String... expectedErrorProps) {
        Set<String> actualErrorProps = validateConnectorConfig(UReplicator3Connector.class.getCanonicalName(), config).keySet();
        assertEquals(Set.of(expectedErrorProps), actualErrorProps);
    }

    private Map<String, List<String>> validateConnectorConfig(String connectorClass, Map<String, String> config) {
        ConfigInfos validationResult = connect.validateConnectorConfig(connectorClass, config);
        return validationResult.configs().stream()
                .map(ConfigInfo::configValue)
                .filter(configValue -> !configValue.errors().isEmpty())
                .collect(Collectors.toMap(
                        ConfigValueInfo::name,
                        ConfigValueInfo::errors
                ));
    }

    private void produceTestRecord(Producer<byte[], byte[]> producer, String topic, int partition, int iteration) {
        byte[] keyBytes = "key".getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = ("value " + (iteration % 10)).getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, partition, keyBytes, valueBytes);
        producer.send(record);
    }

    private void manuallyPublishWorkload(ReplicatedTopicPartition rtp, Workload w) throws IOException {
        String resource = String.format(
                "uber/workloads/topics/overrides/%s/%s/%s/%d",
                rtp.srcKafka(), rtp.dstKafka(), rtp.topic(), rtp.partition()
        );
        WorkloadEntity entity = WorkloadEntity.of(w);
        String body = OBJECT_MAPPER.writeValueAsString(entity);
        Response response = connect.requestPost(connect.endpointForResource(resource), body, Map.of());
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    private void manuallyDeleteWorkload(ReplicatedTopicPartition rtp) {
        String resource = String.format(
                "uber/workloads/topics/overrides/%s/%s/%s/%d",
                rtp.srcKafka(), rtp.dstKafka(), rtp.topic(), rtp.partition()
        );
        Response response = connect.requestDelete(connect.endpointForResource(resource));
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    private Map<ReplicatedTopicPartition, Workload> getAllWorkloads() throws IOException {
        Response response = connect.requestGet(connect.endpointForResource("uber/workloads/topics/all?refresh=true"));
        String entity = (String) response.getEntity();

        return OBJECT_MAPPER
                .readerFor(new TypeReference<List<PartitionWorkloadEntity>>() {})
                .<List<PartitionWorkloadEntity>>readValue(entity)
                .stream()
                .collect(Collectors.toMap(
                        PartitionWorkloadEntity::replicatedTopicPartition,
                        PartitionWorkloadEntity::workload
                ));
    }

    private Map<String, Workload> getWorkerWorkloads() throws IOException {
        Response response = connect.requestGet(connect.endpointForResource("uber/workloads/workers/all?refresh=true"));
        String entity = (String) response.getEntity();

        return OBJECT_MAPPER
                .readerFor(new TypeReference<List<WorkerWorkloadEntity>>() {})
                .<List<WorkerWorkloadEntity>>readValue(entity)
                .stream()
                .collect(Collectors.toMap(
                        WorkerWorkloadEntity::worker,
                        w -> w.totalWorkload().workload()
                ));
    }

    private Set<AssignedPartition> getAllAssignedPartitions(String connector) throws IOException {
        Response response = connect.requestGet(connect.endpointForResource("uber/assignments/" + connector + "/all"));
        String entity = (String) response.getEntity();

        return OBJECT_MAPPER
                .readerFor(new TypeReference<List<TaskAssignmentEntity>>() {})
                .<List<TaskAssignmentEntity>>readValue(entity)
                .stream()
                .map(TaskAssignmentEntity::partitions)
                .flatMap(List::stream)
                .map(AssignedPartitionEntity::assignedPartition)
                .collect(Collectors.toSet());
    }
}
