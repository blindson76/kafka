package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.runtime.ConnectorConfig;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.connect.utils.BrokerResolver;
import com.uber.data.kafka.exception.ResolverException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.uber.data.kafka.connect.utils.UberConnectUtils.createErrorMessage;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public abstract class UReplicator3Config extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(UReplicator3Config.class);

    public static final String SOURCE_CLUSTER_CONFIG = "source.cluster";
    private static final String SOURCE_CLUSTER_DOC = "Name of source cluster to consume from (e.g., phx2-dev1)";

    public static final String TARGET_CLUSTER_CONFIG = "target.cluster";
    private static final String TARGET_CLUSTER_DOC = "Name of target cluster to produce to (e.g., dca1-dev1)";

    public static final String CONSUMER_POLL_TIMEOUT_MS_CONFIG = "consumer.poll.timeout.ms";
    public static final long CONSUMER_POLL_TIMEOUT_MS_DEFAULT = 1000;
    private static final String CONSUMER_POLL_TIMEOUT_MS_DOC = "The timeout to use when invoking Consumer::poll";

    public static final String CLIENTS_PREFIX = "all.kafka.clients.";
    public static final String SOURCE_CLIENTS_PREFIX = "source.kafka.clients.";
    public static final String TARGET_CLIENTS_PREFIX = "target.kafka.clients.";
    public static final String SOURCE_CONSUMER_PREFIX = "source.consumer.";
    // This prefix controls "phantom" properties: ones not actually used by our connector code
    // but instead, picked up by our REST extension and translated into properties recognized by
    // the Connect runtime (i.e., "producer.override.<property>")
    public static final String TARGET_PRODUCER_PREFIX = "target.producer.";
    public static final String SOURCE_ADMIN_PREFIX = "source.admin.";
    public static final String TARGET_ADMIN_PREFIX = "target.admin.";

    public static final String WORKLOAD_REPORT_INTERVAL_MS_CONFIG = "workload.report.interval.ms";
    private static final long WORKLOAD_REPORT_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);

    public static final String OFFSET_REPORTER_ENABLED_CONFIG = "offset.reporter.enabled";
    private static final boolean OFFSET_REPORTER_ENABLED_DEFAULT = true;

    public static final String SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG = SOURCE_CLIENTS_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    public static final String TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG = TARGET_CLIENTS_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    @SuppressWarnings("this-escape")
    protected UReplicator3Config(ConfigDef config, Map<String, ?> props) {
        super(config, props);
    }

    protected static ConfigDef baseConfig() {
        return new ConfigDef()
                .define(
                        SOURCE_CLUSTER_CONFIG,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        SOURCE_CLUSTER_DOC
                ).define(
                        TARGET_CLUSTER_CONFIG,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        TARGET_CLUSTER_DOC
                ).define(
                        CONSUMER_POLL_TIMEOUT_MS_CONFIG,
                        Type.LONG,
                        CONSUMER_POLL_TIMEOUT_MS_DEFAULT,
                        atLeast(1),
                        Importance.MEDIUM,
                        CONSUMER_POLL_TIMEOUT_MS_DOC
                ).defineInternal(
                        WORKLOAD_REPORT_INTERVAL_MS_CONFIG,
                        Type.LONG,
                        WORKLOAD_REPORT_INTERVAL_MS_DEFAULT,
                        Importance.LOW
                ).defineInternal(
                        OFFSET_REPORTER_ENABLED_CONFIG,
                        Type.BOOLEAN,
                        OFFSET_REPORTER_ENABLED_DEFAULT,
                        Importance.LOW
                ).defineInternal(
                        ConnectorConfig.NAME_CONFIG,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.LOW
                );
    }

    protected abstract String role();

    public String sourceCluster() {
        return getString(SOURCE_CLUSTER_CONFIG);
    }

    public String targetCluster() {
        return getString(TARGET_CLUSTER_CONFIG);
    }

    public Duration consumerPollTimeout() {
        long timeoutMs = getLong(CONSUMER_POLL_TIMEOUT_MS_CONFIG);
        return Duration.ofMillis(timeoutMs);
    }

    public String connectorName() {
        return getString(ConnectorConfig.NAME_CONFIG);
    }

    public Duration workloadReportInterval() {
        Long durationMs = getLong(WORKLOAD_REPORT_INTERVAL_MS_CONFIG);
        return durationMs == null || durationMs <= 0
                ? null
                : Duration.ofMillis(durationMs);
    }

    public boolean offsetReporterEnabled() {
        return getBoolean(OFFSET_REPORTER_ENABLED_CONFIG);
    }

    public Admin sourceAdmin() {
        return admin(
                sourceBootstrap(),
                clientId(true, "admin"),
                CLIENTS_PREFIX,
                SOURCE_CLIENTS_PREFIX,
                SOURCE_ADMIN_PREFIX
        );
    }

    public Admin targetAdmin() {
        return admin(
                targetBootstrap(),
                clientId(false, "admin"),
                CLIENTS_PREFIX,
                TARGET_CLIENTS_PREFIX,
                TARGET_ADMIN_PREFIX
        );
    }

    public Consumer<byte[], byte[]> sourceConsumer() {
        return consumer(
                sourceBootstrap(),
                clientId(true, "consumer"),
                CLIENTS_PREFIX,
                SOURCE_CLIENTS_PREFIX,
                SOURCE_CONSUMER_PREFIX
        );
    }

    private String sourceBootstrap() {
        String override = sourceBootstrapOverride();
        if (override != null) {
            log.info("Using overridden bootstrap servers for source cluster: {}", override);
            return override;
        }

        return resolveBootstrap(SOURCE_CLUSTER_CONFIG, sourceCluster());
    }

    protected String sourceBootstrapOverride() {
        return originalsStrings().get(SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG);
    }

    private String targetBootstrap() {
        String override = targetBootstrapOverride();
        if (override != null) {
            log.info("Using overridden bootstrap servers for target cluster: {}", override);
            return override;
        }

        return resolveBootstrap(TARGET_CLUSTER_CONFIG, targetCluster());
    }

    protected String targetBootstrapOverride() {
        return originalsStrings().get(TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG);
    }

    // TODO: Maybe align this with the IDs assigned by the Connect runtime to the
    //       clients it instantiates on behalf of connectors and tasks?
    private String clientId(boolean source, String clientType) {
        return "ureplicator3-%s-%s-%s".formatted(
                role(),
                source ? "source" : "target",
                clientType
        );
    }

    private Admin admin(String bootstrapServers, String clientId, String... overridesPrefixes) {
        Map<String, Object> adminProps = new HashMap<>(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                CommonClientConfigs.CLIENT_ID_CONFIG, clientId
        ));

        for (String overridesPrefix : overridesPrefixes)
            adminProps.putAll(originalsWithPrefix(overridesPrefix));

        return Admin.create(adminProps);
    }

    private Consumer<byte[], byte[]> consumer(String bootstrapServers, String clientId, String... overridesPrefixes) {
        Map<String, Object> consumerProps = new HashMap<>(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                CommonClientConfigs.CLIENT_ID_CONFIG, clientId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        ));

        for (String overridesPrefix : overridesPrefixes)
            consumerProps.putAll(originalsWithPrefix(overridesPrefix));

        return new KafkaConsumer<>(consumerProps);
    }

    @VisibleForTesting
    static String resolveBootstrap(String prop, String cluster) {
        try {
            return BrokerResolver.resolveBootstrap(cluster);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(
                    "Failed to initialize broker resolver. If testing locally, manually specify the bootstrap servers "
                            + "for the source and target cluster using the " + SOURCE_CLUSTER_BOOTSTRAP_SERVERS_CONFIG + " "
                            + "and " + TARGET_CLUSTER_BOOTSTRAP_SERVERS_CONFIG + " properties"
            );
        } catch (ResolverException e) {
            log.error("Failed to resolve bootstrap servers for {}", cluster, e);
            String errorMessage = createErrorMessage("failed to resolve bootstrap servers for cluster", e);
            throw new ConfigException(prop, cluster, errorMessage);
        }
    }

}
