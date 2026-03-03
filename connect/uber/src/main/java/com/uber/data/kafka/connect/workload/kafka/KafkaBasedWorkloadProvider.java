package com.uber.data.kafka.connect.workload.kafka;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;

import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStores;
import com.uber.data.kafka.connect.workload.common.CachedWorkload;
import com.uber.data.kafka.connect.workload.common.Workload;
import com.uber.data.kafka.connect.workload.common.WorkloadProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.data.kafka.connect.utils.UberConnectUtils.assignedPartitions;

public class KafkaBasedWorkloadProvider implements WorkloadProvider {

    private static final AtomicBoolean DESTROY_ON_CLOSE = new AtomicBoolean(false);
    private static final Logger log = LoggerFactory.getLogger(KafkaBasedWorkloadProvider.class);

    private static KafkaBasedWorkloadStore workloadStore;

    private KafkaBasedWorkloadProviderConfig config;

    @Override
    public void configure(Map<String, ?> props) {
        log.info("Starting provider");
        config = new KafkaBasedWorkloadProviderConfig(props);
        initializeStore(config);
        AssignmentStores.initialize(config.assignmentStoreConfig());
        log.info("Finished Starting provider");
    }

    @Override
    public Double taskLoad(String connector, int taskId, Map<String, String> connectorConfig, Map<String, String> taskConfig) {
        Set<ReplicatedTopicPartition> replicatedTopicPartitions = assignedPartitions(connector, taskId, connectorConfig);
        if (replicatedTopicPartitions == null) {
            return null;
        }

        return replicatedTopicPartitions.stream()
                .map(rtp -> workloadStore.getWorkload(rtp.srcKafka(), rtp.dstKafka(), rtp.topicPartition()))
                .map(w -> w != null ? w : config.defaultPartitionWorkload())
                .mapToDouble(this::scaleWorkload)
                .sum();
    }

    @Override
    public void close() {
        if (!DESTROY_ON_CLOSE.get()) {
            log.debug("Skipping teardown of workload store on close; this should only be done in testing environments");
            return;
        }
        log.info("Shutting down provider");
        destroyStore();
        log.info("Finished shutting down provider");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    public static Future<?> publishWorkload(String srcKafka, String dstKafka, TopicPartition topicPartition, Workload workload) {
        assertStoreInitialized();

        return workloadStore.putWorkload(srcKafka, dstKafka, topicPartition, workload);
    }

    public static void refreshWorkloadCache() {
        assertStoreInitialized();

        AssignmentStores.get().refresh();
        workloadStore.refresh();
    }

    public static Map<ReplicatedTopicPartition, Workload> getAllWorkloads() {
        assertStoreInitialized();

        return workloadStore.getAllWorkloads();
    }

    public static List<CachedWorkload> getHistoricalWorkloads(String srcKafka, String dstKafka, TopicPartition topicPartition) {
        assertStoreInitialized();

        return workloadStore.getHistoricalWorkloads(srcKafka, dstKafka, topicPartition);
    }

    public static void clearCache() {
        assertStoreInitialized();

        workloadStore.clear();
    }

    public static void assertStoreInitialized() throws IllegalStateException {
        if (workloadStore == null)
            throw new IllegalStateException("Workload store has not been initialized yet, or has already been shut down");
    }

    public static void destroyOnClose(boolean destroy) {
        DESTROY_ON_CLOSE.set(destroy);
    }

    private static synchronized void initializeStore(KafkaBasedWorkloadProviderConfig config) {
        if (workloadStore != null) {
            log.warn("Provider appears to have been configured multiple times on the same JVM. This should only happen during testing");
            return;
        }
        workloadStore = new KafkaBasedWorkloadStore(config);
        workloadStore.start();
    }

    private static synchronized void destroyStore() {
        Utils.closeQuietly(workloadStore, "workload store");
        workloadStore = null;
    }

    private double scaleWorkload(Workload taskWorkload) {
        double byteRateLoad = taskWorkload.getBytesPerSecond() / config.maxWorkerWorkload().getBytesPerSecond();
        double messageRateLoad = taskWorkload.getMessagesPerSecond() / config.maxWorkerWorkload().getMessagesPerSecond();
        return Math.max(byteRateLoad, messageRateLoad);
    }

}
