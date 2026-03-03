package com.uber.data.kafka.connect.distributed.assignment;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import com.uber.data.kafka.connect.workload.common.WorkloadProvider;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class UberClusterAssignorConfig extends AbstractConfig {

    // TODO: Does this still need to be configurable?
    public static final String UBER_WORKLOAD_PROVIDER_CONFIG = "uber.workload.provider";
    public static final String UBER_WORKLOAD_PROVIDER_DOC = "Implementation of the "
            + WorkloadProvider.class.getName() + " interface that will be used to determine the total workload "
            + "of each worker. If left unset, then the default Kafka Connect rebalancing algorithm will be used instead, "
            + "which does not take workload into account and weighs all tasks equally";

    // TODO: This can be moved to Flipr
    public static final String UBER_WORKER_LOAD_THRESHOLD_CONFIG = "uber.worker.load.threshold";
    public static final double UBER_WORKER_LOAD_THRESHOLD_DEFAULT = 1;
    public static final String UBER_WORKER_LOAD_THRESHOLD_DOC = "The maximum total load "
            + "that a single worker should be assigned during rebalance. This will be enforced on a best-effort basis; "
            + "in some cases (such as if the cluster is underprovisioned), this threshold will be breached instead of "
            + "leaving tasks unassigned";

    // TODO: Flipr
    public static final String UBER_DEFAULT_TASK_LOAD_CONFIG = "uber.default.task.load";
    public static final double UBER_DEFAULT_TASK_LOAD_DEFAULT = 0.1; // Allow up to 10 unknown tasks per worker
    public static final String UBER_DEFAULT_TASK_LOAD_DOC = "The default load to assume "
            + "for tasks that do not have a known load according to the configured workload provider";

    // TODO: Flipr
    public static final String UBER_PERIODIC_REBALANCE_INTERVAL_MS_CONFIG = "uber.periodic.rebalance.interval.ms";
    public static final int UBER_PERIODIC_REBALANCE_INTERVAL_MS_DEFAULT = (int) TimeUnit.MINUTES.toMillis(5);
    public static final String UBER_PERIODIC_REBALANCE_INTERVAL_MS_DOC = "How frequently to trigger a "
            + "cluster rebalance, even if there has been no change in the number of workers, connectors, or tasks. "
            + "This can be used to respond to changes in traffic and balance workloads across workers. "
            + "Set to zero or a negative value to disable periodic rebalancing";

    private final WorkloadProvider workloadProvider;

    @SuppressWarnings("this-escape")
    public UberClusterAssignorConfig(Map<?, ?> props) {
        super(config(), props);
        WorkloadProvider configuredWorkloadProvider = getConfiguredInstance(UBER_WORKLOAD_PROVIDER_CONFIG, WorkloadProvider.class);
        this.workloadProvider = configuredWorkloadProvider != null ? configuredWorkloadProvider : WorkloadProvider.noOp();
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(UBER_WORKLOAD_PROVIDER_CONFIG,
                        ConfigDef.Type.CLASS,
                        null, // Null instead of ConfigDef.NO_DEFAULT_VALUE to signify that the property is optional
                        ConfigDef.Importance.HIGH,
                        UBER_WORKLOAD_PROVIDER_DOC)
                .define(UBER_WORKER_LOAD_THRESHOLD_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        UBER_WORKER_LOAD_THRESHOLD_DEFAULT,
                        atLeast(0),
                        Importance.MEDIUM,
                        UBER_WORKER_LOAD_THRESHOLD_DOC)
                .define(UBER_DEFAULT_TASK_LOAD_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        UBER_DEFAULT_TASK_LOAD_DEFAULT,
                        atLeast(0),
                        Importance.MEDIUM,
                        UBER_DEFAULT_TASK_LOAD_DOC
                ).define(UBER_PERIODIC_REBALANCE_INTERVAL_MS_CONFIG,
                        Type.INT,
                        UBER_PERIODIC_REBALANCE_INTERVAL_MS_DEFAULT,
                        Importance.MEDIUM,
                        UBER_PERIODIC_REBALANCE_INTERVAL_MS_DOC
                );
    }

    public WorkloadProvider workloadProvider() {
        return workloadProvider;
    }

    public double uberWorkerLoadThreshold() {
        return getDouble(UBER_WORKER_LOAD_THRESHOLD_CONFIG);
    }

    public double uberDefaultTaskLoad() {
        return getDouble(UBER_DEFAULT_TASK_LOAD_CONFIG);
    }

    public int uberPeriodicRebalanceIntervalMs() {
        return getInt(UBER_PERIODIC_REBALANCE_INTERVAL_MS_CONFIG);
    }

}
