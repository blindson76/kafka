package com.uber.data.kafka.connect.ureplicator3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UReplicator3TaskConfig extends UReplicator3Config {

    private static final Logger log = LoggerFactory.getLogger(UReplicator3TaskConfig.class);

    public static final String TASK_ID_CONFIG = "task.id";

    @SuppressWarnings("this-escape")
    public UReplicator3TaskConfig(Map<String, ?> props) {
        super(config(), props);
    }

    protected static ConfigDef config() {
        return baseConfig()
                .defineInternal(
                        TASK_ID_CONFIG,
                        Type.INT,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH
                );
    }

    @Override
    protected String role() {
        return "task-" + connectorName() + "-" + taskId();
    }

    public int taskId() {
        return getInt(TASK_ID_CONFIG);
    }

}
