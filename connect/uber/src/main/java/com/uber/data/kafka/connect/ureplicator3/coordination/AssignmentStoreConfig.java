package com.uber.data.kafka.connect.ureplicator3.coordination;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.TopicConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class AssignmentStoreConfig extends AbstractConfig {

  private static final Logger log = LoggerFactory.getLogger(AssignmentStoreConfig.class);

  private static final String CONFIG_PREFIX = "uber.ureplicator3.assignments.";

  public static final String STORAGE_TOPIC_CONFIG = CONFIG_PREFIX + "storage.topic";
  public static final String STORAGE_TOPIC_PREFIX = STORAGE_TOPIC_CONFIG + ".";
  private static final String STORAGE_TOPIC_DOC = "Topic used to store assignments. "
          + "This topic must be compacted (i.e., have the 'compacted' cleanup policy) and can have "
          + "any number of partitions, but it's recommended to use a low number (e.g., 5 or 10). "
          + "If the topic does not exist, it will be automatically created on startup. "
          + "Additional properties for the topic can be specified using the " + STORAGE_TOPIC_PREFIX + " "
          + "prefix";

  private static final String PARTITIONS_SUFFIX = "partitions";
  public static final String STORAGE_TOPIC_PARTITIONS_CONFIG = STORAGE_TOPIC_PREFIX + PARTITIONS_SUFFIX;
  private static final int STORAGE_TOPIC_PARTITIONS_DEFAULT = 10;
  private static final String STORAGE_TOPIC_PARTITIONS_DOC = "The number of partitions to create "
          + "the assignments topic with, if it does not already exist. If the topic already exists this property "
          + "will be ignored, even if the topic has a different number of partitions";

  private static final String REPLICATION_FACTOR_SUFFIX = "replication.factor";
  public static final String STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG = STORAGE_TOPIC_PREFIX + REPLICATION_FACTOR_SUFFIX;
  private static final short STORAGE_TOPIC_REPLICATION_FACTOR_DEFAULT = (short) 3;
  private static final String STORAGE_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor to create "
          + "the assignments topic with, if it does not already exist. If the topic already exists this property "
          + "will be ignored, even if the topic has a different replication factor";

  private static final String SEGMENT_BYTES_SUFFIX = TopicConfig.SEGMENT_BYTES_CONFIG;
  public static final String STORAGE_TOPIC_SEGMENT_BYTES_CONFIG = STORAGE_TOPIC_PREFIX + SEGMENT_BYTES_SUFFIX;
  private static final int STORAGE_TOPIC_SEGMENT_BYTES_DEFAULT = 1024 * 1024 * 3; // Lower default segment size in order to enable more-frequent compaction
  private static final String STORAGE_TOPIC_SEGMENT_BYTES_DOC = "The per-partition segment size "
          + "for the assignments topic. This should be relatively low in order to enable topic compaction to take place, which "
          + "will help keep the topic from growing too large and causing slow startup time for workers. Like other topic "
          + "properties, if the topic already exists, this property will be ignored, even if the topic has a different segment size";

  public static final String CLIENTS_PREFIX = CONFIG_PREFIX + "kafka.clients";
  public static final String CONSUMER_PREFIX = CONFIG_PREFIX + "consumer";
  public static final String PRODUCER_PREFIX = CONFIG_PREFIX + "producer";
  public static final String ADMIN_PREFIX = CONFIG_PREFIX + "admin";

  public AssignmentStoreConfig(Map<String, ?> props) {
    super(config(), props);
  }

  public static ConfigDef config() {
    return addToConfig(new ConfigDef());
  }

  public static ConfigDef addToConfig(ConfigDef base) {
    return base
            .define(
                    STORAGE_TOPIC_CONFIG,
                    Type.STRING,
                    Importance.HIGH,
                    STORAGE_TOPIC_DOC
            ).define(
                    STORAGE_TOPIC_PARTITIONS_CONFIG,
                    Type.INT,
                    STORAGE_TOPIC_PARTITIONS_DEFAULT,
                    atLeast(1),
                    Importance.LOW,
                    STORAGE_TOPIC_PARTITIONS_DOC
            ).define(
                    STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG,
                    Type.SHORT,
                    STORAGE_TOPIC_REPLICATION_FACTOR_DEFAULT,
                    atLeast(1),
                    Importance.LOW,
                    STORAGE_TOPIC_REPLICATION_FACTOR_DOC
            ).define(
                    STORAGE_TOPIC_SEGMENT_BYTES_CONFIG,
                    Type.INT,
                    STORAGE_TOPIC_SEGMENT_BYTES_DEFAULT,
                    atLeast(1),
                    Importance.LOW,
                    STORAGE_TOPIC_SEGMENT_BYTES_DOC
            );
  }

  public Map<String, Object> adminConfig() {
    return clientConfig(ADMIN_PREFIX);
  }

  public Map<String, Object> producerConfig() {
    return clientConfig(PRODUCER_PREFIX);
  }

  public Map<String, Object> consumerConfig() {
    return clientConfig(CONSUMER_PREFIX);
  }

  public String storageTopic() {
    return getString(STORAGE_TOPIC_CONFIG);
  }

  public int storageTopicPartitions() {
    return getInt(STORAGE_TOPIC_PARTITIONS_CONFIG);
  }

  public short storageTopicReplicationFactor() {
    return getShort(STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG);
  }

  public Map<String, Object> storageTopicSettings() {
    Map<String, Object> result = originalsWithPrefix(STORAGE_TOPIC_PREFIX);

    // AbstractConfig::originalsWithPrefix doesn't include default values, we apply that manually here
    result.computeIfAbsent(TopicConfig.SEGMENT_BYTES_CONFIG, k -> STORAGE_TOPIC_SEGMENT_BYTES_DEFAULT);

    Object removedPolicy = result.remove(TopicConfig.CLEANUP_POLICY_CONFIG);
    if (removedPolicy != null) {
      log.warn("Ignoring '{}cleanup.policy={}' setting, since compaction is always used", STORAGE_TOPIC_PREFIX, removedPolicy);
    }

    result.remove(REPLICATION_FACTOR_SUFFIX);
    result.remove(PARTITIONS_SUFFIX);

    return result;
  }

  private Map<String, Object> clientConfig(String specialPrefix) {
    Map<String, Object> result = new HashMap<>(originals());
    result.putAll(originalsWithPrefix(CLIENTS_PREFIX));
    result.putAll(originalsWithPrefix(specialPrefix));
    return result;
  }
}
