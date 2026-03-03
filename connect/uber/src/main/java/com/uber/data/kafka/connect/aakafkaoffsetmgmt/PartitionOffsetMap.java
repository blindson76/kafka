package com.uber.data.kafka.connect.aakafkaoffsetmgmt;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// helper class to keep track of offset mapping for topic partitions
public class PartitionOffsetMap {

  // first Pair is <dst partition, src partition>, second Pair is <dst offset, source offset>
  private final Map<Pair<Integer, Integer>, Pair<Long, Long>> partitionOffsetMap = new ConcurrentHashMap<>();

  public void put(int dstPartition, int srcPartition, long dstOffset, long srcOffset) {
    partitionOffsetMap.put(Pair.of(dstPartition, srcPartition), Pair.of(dstOffset, srcOffset));
  }

  public void removePartition(int partition) {
    partitionOffsetMap.remove(Pair.of(partition, partition));
  }

  public Map<Pair<Integer, Integer>, Pair<Long, Long>> getPartitionOffsetMap() {
    return ImmutableMap.copyOf(partitionOffsetMap);
  }

}
