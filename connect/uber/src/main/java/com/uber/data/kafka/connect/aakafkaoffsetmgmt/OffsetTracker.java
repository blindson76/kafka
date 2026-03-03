package com.uber.data.kafka.connect.aakafkaoffsetmgmt;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetTracker {
    private final Map<TopicPartition, Long> highestOffsetCopied = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> highestOffsetSkipped = new ConcurrentHashMap<>();

    public void trackSkipped(TopicPartition tp, long offset) {
        highestOffsetSkipped.put(tp, offset);
    }

    public void trackCopied(TopicPartition tp, long offset) {
        highestOffsetCopied.put(tp, offset);
    }

    public Map<TopicPartition, Long> getHighestSkipped() {
        return highestOffsetSkipped;
    }

    public Map<TopicPartition, Long> getHighestCopied() {
        return highestOffsetCopied;
    }

    public void clear() {
        highestOffsetCopied.clear();
        highestOffsetSkipped.clear();
    }
}