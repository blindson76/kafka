package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.data.kafka.connect.workload.common.Workload;

public record WorkloadEntity(
    @JsonProperty("bytes_per_second") double bytesPerSecond,
    @JsonProperty("messages_per_second") double messagesPerSecond
) {

    public static WorkloadEntity of(Workload workload) {
        return new WorkloadEntity(workload.getBytesPerSecond(), workload.getMessagesPerSecond());
    }

    public Workload workload() {
        return new Workload(bytesPerSecond, messagesPerSecond);
    }
}