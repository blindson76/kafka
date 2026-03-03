package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TaskWorkloadEntity(
    @JsonProperty String connector,
    @JsonProperty int taskId,
    @JsonProperty WorkloadEntity workload
) {
}
