package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.data.kafka.connect.workload.common.CachedWorkload;

public record CachedWorkloadEntity(
        @JsonProperty long lastUpdated,
        @JsonProperty WorkloadEntity workload
) {

    public static CachedWorkloadEntity of(CachedWorkload cachedWorkload) {
        WorkloadEntity workload = WorkloadEntity.of(cachedWorkload.workload());
        return new CachedWorkloadEntity(cachedWorkload.lastUpdated(), workload);
    }
}
