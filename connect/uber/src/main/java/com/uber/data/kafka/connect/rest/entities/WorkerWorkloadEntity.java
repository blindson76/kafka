package com.uber.data.kafka.connect.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public record WorkerWorkloadEntity(
        @JsonProperty String worker,
        @JsonProperty("total_workload") WorkloadEntity totalWorkload,
        @JsonProperty("task_workloads") List<TaskWorkloadEntity> taskWorkloads
) {
}
