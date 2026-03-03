package com.uber.data.kafka.connect.workload.common;

import java.util.Objects;

public record CachedWorkload(long lastUpdated, Workload workload) implements Comparable<CachedWorkload> {

    public CachedWorkload(long lastUpdated, Workload workload) {
        this.lastUpdated = lastUpdated;
        this.workload = Objects.requireNonNull(workload, "workload may not be null");
    }

    @Override
    public int compareTo(CachedWorkload that) {
        if (this == that)
            return 0;

        if (that == null)
            return 1;

        int result = this.workload.compareTo(that.workload);
        if (result != 0)
            return result;

        return Long.compare(this.lastUpdated, that.lastUpdated);
    }


}
