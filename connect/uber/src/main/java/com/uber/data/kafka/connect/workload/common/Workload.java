package com.uber.data.kafka.connect.workload.common;

import java.util.Objects;

/**
 * POJO that tracks two generic workload statistics: bytes per second, and messages per second.
 */
public class Workload implements Comparable<Workload> {

    public static final Workload EMPTY = new Workload(0, 0);

    private final double bytesPerSecond;
    private final double messagesPerSecond;

    public Workload(double bytesPerSecond, double messagesPerSecond) {
        this.bytesPerSecond = bytesPerSecond;
        this.messagesPerSecond = messagesPerSecond;
    }

    public double getBytesPerSecond() {
        return bytesPerSecond;
    }

    public double getMessagesPerSecond() {
        return messagesPerSecond;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Workload workload = (Workload) o;
        return Double.compare(bytesPerSecond, workload.bytesPerSecond) == 0 && Double.compare(messagesPerSecond, workload.messagesPerSecond) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesPerSecond, messagesPerSecond);
    }

    @Override
    public String toString() {
        return "bytesPerSecond=" + bytesPerSecond + ",messagesPerSecond=" + messagesPerSecond;
    }

    @Override
    public int compareTo(Workload that) {
        if (this == that)
            return 0;

        if (that == null)
            return 1;

        int cmp = Double.compare(this.bytesPerSecond, that. bytesPerSecond);
        if (cmp != 0)
            return cmp;

        cmp = Double.compare(this.messagesPerSecond, that.messagesPerSecond);
        return cmp;
    }

    public Workload plus(Workload that) {
        if (that == null)
            return this;

        return new Workload(
            this.bytesPerSecond + that.bytesPerSecond,
            this.messagesPerSecond + that.messagesPerSecond
        );
    }
}
