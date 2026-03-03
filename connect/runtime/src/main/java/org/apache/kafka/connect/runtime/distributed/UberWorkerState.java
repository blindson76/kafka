package org.apache.kafka.connect.runtime.distributed;

public class UberWorkerState extends ExtendedWorkerState {

    private final UberAssignmentV1 assignment;

    public UberWorkerState(String url, long offset, UberAssignmentV1 assignment) {
        super(url, offset, assignment);
        this.assignment = assignment;
    }

    @Override
    public UberAssignmentV1 assignment() {
        return assignment;
    }
}
