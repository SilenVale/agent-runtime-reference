package store;

public enum TaskState {
    ACCEPTED,
    RUNNING,
    WAITING_USER,
    WAITING_HUMAN,
    RECONCILING,
    CANCEL_REQUESTED,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    HANDOFF;

    public boolean terminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
