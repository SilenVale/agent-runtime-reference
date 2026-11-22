package store;

public final class ApprovalRow {
    private final String taskId;
    private final String status;
    private final long deadline;

    public ApprovalRow(String taskId, String status, long deadline) {
        this.taskId = taskId;
        this.status = status;
        this.deadline = deadline;
    }

    public String taskId() {
        return taskId;
    }

    public String status() {
        return status;
    }

    public long deadline() {
        return deadline;
    }
}
