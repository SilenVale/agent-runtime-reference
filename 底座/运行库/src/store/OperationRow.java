package store;

public final class OperationRow {
    private final String operationId;
    private final String taskId;
    private final String status;

    public OperationRow(String operationId, String taskId, String status) {
        this.operationId = operationId;
        this.taskId = taskId;
        this.status = status;
    }

    public String operationId() {
        return operationId;
    }

    public String taskId() {
        return taskId;
    }

    public String status() {
        return status;
    }
}
