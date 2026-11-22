package store;

public final class OperationRow {
    private final String operationId;
    private final String taskId;
    private final String status;
    private final String nodeKey;
    private final String paramDigest;

    public OperationRow(String operationId, String taskId, String status) {
        this(operationId, taskId, status, null, null);
    }

    public OperationRow(String operationId, String taskId, String status, String nodeKey, String paramDigest) {
        this.operationId = operationId;
        this.taskId = taskId;
        this.status = status;
        this.nodeKey = nodeKey;
        this.paramDigest = paramDigest;
    }

    public OperationRow withStatus(String nextStatus) {
        return new OperationRow(operationId, taskId, nextStatus, nodeKey, paramDigest);
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

    public String nodeKey() {
        return nodeKey;
    }

    public String paramDigest() {
        return paramDigest;
    }
}
