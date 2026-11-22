package store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Transaction implements AutoCloseable {
    private final Database database;
    private final Map<String, TaskRow> tasks;
    private final Map<String, OperationRow> operations;
    private final Map<String, CheckpointRow> checkpoints;
    private final Map<String, ApprovalRow> approvals;
    private final List<SessionNote> sessions;
    private boolean finished;

    Transaction(Database database) {
        this.database = database;
        this.tasks = new HashMap<>(database.tasks);
        this.operations = new HashMap<>(database.operations);
        this.checkpoints = new HashMap<>(database.checkpoints);
        this.approvals = new HashMap<>(database.approvals);
        this.sessions = new ArrayList<>(database.sessions);
    }

    public long now() {
        return database.now();
    }

    public TaskRow task(String taskId) {
        return tasks.get(taskId);
    }

    public TaskRow requireTask(String taskId) {
        TaskRow task = task(taskId);
        if (task == null) {
            throw new Rejected("任务不存在");
        }
        return task;
    }

    public void saveTask(TaskRow task) {
        ensureOpen();
        tasks.put(task.taskId(), task);
    }

    public void saveOperation(OperationRow operation) {
        ensureOpen();
        operations.put(operation.operationId(), operation);
    }

    public OperationRow operation(String operationId) {
        return operations.get(operationId);
    }

    public void saveCheckpoint(CheckpointRow checkpoint) {
        ensureOpen();
        checkpoints.put(checkpoint.threadId(), checkpoint);
    }

    public CheckpointRow checkpoint(String threadId) {
        return checkpoints.get(threadId);
    }

    public void saveApproval(ApprovalRow approval) {
        ensureOpen();
        approvals.put(approval.taskId(), approval);
    }

    public ApprovalRow approval(String taskId) {
        return approvals.get(taskId);
    }

    public void appendSession(SessionNote note) {
        ensureOpen();
        sessions.add(note);
    }

    public void commit() {
        ensureOpen();
        database.tasks.clear();
        database.tasks.putAll(tasks);
        database.operations.clear();
        database.operations.putAll(operations);
        database.checkpoints.clear();
        database.checkpoints.putAll(checkpoints);
        database.approvals.clear();
        database.approvals.putAll(approvals);
        database.sessions.clear();
        database.sessions.addAll(sessions);
        finished = true;
    }

    public void rollback() {
        finished = true;
    }

    @Override
    public void close() {
        if (!finished) {
            rollback();
        }
    }

    private void ensureOpen() {
        if (finished) {
            throw new Rejected("事务已结束");
        }
    }
}
