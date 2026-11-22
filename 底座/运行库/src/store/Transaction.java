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
    private final Map<String, IdempotencyRow> idempotency;
    private final Map<String, EventRow> events;
    private final Map<String, OutboxRow> outbox;
    private final Map<String, InboxRow> inbox;
    private final Map<String, QuotaRow> quotas;
    private final Map<String, BudgetCall> budgetCalls;
    private final List<SessionNote> sessions;
    private boolean finished;

    Transaction(Database database) {
        this.database = database;
        this.tasks = new HashMap<>(database.tasks);
        this.operations = new HashMap<>(database.operations);
        this.checkpoints = new HashMap<>(database.checkpoints);
        this.approvals = new HashMap<>(database.approvals);
        this.idempotency = new HashMap<>(database.idempotency);
        this.events = new HashMap<>(database.events);
        this.outbox = new HashMap<>(database.outbox);
        this.inbox = new HashMap<>(database.inbox);
        this.quotas = new HashMap<>(database.quotas);
        this.budgetCalls = new HashMap<>(database.budgetCalls);
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

    public IdempotencyRow idempotency(String tenant, String actor, String key) {
        return idempotency.get(tenant + "|" + actor + "|" + key);
    }

    public void saveIdempotency(IdempotencyRow row) {
        ensureOpen();
        idempotency.put(row.id(), row);
    }

    public void saveEvent(EventRow event) {
        ensureOpen();
        events.put(event.id(), event);
    }

    public int nextEventSeq(String taskId) {
        int max = 0;
        for (EventRow event : events.values()) {
            if (event.taskId().equals(taskId) && event.eventSeq() > max) {
                max = event.eventSeq();
            }
        }
        return max + 1;
    }

    public void saveOutbox(OutboxRow row) {
        ensureOpen();
        outbox.put(row.messageId(), row);
    }

    public OutboxRow outbox(String messageId) {
        return outbox.get(messageId);
    }

    public boolean hasPendingOutbox(String taskId) {
        for (OutboxRow row : outbox.values()) {
            if (row.taskId().equals(taskId) && "PENDING".equals(row.status())) {
                return true;
            }
        }
        return false;
    }

    public int maxDispatchSeq(String taskId) {
        int max = 0;
        for (OutboxRow row : outbox.values()) {
            if (row.taskId().equals(taskId) && row.dispatchSeq() > max) {
                max = row.dispatchSeq();
            }
        }
        return max;
    }

    public void saveInbox(InboxRow row) {
        ensureOpen();
        inbox.put(row.id(), row);
    }

    public InboxRow inbox(String consumer, String messageId) {
        return inbox.get(consumer + "|" + messageId);
    }

    public void saveQuota(QuotaRow quota) {
        ensureOpen();
        quotas.put(quota.tenant(), quota);
    }

    public QuotaRow quota(String tenant) {
        return quotas.get(tenant);
    }

    public void saveBudgetCall(BudgetCall call) {
        ensureOpen();
        budgetCalls.put(call.callId(), call);
    }

    public BudgetCall budgetCall(String callId) {
        return budgetCalls.get(callId);
    }

    public OperationRow operationForNode(String taskId, String nodeKey) {
        for (OperationRow row : operations.values()) {
            if (row.taskId().equals(taskId) && nodeKey != null && nodeKey.equals(row.nodeKey())) {
                return row;
            }
        }
        return null;
    }

    public void commit() {
        ensureOpen();
        replace(database.tasks, tasks);
        replace(database.operations, operations);
        replace(database.checkpoints, checkpoints);
        replace(database.approvals, approvals);
        replace(database.idempotency, idempotency);
        replace(database.events, events);
        replace(database.outbox, outbox);
        replace(database.inbox, inbox);
        replace(database.quotas, quotas);
        replace(database.budgetCalls, budgetCalls);
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

    private static <K, V> void replace(Map<K, V> target, Map<K, V> source) {
        target.clear();
        target.putAll(source);
    }
}
