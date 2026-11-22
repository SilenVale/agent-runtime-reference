package store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 进程内的 PostgreSQL 事务模型。提交后的行对所有进程可见。未提交的事务随进程退出丢弃。
 */
public final class Database {
    final Map<String, TaskRow> tasks = new HashMap<>();
    final Map<String, OperationRow> operations = new HashMap<>();
    final Map<String, CheckpointRow> checkpoints = new HashMap<>();
    final Map<String, ApprovalRow> approvals = new HashMap<>();
    final Map<String, IdempotencyRow> idempotency = new HashMap<>();
    final Map<String, EventRow> events = new HashMap<>();
    final Map<String, OutboxRow> outbox = new HashMap<>();
    final Map<String, InboxRow> inbox = new HashMap<>();
    final Map<String, QuotaRow> quotas = new HashMap<>();
    final Map<String, BudgetCall> budgetCalls = new HashMap<>();
    final Map<String, Integer> followUps = new HashMap<>();
    final List<SessionNote> sessions = new ArrayList<>();
    private long now;

    public Transaction begin() {
        return new Transaction(this);
    }

    public long now() {
        return now;
    }

    public void advanceClock(long millis) {
        now += millis;
    }

    public TaskRow task(String taskId) {
        return tasks.get(taskId);
    }

    public Collection<TaskRow> tasks() {
        return List.copyOf(tasks.values());
    }

    public OperationRow operation(String operationId) {
        return operations.get(operationId);
    }

    public CheckpointRow checkpoint(String threadId) {
        return checkpoints.get(threadId);
    }

    public ApprovalRow approval(String taskId) {
        return approvals.get(taskId);
    }

    public List<SessionNote> sessions() {
        return List.copyOf(sessions);
    }

    public Collection<EventRow> events() {
        return List.copyOf(events.values());
    }

    public Collection<OutboxRow> outbox() {
        return List.copyOf(outbox.values());
    }

    public Collection<InboxRow> inbox() {
        return List.copyOf(inbox.values());
    }

    public BudgetCall budgetCall(String callId) {
        return budgetCalls.get(callId);
    }

    public int followUps(String taskId) {
        return followUps.getOrDefault(taskId, 0);
    }

    public String operationForAny(String taskId) {
        for (OperationRow row : operations.values()) {
            if (row.taskId().equals(taskId)) {
                return row.operationId();
            }
        }
        return null;
    }

    public List<EventRow> eventsOf(String taskId) {
        List<EventRow> rows = new ArrayList<>();
        for (EventRow event : events.values()) {
            if (event.taskId().equals(taskId)) {
                rows.add(event);
            }
        }
        rows.sort((left, right) -> Integer.compare(left.eventSeq(), right.eventSeq()));
        return rows;
    }

    public Collection<IdempotencyRow> idempotency() {
        return List.copyOf(idempotency.values());
    }

    public List<OutboxRow> pendingOutbox() {
        List<OutboxRow> pending = new ArrayList<>();
        for (OutboxRow row : outbox.values()) {
            if ("PENDING".equals(row.status())) {
                pending.add(row);
            }
        }
        pending.sort((left, right) -> Integer.compare(left.dispatchSeq(), right.dispatchSeq()));
        return pending;
    }
}
