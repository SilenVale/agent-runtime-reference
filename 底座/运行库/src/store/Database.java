package store;

import java.util.ArrayList;
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
}
