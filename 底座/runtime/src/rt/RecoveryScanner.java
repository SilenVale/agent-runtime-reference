package rt;

import store.Database;
import store.Ids;
import store.OutboxRow;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

/**
 * 恢复扫描是 Runtime 里的另一个角色，不由 Worker 兼任。
 * 它只登记 Outbox，不跑图，也不改执行代次。
 */
public final class RecoveryScanner {
    private final Ids ids;

    public RecoveryScanner() {
        this(new Ids());
    }

    public RecoveryScanner(Ids ids) {
        this.ids = ids;
    }

    public String role() {
        return "recovery-scanner";
    }

    public int scan(Database database) {
        int registered = 0;
        for (TaskRow task : database.tasks()) {
            if (task.state() != TaskState.RUNNING) {
                continue;
            }
            if (task.leaseExpiresAt() == null || task.leaseExpiresAt() > database.now()) {
                continue;
            }
            try (Transaction tx = database.begin()) {
                TaskRow current = tx.requireTask(task.taskId());
                if (current.state() != TaskState.RUNNING
                        || current.leaseExpiresAt() == null
                        || current.leaseExpiresAt() > tx.now()
                        || tx.hasPendingOutbox(current.taskId())) {
                    tx.rollback();
                    continue;
                }
                int nextSeq = tx.maxDispatchSeq(current.taskId()) + 1;
                tx.saveOutbox(new OutboxRow(
                        ids.next("msg"),
                        current.taskId(),
                        nextSeq,
                        current.epoch(),
                        "RECOVERY",
                        "PENDING",
                        current.runVersion()));
                tx.commit();
                registered++;
            }
        }
        return registered;
    }
}
