package access;

import store.Database;
import store.EventRow;
import store.IdempotencyRow;
import store.Ids;
import store.OutboxRow;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

/**
 * 受理只写运行库。这里不调用图。
 */
public final class AcceptService {
    private final Database database;
    private final Ids ids;

    public AcceptService(Database database, Ids ids) {
        this.database = database;
        this.ids = ids;
    }

    public AcceptResult accept(String tenant, String actor, String key, String digest, long budgetLimit) {
        return accept(tenant, actor, key, digest, budgetLimit, true);
    }

    public AcceptResult accept(
            String tenant,
            String actor,
            String key,
            String digest,
            long budgetLimit,
            boolean commit) {
        try (Transaction tx = database.begin()) {
            IdempotencyRow existing = tx.idempotency(tenant, actor, key);
            if (existing != null) {
                tx.rollback();
                if (!existing.paramDigest().equals(digest)) {
                    return AcceptResult.conflict();
                }
                return AcceptResult.existing(existing.taskId());
            }
            String taskId = ids.next("task");
            String eventId = ids.next("event");
            String messageId = ids.next("msg");
            tx.saveTask(TaskRow.builder(taskId, tenant)
                    .actor(actor)
                    .idempotencyKey(key)
                    .paramDigest(digest)
                    .runVersion("v1")
                    .state(TaskState.ACCEPTED)
                    .epoch(0)
                    .budgetLimit(budgetLimit)
                    .spent(0)
                    .reserved(0)
                    .build());
            tx.saveIdempotency(new IdempotencyRow(tenant, actor, key, digest, taskId));
            tx.saveEvent(new EventRow(eventId, taskId, 1, "任务已受理"));
            tx.saveOutbox(new OutboxRow(messageId, taskId, 1, 0, "START", "PENDING", "v1"));
            if (!commit) {
                tx.rollback();
                return AcceptResult.notCommitted();
            }
            tx.commit();
            return AcceptResult.accepted(taskId);
        }
    }
}
