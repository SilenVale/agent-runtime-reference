package rt;

import store.Database;
import store.TaskRow;
import store.Transaction;

/**
 * 缩容时停止新的接管，并把还没做完的任务留在恢复引用上。
 */
public final class Drainer {
    private boolean accepting = true;

    public boolean accepting() {
        return accepting;
    }

    public void stopNewTakeover() {
        accepting = false;
    }

    public void handoff(Database database, String taskId, int epoch, String owner, String threadId) {
        RecoveryPoint points = new RecoveryPoint();
        try (Transaction tx = database.begin()) {
            points.register(tx, taskId, epoch, owner, threadId);
            TaskRow task = tx.requireTask(taskId);
            tx.saveTask(task.toBuilder().owner(null).leaseExpiresAt(database.now()).build());
            tx.commit();
        }
    }
}
