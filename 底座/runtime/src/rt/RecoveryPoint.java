package rt;

import store.CheckpointRow;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.Transaction;

/**
 * 当前执行者才能登记恢复引用。引用可以指向较早代次的存档。
 * 旧执行者自己写的新存档不能登记成当前恢复点。
 */
public final class RecoveryPoint {
    private final TaskMachine machine = new TaskMachine();

    public void register(Transaction tx, String taskId, int epoch, String owner, String threadId) {
        TaskRow task = machine.requireRight(tx, taskId, epoch, owner);
        CheckpointRow checkpoint = tx.checkpoint(threadId);
        if (checkpoint == null || !checkpoint.taskId().equals(taskId)) {
            throw new Rejected("恢复引用无效");
        }
        tx.saveTask(task.toBuilder().recoveryRef(threadId).build());
    }
}
