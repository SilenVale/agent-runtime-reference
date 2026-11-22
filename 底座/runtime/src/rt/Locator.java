package rt;

import store.Database;
import store.TaskRow;

/**
 * 没有 Checkpoint 时，按已保存的输入和操作账本确定位置。
 */
public final class Locator {
    public String locate(Database database, String taskId) {
        TaskRow task = database.task(taskId);
        if (task.recoveryRef() != null && database.checkpoint(task.recoveryRef()) != null) {
            return "checkpoint:" + task.recoveryRef();
        }
        String operationId = database.operationForAny(taskId);
        if (operationId != null) {
            return "ledger:" + operationId;
        }
        return "input:" + task.taskId();
    }
}
