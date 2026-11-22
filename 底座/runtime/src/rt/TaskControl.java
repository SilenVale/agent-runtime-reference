package rt;

import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

/**
 * 取消、核对和人工等待。取消不等于把假写撤掉。
 */
public final class TaskControl {
    private final TaskMachine machine = new TaskMachine();

    public void requestCancel(Transaction tx, String taskId) {
        TaskRow task = tx.requireTask(taskId);
        if (task.state().terminal()) {
            throw new Rejected("终态不能重新启动");
        }
        tx.saveTask(task.toBuilder().state(TaskState.CANCEL_REQUESTED).build());
    }

    public void finishCancel(Transaction tx, String taskId, int epoch, String owner, boolean businessConfirmed) {
        TaskRow task = machine.requireRight(tx, taskId, epoch, owner);
        if (task.state() != TaskState.CANCEL_REQUESTED) {
            throw new Rejected("还没有取消请求");
        }
        if (!businessConfirmed) {
            throw new Rejected("结果不明不能进入终态");
        }
        tx.saveTask(task.toBuilder().state(TaskState.CANCELLED).build());
    }

    public void noteUnknown(Transaction tx, String taskId, int limit) {
        int next = tx.followUps(taskId) + 1;
        tx.saveFollowUps(taskId, next);
        TaskRow task = tx.requireTask(taskId);
        TaskState nextState = next >= limit ? TaskState.WAITING_HUMAN : TaskState.RECONCILING;
        tx.saveTask(task.toBuilder().state(nextState).build());
    }
}
