package store;

/**
 * 任务状态机。会话记录、操作账本和 Checkpoint 各自保存，不能互相代替。
 */
public final class TaskMachine {
    public TaskRow takeover(Transaction tx, String taskId, String newOwner, long leaseUntil) {
        TaskRow task = tx.requireTask(taskId);
        if (task.state().terminal()) {
            throw new Rejected("终态不能接管");
        }
        if (holdsValidLease(tx, task)) {
            return task;
        }
        TaskState next = task.state() == TaskState.ACCEPTED ? TaskState.RUNNING : task.state();
        TaskRow updated = task.toBuilder()
                .state(next)
                .epoch(task.epoch() + 1)
                .owner(newOwner)
                .leaseExpiresAt(leaseUntil)
                .build();
        tx.saveTask(updated);
        return updated;
    }

    public void submit(Transaction tx, String taskId, int epoch, String owner, TaskState target) {
        TaskRow task = requireRight(tx, taskId, epoch, owner);
        tx.saveTask(task.toBuilder().state(target).build());
    }

    public TaskRow renew(Transaction tx, String taskId, int epoch, String owner, long leaseUntil) {
        TaskRow task = requireRight(tx, taskId, epoch, owner);
        if (leaseUntil <= tx.now()) {
            throw new Rejected("租约已失效");
        }
        TaskRow renewed = task.toBuilder().leaseExpiresAt(leaseUntil).build();
        tx.saveTask(renewed);
        return renewed;
    }

    public TaskRow requireRight(Transaction tx, String taskId, int epoch, String owner) {
        TaskRow task = tx.requireTask(taskId);
        if (task.state().terminal()) {
            throw new Rejected("终态拒绝回写");
        }
        if (task.epoch() != epoch || task.owner() == null || !task.owner().equals(owner)) {
            throw new Rejected("旧代次不能回写");
        }
        if (task.leaseExpiresAt() == null || task.leaseExpiresAt() <= tx.now()) {
            throw new Rejected("租约已失效");
        }
        return task;
    }

    public void markWaiting(Transaction tx, String taskId, long deadline) {
        TaskRow task = tx.requireTask(taskId);
        if (task.state().terminal()) {
            throw new Rejected("终态不能进入等待");
        }
        tx.saveTask(task.toBuilder()
                .state(TaskState.WAITING_USER)
                .owner(null)
                .leaseExpiresAt(null)
                .build());
        tx.saveApproval(new ApprovalRow(taskId, "PENDING", deadline));
    }

    public void onWaitingDeadline(Transaction tx, String taskId) {
        ApprovalRow approval = tx.approval(taskId);
        if (approval == null) {
            throw new Rejected("没有审批记录");
        }
        if (tx.now() >= approval.deadline()) {
            return;
        }
    }

    public void appendSession(Transaction tx, String conversationId, String text) {
        tx.appendSession(new SessionNote(conversationId, text));
    }

    private static boolean holdsValidLease(Transaction tx, TaskRow task) {
        return task.owner() != null
                && task.leaseExpiresAt() != null
                && task.leaseExpiresAt() > tx.now();
    }
}
