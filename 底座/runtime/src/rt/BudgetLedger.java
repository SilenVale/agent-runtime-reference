package rt;

import store.BudgetCall;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.Transaction;

/**
 * 预算记在任务上。预留、结算、释放都认稳定的 call_id。
 * 结算不要求原代次仍是当前代次。旧代次不能新开预留，也不能覆盖累计。
 */
public final class BudgetLedger {
    private final TaskMachine machine = new TaskMachine();

    public void reserve(
            Transaction tx,
            String taskId,
            int epoch,
            String owner,
            String callId,
            String reservationId,
            long amount) {
        TaskRow task = machine.requireRight(tx, taskId, epoch, owner);
        BudgetCall existing = tx.budgetCall(callId);
        if (existing != null) {
            if (!existing.reservationId().equals(reservationId) || existing.amount() != amount) {
                throw new Rejected("同一call_id的预留不一致");
            }
            return;
        }
        long available = task.budgetLimit() - task.spent() - task.reserved();
        if (available < amount) {
            throw new Rejected("预算不足");
        }
        tx.saveBudgetCall(new BudgetCall(callId, reservationId, taskId, amount, "RESERVED"));
        tx.saveTask(task.toBuilder().reserved(task.reserved() + amount).build());
    }

    public void settle(Transaction tx, String callId, long actual) {
        BudgetCall call = tx.budgetCall(callId);
        if (call == null) {
            throw new Rejected("没有这笔预留");
        }
        if ("SETTLED".equals(call.state())) {
            return;
        }
        if (actual > call.amount()) {
            throw new Rejected("结算不能超过预留");
        }
        TaskRow task = tx.requireTask(call.taskId());
        tx.saveTask(task.toBuilder()
                .reserved(task.reserved() - call.amount())
                .spent(task.spent() + actual)
                .build());
        tx.saveBudgetCall(call.settle(actual));
    }

    public void overwriteSpent(Transaction tx, String taskId, int epoch, String owner, long spent) {
        TaskRow task = machine.requireRight(tx, taskId, epoch, owner);
        tx.saveTask(task.toBuilder().spent(spent).build());
    }
}
