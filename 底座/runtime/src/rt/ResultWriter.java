package rt;

import store.Ids;
import store.OperationRow;
import store.TaskMachine;
import store.Transaction;
import store.EventRow;

/**
 * T5：持有执行权的一方把结果写回运行库。它不代替假写自己的提交。
 */
public final class ResultWriter {
    private final Ids ids;
    private final TaskMachine machine = new TaskMachine();

    public ResultWriter(Ids ids) {
        this.ids = ids;
    }

    public void record(Transaction tx, String taskId, int epoch, String owner, String operationId, String status) {
        machine.requireRight(tx, taskId, epoch, owner);
        OperationRow row = tx.operation(operationId);
        tx.saveOperation(row.withStatus(status));
        tx.saveEvent(new EventRow(ids.next("event"), taskId, tx.nextEventSeq(taskId), "结果已记录"));
    }
}
