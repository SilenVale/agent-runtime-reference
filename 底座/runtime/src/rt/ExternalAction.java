package rt;

import fake.FakeWrite;
import store.Ids;
import store.OperationRow;
import store.Rejected;
import store.TaskMachine;
import store.Transaction;

/**
 * operation_id 在调用前写入操作账本。超时后按原号查询，不换号再写。
 */
public final class ExternalAction {
    private final Ids ids;
    private final TaskMachine machine = new TaskMachine();

    public ExternalAction(Ids ids) {
        this.ids = ids;
    }

    public String prepare(Transaction tx, String taskId, int epoch, String owner, String nodeKey, String digest) {
        machine.requireRight(tx, taskId, epoch, owner);
        OperationRow existing = tx.operationForNode(taskId, nodeKey);
        if (existing != null) {
            if (existing.paramDigest() != null && !existing.paramDigest().equals(digest)) {
                throw new Rejected("同一定位换参数");
            }
            return existing.operationId();
        }
        String operationId = ids.next("op");
        tx.saveOperation(new OperationRow(operationId, taskId, "PLANNED", nodeKey, digest));
        return operationId;
    }

    public String invoke(FakeWrite fake, String operationId, String digest, String body, boolean dropResponse) {
        String found = fake.query(operationId);
        if (found != null) {
            return found;
        }
        String written = fake.write(operationId, digest, body);
        if (dropResponse) {
            return null;
        }
        return written;
    }
}
