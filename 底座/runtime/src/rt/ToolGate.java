package rt;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import fake.FakeWrite;
import store.OperationRow;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

/**
 * 所有工具调用都过这一道闸门。发现到的名字不会自动变成可调用工具。
 */
public final class ToolGate {
    private final Set<String> allowed = new LinkedHashSet<>();
    private final ExternalAction actions;
    private final TaskMachine machine = new TaskMachine();

    public ToolGate(ExternalAction actions) {
        this.actions = actions;
    }

    public void allow(String tool) {
        allowed.add(tool);
    }

    public List<String> visible(List<String> discovered) {
        List<String> kept = new ArrayList<>();
        for (String tool : discovered) {
            if (allowed.contains(tool)) {
                kept.add(tool);
            }
        }
        return kept;
    }

    public String call(
            Transaction tx,
            String taskId,
            int epoch,
            String owner,
            String tool,
            boolean write,
            boolean queryable,
            String nodeKey,
            String digest,
            FakeWrite fake,
            boolean timeout) {
        if (!allowed.contains(tool)) {
            throw new Rejected("未进工具闸门");
        }
        machine.requireRight(tx, taskId, epoch, owner);
        if (!write) {
            return "read:" + tool;
        }
        String operationId = actions.prepare(tx, taskId, epoch, owner, nodeKey, digest);
        if (!queryable) {
            if (timeout) {
                markHuman(tx, taskId);
                return "HUMAN";
            }
        } else {
            String found = fake.query(operationId);
            if (found != null) {
                return found;
            }
            if (timeout) {
                OperationRow row = tx.operation(operationId);
                tx.saveOperation(row.withStatus("UNKNOWN"));
                return "UNKNOWN";
            }
        }
        String body = fake.write(operationId, digest, "written");
        tx.saveOperation(tx.operation(operationId).withStatus("DISPATCHED"));
        return body;
    }

    private void markHuman(Transaction tx, String taskId) {
        TaskRow task = tx.requireTask(taskId);
        tx.saveTask(task.toBuilder().state(TaskState.WAITING_HUMAN).build());
    }
}
