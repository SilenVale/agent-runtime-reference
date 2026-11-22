package access;

import store.Database;
import store.TaskRow;

/**
 * 入站只复用受理。连接断开不取消任务，也不改 Checkpoint。
 */
public final class InboundGateway {
    private final AcceptService acceptService;

    public InboundGateway(AcceptService acceptService) {
        this.acceptService = acceptService;
    }

    public AcceptResult submit(String tenant, String actor, String key, String digest, long budgetLimit) {
        return acceptService.accept(tenant, actor, key, digest, budgetLimit);
    }

    public TaskRow disconnect(Database database, String taskId) {
        return database.task(taskId);
    }
}
