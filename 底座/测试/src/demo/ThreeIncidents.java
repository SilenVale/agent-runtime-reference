package demo;

import java.util.ArrayList;

import access.AcceptResult;
import access.AcceptService;
import dispatch.OutboxDispatcher;
import fake.FakeWrite;
import rt.ExternalAction;
import rt.LocalSlots;
import rt.RecoveryScanner;
import rt.ScheduleConsumer;
import store.Database;
import store.Ids;
import store.KafkaLog;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

/**
 * 三段演示：同一个幂等键只有一条任务；Worker 退出后另一个 Worker 从恢复路径继续；同一个 operation_id 不写第二次。
 */
public final class ThreeIncidents {
    public static void main(String[] args) {
        System.out.print(run());
    }

    public static String run() {
        StringBuilder out = new StringBuilder();
        Database db = new Database();
        Ids ids = new Ids();
        KafkaLog kafka = new KafkaLog();
        AcceptService accept = new AcceptService(db, ids);
        AcceptResult first = accept.accept("tenant", "actor", "same-key", "digest", 100);
        AcceptResult second = accept.accept("tenant", "actor", "same-key", "digest", 100);
        out.append("受理 ").append(first.status()).append(' ').append(first.taskId()).append('\n');
        out.append("再次受理 ").append(second.taskId())
                .append(" 任务数=").append(db.tasks().size()).append('\n');

        new OutboxDispatcher().pump(db, kafka);
        out.append("投递 ").append(kafka.messages().get(0).messageId()).append('\n');

        ScheduleConsumer consumer = new ScheduleConsumer(ids);
        LocalSlots slots = new LocalSlots(4, new ArrayList<>());
        consumer.consumeNext(db, kafka, "runtime-workers", "pod-a", slots, new ArrayList<>(), true, true);
        TaskRow taken = db.task(first.taskId());
        out.append("接管 epoch=").append(taken.epoch()).append(" owner=").append(taken.owner()).append('\n');

        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            new TaskMachine().submit(tx, first.taskId(), 0, "pod-a", TaskState.SUCCEEDED);
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("旧代次");
        }
        if (!rejected) {
            throw new IllegalStateException("代次没有被拒绝");
        }
        out.append("代次拒绝\n");

        db.advanceClock(ScheduleConsumer.LEASE_MS + 1);
        new RecoveryScanner(ids).scan(db);
        new OutboxDispatcher().pump(db, kafka);
        consumer.consumeNext(db, kafka, "runtime-workers", "pod-b", slots, new ArrayList<>(), true, true);
        TaskRow continued = db.task(first.taskId());
        out.append("另一个Worker继续 epoch=").append(continued.epoch())
                .append(" owner=").append(continued.owner()).append('\n');

        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = new ExternalAction(ids).prepare(
                    tx, first.taskId(), continued.epoch(), continued.owner(), "node-write", "digest");
            tx.commit();
        }
        FakeWrite fake = new FakeWrite();
        new ExternalAction(ids).invoke(fake, operationId, "digest", "done", true);
        String found = new ExternalAction(ids).invoke(fake, operationId, "digest", "done", false);
        out.append("按原编号查询 ").append(operationId)
                .append(" 结果=").append(found)
                .append(" 写入次数=").append(fake.writes()).append('\n');
        return out.toString();
    }
}
