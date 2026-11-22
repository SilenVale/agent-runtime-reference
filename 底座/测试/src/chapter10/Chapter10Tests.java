package chapter10;

import java.util.ArrayList;
import java.util.List;

import access.AcceptService;
import access.SseFeed;
import dispatch.OutboxDispatcher;
import fake.FakeWrite;
import rt.ExternalAction;
import rt.Locator;
import rt.RecoveryPoint;
import rt.RecoveryScanner;
import rt.ResultWriter;
import rt.ScheduleConsumer;
import rt.TaskControl;
import rt.ToolGate;
import store.CheckpointRow;
import store.Database;
import store.EventRow;
import store.Ids;
import store.KafkaLog;
import store.Rejected;
import store.Resync;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter10Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter10Tests tests = new Chapter10Tests();
        tests.T1之后HTTP响应丢失();
        tests.Kafka已发送Outbox未标记();
        tests.T2之后offset未提交();
        tests.offset已提交图还没启动();
        tests.旧Worker停顿后又恢复();
        tests.T3之后业务还没发出();
        tests.T4成功T5前退出();
        tests.T6成功T5未提交();
        tests.旧代次写Checkpoint();
        tests.取消与完成同时到达();
        tests.事件已提交SSE中断();
        tests.补读过程中又有新事件();
        tests.游标超出保留期();
        tests.外部结果无法再核实();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void T1之后HTTP响应丢失() {
        Database db = new Database();
        AcceptService accept = new AcceptService(db, new Ids());
        String taskId = accept.accept("tenant", "actor", "key", "digest", 10).taskId();
        String again = accept.accept("tenant", "actor", "key", "digest", 10).taskId();
        check(taskId.equals(again) && db.tasks().size() == 1, "响应丢失后原键取回原任务");
        done("T1之后HTTP响应丢失");
    }

    private void Kafka已发送Outbox未标记() {
        Database db = new Database();
        Ids ids = new Ids();
        KafkaLog kafka = new KafkaLog();
        new AcceptService(db, ids).accept("tenant", "actor", "key", "digest", 10);
        kafka.loseNextAck();
        OutboxDispatcher dispatcher = new OutboxDispatcher();
        dispatcher.pump(db, kafka);
        String messageId = kafka.messages().get(0).messageId();
        dispatcher.pump(db, kafka);
        check(messageId.equals(kafka.messages().get(1).messageId()), "未标记的消息用原ID再投");
        check(db.task(db.tasks().iterator().next().taskId()).state() == TaskState.ACCEPTED, "再投没有回退任务");
        done("Kafka已发送Outbox未标记");
    }

    private void T2之后offset未提交() {
        Held held = started(false);
        check(held.db.task(held.taskId).epoch() == 1, "接管已经提交");
        check(held.kafka.offset(held.group) == 0, "offset还没提交");
        ScheduleConsumer.Outcome replay = held.consume(true);
        check("REUSED".equals(replay.kind()) && held.db.task(held.taskId).epoch() == 1, "重投认得出原来的代次");
        done("T2之后offset未提交");
    }

    private void offset已提交图还没启动() {
        Held held = started(true);
        check(held.db.checkpoint(held.db.task(held.taskId).threadId()) == null, "图还没有Checkpoint");
        held.db.advanceClock(ScheduleConsumer.LEASE_MS + 1);
        check(new RecoveryScanner(held.ids).scan(held.db) == 1, "改由租约到期发出恢复消息");
        done("offset已提交图还没启动");
    }

    private void 旧Worker停顿后又恢复() {
        Database db = running();
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            new TaskMachine().submit(tx, "task-1", 0, "owner-old", TaskState.SUCCEEDED);
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("旧代次");
        }
        check(rejected && db.task("task-1").state() == TaskState.RUNNING, "旧执行者的回写被拒绝");
        done("旧Worker停顿后又恢复");
    }

    private void T3之后业务还没发出() {
        Database db = running();
        ExternalAction action = new ExternalAction(new Ids());
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node", "digest");
            tx.commit();
        }
        String again;
        try (Transaction tx = db.begin()) {
            again = action.prepare(tx, "task-1", 1, "owner-a", "node", "digest");
            tx.commit();
        }
        check(operationId.equals(again) && new FakeWrite().writes() == 0, "还没发出的动作沿用原号");
        done("T3之后业务还没发出");
    }

    private void T4成功T5前退出() {
        Database db = running();
        Ids ids = new Ids();
        ExternalAction action = new ExternalAction(ids);
        FakeWrite fake = new FakeWrite();
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node", "digest");
            tx.commit();
        }
        action.invoke(fake, operationId, "digest", "done", true);
        String reused = action.invoke(fake, operationId, "digest", "done", false);
        try (Transaction tx = db.begin()) {
            new ResultWriter(ids).record(tx, "task-1", 1, "owner-a", operationId, "SUCCEEDED");
            tx.commit();
        }
        check("done".equals(reused) && fake.writes() == 1, "不把已经完成的假写再做一遍");
        done("T4成功T5前退出");
    }

    private void T6成功T5未提交() {
        Database db = running();
        try (Transaction tx = db.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "node-2"));
            tx.saveTask(db.task("task-1").toBuilder().recoveryRef("task-1:epoch-1").build());
            tx.commit();
        }
        try (Transaction tx = db.begin()) {
            tx.saveOperation(new store.OperationRow("op-1", "task-1", "PLANNED"));
            tx.rollback();
        }
        check(db.checkpoint("task-1:epoch-1") != null, "图位置已经提交");
        check(db.operation("op-1") == null, "结果确认没有提交");
        check("checkpoint:task-1:epoch-1".equals(new Locator().locate(db, "task-1")), "从最后确认的恢复点继续");
        done("T6成功T5未提交");
    }

    private void 旧代次写Checkpoint() {
        Database db = running();
        db.advanceClock(1_000L);
        try (Transaction tx = db.begin()) {
            new TaskMachine().takeover(tx, "task-1", "owner-b", db.now() + 30_000L);
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "old"));
            tx.commit();
        }
        try (Transaction tx = db.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "later-old"));
            tx.commit();
        }
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            new RecoveryPoint().register(tx, "task-1", 1, "owner-a", "task-1:epoch-1");
            tx.commit();
        } catch (Rejected ex) {
            rejected = true;
        }
        check(rejected && db.task("task-1").recoveryRef() == null, "旧线程的新存档不能登记成当前恢复点");
        done("旧代次写Checkpoint");
    }

    private void 取消与完成同时到达() {
        Database db = running();
        TaskControl control = new TaskControl();
        try (Transaction tx = db.begin()) {
            control.requestCancel(tx, "task-1");
            tx.commit();
        }
        boolean lost = false;
        try (Transaction tx = db.begin()) {
            new TaskMachine().submit(tx, "task-1", 1, "owner-a", TaskState.SUCCEEDED);
            tx.commit();
        } catch (Rejected ex) {
            lost = ex.getMessage().contains("状态已变化");
        }
        check(lost && db.task("task-1").state() == TaskState.CANCEL_REQUESTED, "后到的完成请求重新检查当前状态");
        done("取消与完成同时到达");
    }

    private void 事件已提交SSE中断() {
        Database db = withEvent();
        List<EventRow> page = new SseFeed().read(db, "tenant", "task-1", 0);
        check(page.size() == 1 && "event-1".equals(page.get(0).eventId()), "断线后按游标补读");
        done("事件已提交SSE中断");
    }

    private void 补读过程中又有新事件() {
        Database db = withEvent();
        SseFeed feed = new SseFeed();
        check(feed.read(db, "tenant", "task-1", 0).size() == 1, "先读到已有事件");
        try (Transaction tx = db.begin()) {
            tx.saveEvent(new EventRow("event-2", "task-1", 2, "继续"));
            tx.commit();
        }
        List<EventRow> next = feed.read(db, "tenant", "task-1", 1);
        check(next.size() == 1 && next.get(0).eventSeq() == 2, "下一轮补读新提交的事件");
        done("补读过程中又有新事件");
    }

    private void 游标超出保留期() {
        Database db = withEvent();
        try (Transaction tx = db.begin()) {
            tx.saveEvent(new EventRow("event-2", "task-1", 2, "二"));
            tx.saveEvent(new EventRow("event-3", "task-1", 3, "三"));
            tx.commit();
        }
        boolean resync = false;
        try {
            new SseFeed(1).read(db, "tenant", "task-1", 0);
        } catch (Resync ex) {
            resync = true;
        }
        check(resync, "超出保留期时要求重新同步");
        done("游标超出保留期");
    }

    private void 外部结果无法再核实() {
        Database db = running();
        FakeWrite fake = new FakeWrite();
        ToolGate gate = new ToolGate(new ExternalAction(new Ids()));
        gate.allow("write_row");
        String result;
        try (Transaction tx = db.begin()) {
            result = gate.call(tx, "task-1", 1, "owner-a", "write_row", true, false, "node", "digest", fake, true);
            tx.commit();
        }
        check("HUMAN".equals(result), "无法核实就转人工");
        check(db.task("task-1").state() == TaskState.WAITING_HUMAN, "留下人工处理");
        check(fake.writes() == 0, "没有自动重试");
        done("外部结果无法再核实");
    }

    private void done(String name) {
        passed++;
        System.out.println("通过 " + name);
    }

    private static Database running() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(1_000L)
                    .budgetLimit(20)
                    .build());
            tx.commit();
        }
        return db;
    }

    private static Database withEvent() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant").state(TaskState.RUNNING).epoch(1).build());
            tx.saveEvent(new EventRow("event-1", "task-1", 1, "任务已受理"));
            tx.commit();
        }
        return db;
    }

    private static Held started(boolean commitOffset) {
        Database db = new Database();
        Ids ids = new Ids();
        KafkaLog kafka = new KafkaLog();
        String taskId = new AcceptService(db, ids).accept("tenant", "actor", "key", "digest", 10).taskId();
        new OutboxDispatcher().pump(db, kafka);
        Held held = new Held(db, ids, kafka, taskId);
        held.consume(commitOffset);
        return held;
    }

    private static void check(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }

    private static final class Held {
        private final Database db;
        private final Ids ids;
        private final KafkaLog kafka;
        private final String taskId;
        private final String group = "runtime-workers";
        private final ScheduleConsumer consumer;
        private final rt.LocalSlots slots = new rt.LocalSlots(4, new ArrayList<>());

        private Held(Database db, Ids ids, KafkaLog kafka, String taskId) {
            this.db = db;
            this.ids = ids;
            this.kafka = kafka;
            this.taskId = taskId;
            this.consumer = new ScheduleConsumer(ids);
        }

        private ScheduleConsumer.Outcome consume(boolean commitOffset) {
            return consumer.consumeNext(db, kafka, group, "pod-a", slots, new ArrayList<>(), true, commitOffset);
        }
    }
}
