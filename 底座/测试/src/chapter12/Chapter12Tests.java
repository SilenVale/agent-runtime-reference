package chapter12;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import access.AcceptService;
import access.SseFeed;
import dispatch.OutboxDispatcher;
import fake.FakeWrite;
import rt.ExternalAction;
import rt.LocalSlots;
import rt.Locator;
import rt.RecoveryScanner;
import rt.ScannerLeader;
import rt.ScheduleConsumer;
import store.Database;
import store.EventRow;
import store.Ids;
import store.KafkaLog;
import store.OutboxRow;
import store.Rejected;
import store.ScheduleMessage;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter12Tests {
    private int passed;

    public static void main(String[] args) throws Exception {
        Chapter12Tests tests = new Chapter12Tests();
        tests.T1提交后投递器退出();
        tests.T2接管后确认前退出();
        tests.确认后图启动前退出();
        tests.消费组再均衡();
        tests.恢复消息重复以及旧轮次迟到();
        tests.多个扫描实例重叠时不重复登记();
        tests.旧Worker恢复后不能回写();
        tests.第一个Checkpoint还没生成();
        tests.SSE断线后按游标补读();
        tests.只有有效代次能提交结果();
        tests.业务写不重复();
        tests.消息或租约都有后续路径();
        tests.Worker全部退出时恢复扫描仍能登记Outbox();
        tests.恢复扫描主实例退出不重复登记且次数不重置();
        tests.结果不明的写不直接重做();
        tests.三份部署清单只包含三个进程();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void T1提交后投递器退出() {
        Database db = new Database();
        KafkaLog kafka = new KafkaLog();
        new AcceptService(db, new Ids()).accept("tenant", "actor", "key", "digest", 10);
        kafka.loseNextAck();
        new OutboxDispatcher().pump(db, kafka);
        check(db.pendingOutbox().size() == 1 && kafka.messages().size() == 1, "投递器退出后原消息还在");
        done("T1提交后投递器退出");
    }

    private void T2接管后确认前退出() {
        Held held = accepted();
        ScheduleConsumer.Outcome abandoned = held.consume("pod-a", false, false);
        check("TAKEN".equals(abandoned.kind()) && held.db.task(held.taskId).epoch() == 0, "确认前退出没有留下代次");
        ScheduleConsumer.Outcome again = held.consume("pod-b", true, true);
        check("TAKEN".equals(again.kind()) && held.db.task(held.taskId).epoch() == 1, "同一条消息可以再被接管");
        done("T2接管后确认前退出");
    }

    private void 确认后图启动前退出() {
        Held held = accepted();
        held.consume("pod-a", true, true);
        check(held.db.checkpoint(held.db.task(held.taskId).threadId()) == null, "图还没启动");
        held.db.advanceClock(ScheduleConsumer.LEASE_MS + 1);
        check(new RecoveryScanner(held.ids).scan(held.db) == 1, "之后改由租约恢复");
        done("确认后图启动前退出");
    }

    private void 消费组再均衡() {
        Held held = accepted();
        held.consume("pod-a", true, true);
        int offset = held.kafka.offset(held.group);
        ScheduleConsumer.Outcome moved = held.consume("pod-b", true, true);
        check("EMPTY".equals(moved.kind()), "新成员从已提交位移继续");
        check(held.kafka.offset(held.group) == offset, "再均衡不把位移拨回零");
        check(held.db.task(held.taskId).epoch() == 1, "没有因此再接管一次");
        done("消费组再均衡");
    }

    private void 恢复消息重复以及旧轮次迟到() {
        Held held = accepted();
        held.consume("pod-a", true, true);
        String original = held.kafka.messages().get(0).messageId();
        held.db.advanceClock(ScheduleConsumer.LEASE_MS + 1);
        RecoveryScanner scanner = new RecoveryScanner(held.ids);
        scanner.scan(held.db);
        new OutboxDispatcher().pump(held.db, held.kafka);
        held.consume("pod-b", true, true);
        ScheduleMessage recovery = held.kafka.messages().get(held.kafka.messages().size() - 1);
        held.kafka.append(recovery);
        ScheduleConsumer.Outcome replay = held.consume("pod-c", true, true);
        check("REUSED".equals(replay.kind()), "恢复消息重复不再接管");
        held.kafka.append(new ScheduleMessage(held.ids.next("late"), held.taskId, 1, 0, "START", "v1"));
        ScheduleConsumer.Outcome late = held.consume("pod-old", true, true);
        check("IGNORED".equals(late.kind()), "旧轮次迟到被忽略");
        check(held.db.task(held.taskId).epoch() == 2, "代次没有被旧轮次推高");
        done("恢复消息重复以及旧轮次迟到");
    }

    private void 多个扫描实例重叠时不重复登记() {
        Database db = running(1L);
        db.advanceClock(1L);
        Ids ids = new Ids();
        check(new RecoveryScanner(ids).scan(db) == 1, "第一个扫描实例登记");
        check(new RecoveryScanner(ids).scan(db) == 0, "重叠的扫描实例不重复登记");
        done("多个扫描实例重叠时不重复登记");
    }

    private void 旧Worker恢复后不能回写() {
        Database db = running(1_000L);
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            new TaskMachine().renew(tx, "task-1", 0, "owner-old", 9_000L);
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("旧代次");
        }
        check(rejected && db.task("task-1").leaseExpiresAt() == 1_000L, "旧Worker不能续上自己的租约");
        done("旧Worker恢复后不能回写");
    }

    private void 第一个Checkpoint还没生成() {
        Database db = running(1_000L);
        check("input:task-1".equals(new Locator().locate(db, "task-1")), "没有存档时回到已保存的输入");
        try (Transaction tx = db.begin()) {
            tx.saveOperation(new store.OperationRow("op-1", "task-1", "SUCCEEDED"));
            tx.commit();
        }
        check("ledger:op-1".equals(new Locator().locate(db, "task-1")), "有操作账本时先看账本");
        done("第一个Checkpoint还没生成");
    }

    private void SSE断线后按游标补读() {
        Database db = running(1_000L);
        try (Transaction tx = db.begin()) {
            tx.saveEvent(new EventRow("event-1", "task-1", 1, "任务已受理"));
            tx.saveEvent(new EventRow("event-2", "task-1", 2, "已接管"));
            tx.commit();
        }
        List<EventRow> page = new SseFeed().read(db, "tenant", "task-1", 1);
        check(page.size() == 1 && page.get(0).eventSeq() == 2, "断线后按游标补读");
        done("SSE断线后按游标补读");
    }

    private void 只有有效代次能提交结果() {
        Database db = running(1_000L);
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            new TaskMachine().submit(tx, "task-1", 1, "someone-else", TaskState.SUCCEEDED);
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("旧代次");
        }
        check(rejected && db.task("task-1").state() == TaskState.RUNNING, "只有当前执行者能提交结果");
        done("只有有效代次能提交结果");
    }

    private void 业务写不重复() {
        FakeWrite fake = new FakeWrite();
        fake.write("op-1", "digest", "done");
        fake.write("op-1", "digest", "done");
        check(fake.writes() == 1 && "done".equals(fake.query("op-1")), "同一个operation_id不写第二次");
        done("业务写不重复");
    }

    private void 消息或租约都有后续路径() {
        Database db = new Database();
        KafkaLog kafka = new KafkaLog();
        kafka.loseNextAck();
        new AcceptService(db, new Ids()).accept("tenant", "actor", "key", "digest", 10);
        new OutboxDispatcher().pump(db, kafka);
        check(db.pendingOutbox().size() == 1, "没确认的消息还有待发送记录");
        Database leased = running(1L);
        leased.advanceClock(1L);
        check(new RecoveryScanner(new Ids()).scan(leased) == 1, "到期租约会登记下一次调度");
        done("消息或租约都有后续路径");
    }

    private void Worker全部退出时恢复扫描仍能登记Outbox() throws Exception {
        Database db = running(1L);
        db.advanceClock(1L);
        check(!new LocalSlots(0, new ArrayList<>()).tryAcquire(), "执行槽位可以是零");
        check(new RecoveryScanner(new Ids()).scan(db) == 1, "Worker退出后扫描仍能登记");
        String runtime = Files.readString(Path.of("deploy/kubernetes/runtime.yaml"));
        check(runtime.contains("worker-and-recovery-scan"), "恢复扫描跟Runtime进程在一起");
        done("Worker全部退出时恢复扫描仍能登记Outbox");
    }

    private void 恢复扫描主实例退出不重复登记且次数不重置() {
        ScannerLeader leader = new ScannerLeader();
        check(leader.claim("scanner-a", 0L), "主实例持有扫描权");
        leader.recordAttempt();
        leader.recordAttempt();
        check(!leader.claim("scanner-b", 1_000L), "租约未到期时另一个实例不能抢走");
        check(leader.claim("scanner-b", ScannerLeader.LEASE_MS), "主实例退出后可以切换");
        check(leader.attempts() == 2, "切换不把已有次数清零");
        Database db = running(1L);
        db.advanceClock(1L);
        Ids ids = new Ids();
        check(new RecoveryScanner(ids).scan(db) == 1, "切换后的扫描只登记一次");
        check(new RecoveryScanner(ids).scan(db) == 0, "短时重叠不重复登记");
        done("恢复扫描主实例退出不重复登记且次数不重置");
    }

    private void 结果不明的写不直接重做() {
        FakeWrite fake = new FakeWrite();
        String body = fake.write("op-7", "digest", "done");
        String queried = fake.query("op-7");
        check(body.equals(queried) && fake.writes() == 1, "查到结果就不再写");
        Database db = running(1_000L);
        rt.ToolGate gate = new rt.ToolGate(new ExternalAction(new Ids()));
        gate.allow("write_row");
        try (Transaction tx = db.begin()) {
            String result = gate.call(tx, "task-1", 1, "owner-a", "write_row", true, false, "node", "digest", fake, true);
            tx.commit();
            check("HUMAN".equals(result), "不能核实就停止自动重试");
        }
        check(fake.writes() == 1, "不明结果没有再写一笔");
        done("结果不明的写不直接重做");
    }

    private void 三份部署清单只包含三个进程() throws Exception {
        check(Files.exists(Path.of("deploy/kubernetes/access.yaml")), "有接入层清单");
        check(Files.exists(Path.of("deploy/kubernetes/dispatch.yaml")), "有消息投递清单");
        check(Files.exists(Path.of("deploy/kubernetes/runtime.yaml")), "有Runtime清单");
        check(!Files.exists(Path.of("deploy/kubernetes/fake.yaml")), "假业务不在这三份清单里");
        done("三份部署清单只包含三个进程");
    }

    private void done(String name) {
        passed++;
        System.out.println("通过 " + name);
    }

    private static Database running(long leaseUntil) {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(leaseUntil)
                    .budgetLimit(10)
                    .build());
            tx.commit();
        }
        return db;
    }

    private static Held accepted() {
        Database db = new Database();
        Ids ids = new Ids();
        KafkaLog kafka = new KafkaLog();
        String taskId = new AcceptService(db, ids).accept("tenant", "actor", "key", "digest", 10).taskId();
        new OutboxDispatcher().pump(db, kafka);
        return new Held(db, ids, kafka, taskId);
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
        private final LocalSlots slots = new LocalSlots(4, new ArrayList<>());

        private Held(Database db, Ids ids, KafkaLog kafka, String taskId) {
            this.db = db;
            this.ids = ids;
            this.kafka = kafka;
            this.taskId = taskId;
            this.consumer = new ScheduleConsumer(ids);
        }

        private ScheduleConsumer.Outcome consume(String pod, boolean commitTx, boolean commitOffset) {
            return consumer.consumeNext(db, kafka, group, pod, slots, new ArrayList<>(), commitTx, commitOffset);
        }
    }
}
