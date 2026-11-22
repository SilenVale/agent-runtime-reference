package chapter3;

import java.util.ArrayList;
import java.util.List;

import access.AcceptResult;
import access.AcceptService;
import dispatch.OutboxDispatcher;
import rt.LocalSlots;
import rt.RecoveryScanner;
import rt.ScheduleConsumer;
import store.Database;
import store.EventRow;
import store.Ids;
import store.InboxRow;
import store.KafkaLog;
import store.OutboxRow;
import store.QuotaRow;
import store.ScheduleMessage;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter3Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter3Tests tests = new Chapter3Tests();
        tests.同键同参返回原任务且同键异参冲突();
        tests.T1同时提交任务幂等键受理事件和Outbox并返回202();
        tests.初始epoch为0且接管成功后才增加();
        tests.投递器在确认前退出会用原消息ID再投();
        tests.Inbox按逻辑消费方加消息ID去掉重复接管();
        tests.额度不足不授予owner并留下延期调度();
        tests.T2提交前退出允许再消费且offset已提交后改由租约恢复();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 同键同参返回原任务且同键异参冲突() {
        Database db = new Database();
        AcceptService accept = new AcceptService(db, new Ids());
        AcceptResult first = accept.accept("tenant", "actor", "key-1", "digest-a", 100);
        AcceptResult again = accept.accept("tenant", "actor", "key-1", "digest-a", 100);
        AcceptResult clash = accept.accept("tenant", "actor", "key-1", "digest-b", 100);
        check(first.status() == 202 && again.status() == 202, "同键同参仍是202");
        check(first.taskId().equals(again.taskId()), "同键同参返回原任务");
        check(!again.enteredNewTask(), "重复受理不再进入新的T1");
        check(clash.status() == 409 && clash.taskId() == null, "同键异参冲突");
        check(db.tasks().size() == 1, "冲突没有建第二条任务");
        check(db.idempotency().size() == 1, "幂等登记只有一条");
        passed++;
        System.out.println("通过 同键同参返回原任务且同键异参冲突");
    }

    private void T1同时提交任务幂等键受理事件和Outbox并返回202() {
        Database db = new Database();
        Ids ids = new Ids();
        AcceptService accept = new AcceptService(db, ids);
        AcceptResult lost = accept.accept("tenant", "actor", "key-1", "digest-a", 80, false);
        check(lost.status() == 500, "提交前退出没有返回受理");
        check(db.tasks().isEmpty() && db.events().isEmpty() && db.outbox().isEmpty() && db.idempotency().isEmpty(),
                "T1未提交时四样都不存在");
        AcceptResult done = accept.accept("tenant", "actor", "key-1", "digest-a", 80);
        check(done.status() == 202, "返回202");
        TaskRow task = db.task(done.taskId());
        check(task != null, "任务已提交");
        check(db.idempotency().size() == 1, "幂等键已提交");
        check(db.events().size() == 1, "受理事件已提交");
        EventRow event = db.events().iterator().next();
        check(event.eventSeq() == 1 && "任务已受理".equals(event.type()), "事件序号和类型");
        check(db.outbox().size() == 1, "Outbox已提交");
        OutboxRow outbox = db.outbox().iterator().next();
        check(outbox.dispatchSeq() == 1 && outbox.expectedEpoch() == 0 && "START".equals(outbox.scheduleType()),
                "首次调度轮次和预期代次");
        check(!event.eventId().equals(outbox.messageId()), "事件ID和消息ID分开生成");
        AcceptResult retry = accept.accept("tenant", "actor", "key-1", "digest-a", 80);
        check(retry.taskId().equals(done.taskId()) && db.tasks().size() == 1, "响应丢失后原键返回原任务");
        passed++;
        System.out.println("通过 T1同时提交任务幂等键受理事件和Outbox并返回202");
    }

    private void 初始epoch为0且接管成功后才增加() {
        Fixture fixture = acceptedFixture();
        TaskRow created = fixture.db.task(fixture.taskId);
        check(created.epoch() == 0, "初始epoch为0");
        check(created.owner() == null && created.leaseExpiresAt() == null, "受理时没有执行者和租约");
        check(created.state() == TaskState.ACCEPTED && created.spent() == 0, "受理后等待调度且消耗为0");
        fixture.dispatcher.pump(fixture.db, fixture.kafka);
        ScheduleConsumer.Outcome taken = fixture.consume("pod-a", true, false);
        check("TAKEN".equals(taken.kind()), "第一次接管成功");
        check(fixture.db.task(fixture.taskId).epoch() == 1, "接管成功后代次才增加");
        check(fixture.db.task(fixture.taskId).state() == TaskState.RUNNING, "START把任务改成执行中");
        ScheduleConsumer.Outcome again = fixture.consume("pod-b", true, true);
        check("REUSED".equals(again.kind()), "同一条消息重投沿用结论");
        check(fixture.db.task(fixture.taskId).epoch() == 1, "重投不再增加代次");
        passed++;
        System.out.println("通过 初始epoch为0且接管成功后才增加");
    }

    private void 投递器在确认前退出会用原消息ID再投() {
        Fixture fixture = acceptedFixture();
        fixture.kafka.loseNextAck();
        int marked = fixture.dispatcher.pump(fixture.db, fixture.kafka);
        check(marked == 0, "确认前退出不标记已发送");
        check(fixture.kafka.messages().size() == 1, "Kafka已经收下这条消息");
        String messageId = fixture.kafka.messages().get(0).messageId();
        check(fixture.db.pendingOutbox().size() == 1, "Outbox仍待发送");
        fixture.dispatcher.pump(fixture.db, fixture.kafka);
        check(fixture.kafka.messages().size() == 2, "重启后再投一次");
        check(messageId.equals(fixture.kafka.messages().get(1).messageId()), "再投仍用原消息ID");
        check(fixture.db.pendingOutbox().isEmpty(), "确认之后才标记已发送");
        passed++;
        System.out.println("通过 投递器在确认前退出会用原消息ID再投");
    }

    private void Inbox按逻辑消费方加消息ID去掉重复接管() {
        Fixture fixture = acceptedFixture();
        fixture.kafka.loseNextAck();
        fixture.dispatcher.pump(fixture.db, fixture.kafka);
        fixture.dispatcher.pump(fixture.db, fixture.kafka);
        ScheduleConsumer.Outcome first = fixture.consume("pod-a", true, true);
        ScheduleConsumer.Outcome second = fixture.consume("pod-b", true, true);
        check("TAKEN".equals(first.kind()) && "REUSED".equals(second.kind()), "第二次不再接管");
        check(fixture.db.task(fixture.taskId).epoch() == 1, "重复消息没有新代次");
        check("pod-a".equals(fixture.db.task(fixture.taskId).owner()), "执行者仍是第一次接管的Pod");
        int rows = 0;
        for (InboxRow row : fixture.db.inbox()) {
            rows++;
            check(row.consumer().equals(fixture.group), "去重键用消费组");
            check(!row.consumer().equals("pod-a") && !row.consumer().equals("pod-b"), "去重键不用Pod ID");
            check(row.id().equals(fixture.group + "|" + row.messageId()), "键是逻辑消费方加消息ID");
        }
        check(rows == 1, "同一消息只留一条Inbox");
        passed++;
        System.out.println("通过 Inbox按逻辑消费方加消息ID去掉重复接管");
    }

    private void 额度不足不授予owner并留下延期调度() {
        Fixture fixture = acceptedFixture();
        try (Transaction tx = fixture.db.begin()) {
            tx.saveQuota(new QuotaRow("tenant", 0, 0));
            tx.commit();
        }
        fixture.dispatcher.pump(fixture.db, fixture.kafka);
        List<String> trace = fixture.trace;
        ScheduleConsumer.Outcome outcome = fixture.consume("pod-a", true, true);
        check("DEFERRED".equals(outcome.kind()), "额度不足改为延期");
        TaskRow task = fixture.db.task(fixture.taskId);
        check(task.owner() == null && task.state() == TaskState.ACCEPTED && task.epoch() == 0, "不授予owner也不改成执行中");
        int pending = 0;
        for (OutboxRow row : fixture.db.outbox()) {
            if ("PENDING".equals(row.status()) && "DEFER".equals(row.scheduleType())) {
                pending++;
                check(row.expectedEpoch() == 0 && row.dispatchSeq() == 2, "延期是新的调度轮次");
            }
        }
        check(pending == 1, "同一事务留下延期调度");
        check(fixture.slots.held() == 0, "延期后释放本地名额");
        check(trace.indexOf("slot") < trace.indexOf("tx"), "先占名额再开事务");
        passed++;
        System.out.println("通过 额度不足不授予owner并留下延期调度");
    }

    private void T2提交前退出允许再消费且offset已提交后改由租约恢复() {
        Fixture crashed = acceptedFixture();
        crashed.dispatcher.pump(crashed.db, crashed.kafka);
        ScheduleConsumer.Outcome abandoned = crashed.consume("pod-a", false, false);
        check("TAKEN".equals(abandoned.kind()), "事务内曾经准备接管");
        check(crashed.db.task(crashed.taskId).epoch() == 0, "T2提交前退出没有留下代次");
        check(crashed.kafka.offset(crashed.group) == 0, "offset还没提交");
        check(crashed.db.inbox().isEmpty(), "未提交的Inbox不可见");
        ScheduleConsumer.Outcome retried = crashed.consume("pod-b", true, false);
        check("TAKEN".equals(retried.kind()) && crashed.db.task(crashed.taskId).epoch() == 1, "未提交的消息可以再消费");
        check(crashed.kafka.offset(crashed.group) == 0, "这次先不提交offset");
        ScheduleConsumer.Outcome replay = crashed.consume("pod-c", true, true);
        check("REUSED".equals(replay.kind()) && crashed.db.task(crashed.taskId).epoch() == 1, "已提交的接管被Inbox认出来");
        check(crashed.kafka.offset(crashed.group) == 1, "有结论之后才推进offset");

        Fixture handed = acceptedFixture();
        handed.dispatcher.pump(handed.db, handed.kafka);
        handed.consume("pod-a", true, true);
        String original = handed.kafka.messages().get(0).messageId();
        handed.db.advanceClock(ScheduleConsumer.LEASE_MS + 1);
        RecoveryScanner scanner = new RecoveryScanner(handed.ids);
        int registered = scanner.scan(handed.db);
        check(registered == 1, "租约到期后登记恢复");
        check(handed.db.task(handed.taskId).epoch() == 1, "扫描本身不增加代次");
        OutboxRow recovery = null;
        for (OutboxRow row : handed.db.pendingOutbox()) {
            recovery = row;
        }
        check(recovery != null && "RECOVERY".equals(recovery.scheduleType()), "恢复调度已留下");
        check(!original.equals(recovery.messageId()) && recovery.dispatchSeq() == 2, "恢复使用新消息ID和新轮次");
        check(recovery.expectedEpoch() == 1, "恢复携带当前代次");
        check(scanner.scan(handed.db) == 0, "已有待处理调度时不重复登记");
        handed.dispatcher.pump(handed.db, handed.kafka);
        ScheduleConsumer.Outcome continued = handed.consume("pod-b", true, true);
        check("TAKEN".equals(continued.kind()) && handed.db.task(handed.taskId).epoch() == 2, "新执行者从恢复消息接管");
        passed++;
        System.out.println("通过 T2提交前退出允许再消费且offset已提交后改由租约恢复");
    }

    private Fixture acceptedFixture() {
        Database db = new Database();
        Ids ids = new Ids();
        KafkaLog kafka = new KafkaLog();
        AcceptService accept = new AcceptService(db, ids);
        AcceptResult result = accept.accept("tenant", "actor", "key-1", "digest-a", 100);
        List<String> trace = new ArrayList<>();
        return new Fixture(db, ids, kafka, new OutboxDispatcher(), result.taskId(), trace, new LocalSlots(4, trace));
    }

    private static void check(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }

    private static final class Fixture {
        private final Database db;
        private final Ids ids;
        private final KafkaLog kafka;
        private final OutboxDispatcher dispatcher;
        private final String taskId;
        private final List<String> trace;
        private final LocalSlots slots;
        private final String group = "runtime-workers";
        private final ScheduleConsumer consumer;

        private Fixture(
                Database db,
                Ids ids,
                KafkaLog kafka,
                OutboxDispatcher dispatcher,
                String taskId,
                List<String> trace,
                LocalSlots slots) {
            this.db = db;
            this.ids = ids;
            this.kafka = kafka;
            this.dispatcher = dispatcher;
            this.taskId = taskId;
            this.trace = trace;
            this.slots = slots;
            this.consumer = new ScheduleConsumer(ids);
        }

        private ScheduleConsumer.Outcome consume(String podId, boolean commitTx, boolean commitOffset) {
            return consumer.consumeNext(db, kafka, group, podId, slots, trace, commitTx, commitOffset);
        }
    }
}
