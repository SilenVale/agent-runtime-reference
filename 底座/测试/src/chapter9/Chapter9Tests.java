package chapter9;

import java.util.ArrayList;

import access.AcceptService;
import dispatch.OutboxDispatcher;
import fake.FakeWrite;
import rt.Drainer;
import rt.ExternalAction;
import rt.LocalSlots;
import rt.RecoveryScanner;
import rt.ScheduleConsumer;
import store.CheckpointRow;
import store.Database;
import store.Ids;
import store.KafkaLog;
import store.QuotaRow;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter9Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter9Tests tests = new Chapter9Tests();
        tests.执行槽位为0时恢复扫描仍能登记Outbox();
        tests.缩容时未完成交接的任务仍可从恢复点继续();
        tests.单租户额度不足时不占住其他任务的接管();
        tests.灾备后结果不明的写不直接重做();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 执行槽位为0时恢复扫描仍能登记Outbox() {
        Database db = running(1L);
        db.advanceClock(1L);
        LocalSlots slots = new LocalSlots(0, new ArrayList<>());
        check(!slots.tryAcquire(), "没有执行槽位");
        check(new RecoveryScanner(new Ids()).scan(db) == 1, "扫描仍然登记Outbox");
        passed++;
        System.out.println("通过 执行槽位为0时恢复扫描仍能登记Outbox");
    }

    private void 缩容时未完成交接的任务仍可从恢复点继续() {
        Database db = running(1_000L);
        try (Transaction tx = db.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "node-4"));
            tx.commit();
        }
        Drainer drainer = new Drainer();
        drainer.stopNewTakeover();
        drainer.handoff(db, "task-1", 1, "owner-a", "task-1:epoch-1");
        check(!drainer.accepting(), "缩容后不再接新的接管");
        check("task-1:epoch-1".equals(db.task("task-1").recoveryRef()), "恢复点已留下");
        db.advanceClock(1L);
        try (Transaction tx = db.begin()) {
            new store.TaskMachine().takeover(tx, "task-1", "owner-b", db.now() + 30_000L);
            tx.commit();
        }
        check(db.task("task-1").epoch() == 2, "另一个执行者可以继续");
        check("task-1:epoch-1".equals(db.task("task-1").recoveryRef()), "继续时恢复引用还在");
        passed++;
        System.out.println("通过 缩容时未完成交接的任务仍可从恢复点继续");
    }

    private void 单租户额度不足时不占住其他任务的接管() {
        Database db = new Database();
        Ids ids = new Ids();
        KafkaLog kafka = new KafkaLog();
        AcceptService accept = new AcceptService(db, ids);
        String taskA = accept.accept("tenant-a", "actor", "key-a", "digest", 20).taskId();
        String taskB = accept.accept("tenant-b", "actor", "key-b", "digest", 20).taskId();
        try (Transaction tx = db.begin()) {
            tx.saveQuota(new QuotaRow("tenant-a", 0, 0));
            tx.commit();
        }
        new OutboxDispatcher().pump(db, kafka);
        ScheduleConsumer consumer = new ScheduleConsumer(ids);
        java.util.List<String> trace = new ArrayList<>();
        LocalSlots slots = new LocalSlots(4, trace);
        ScheduleConsumer.Outcome first = consumer.consumeNext(db, kafka, "runtime-workers", "pod-a", slots, trace, true, true);
        ScheduleConsumer.Outcome second = consumer.consumeNext(db, kafka, "runtime-workers", "pod-a", slots, trace, true, true);
        String kinds = first.kind() + "," + second.kind();
        check(kinds.contains("DEFERRED") && kinds.contains("TAKEN"), "额度不足只挡住自己，另一个租户仍能接管");
        check(db.task(taskA).epoch() == 0 && db.task(taskB).epoch() == 1, "延期没有占住别人的任务");
        passed++;
        System.out.println("通过 单租户额度不足时不占住其他任务的接管");
    }

    private void 灾备后结果不明的写不直接重做() {
        Database db = running(5_000L);
        ExternalAction action = new ExternalAction(new Ids());
        FakeWrite fake = new FakeWrite();
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node-a", "digest");
            tx.commit();
        }
        check(action.invoke(fake, operationId, "digest", "done", true) == null, "结果一度不明");
        String found = action.invoke(fake, operationId, "digest", "done", false);
        check("done".equals(found) && fake.writes() == 1, "恢复后按原编号复用，不直接重做");
        check(db.task("task-1").state() != TaskState.FAILED, "结果不明没有被标成失败");
        passed++;
        System.out.println("通过 灾备后结果不明的写不直接重做");
    }

    private static Database running(long leaseUntil) {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(leaseUntil)
                    .budgetLimit(100)
                    .build());
            tx.commit();
        }
        return db;
    }

    private static void check(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }
}
