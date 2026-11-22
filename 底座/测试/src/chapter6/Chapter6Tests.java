package chapter6;

import fake.FakeWrite;
import rt.ExternalAction;
import rt.ResultWriter;
import store.CheckpointRow;
import store.Database;
import store.Ids;
import store.Rejected;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter6Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter6Tests tests = new Chapter6Tests();
        tests.T3提交前不允许调用假写();
        tests.T4成功而T5未写时按原operation_id复用结果();
        tests.T5与T6分开提交且图位置落后时不重做已确认的写();
        tests.同一定位换参数被拒绝();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void T3提交前不允许调用假写() {
        Database db = running();
        ExternalAction action = new ExternalAction(new Ids());
        FakeWrite fake = new FakeWrite();
        try (Transaction tx = db.begin()) {
            action.prepare(tx, "task-1", 1, "owner-a", "node-a", "digest-x");
            check(fake.writes() == 0, "事务还没提交就不能调用");
            tx.rollback();
        }
        check(db.operationForAny("task-1") == null && fake.writes() == 0, "T3回滚后没有调用");
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node-a", "digest-x");
            tx.commit();
        }
        check(fake.writes() == 0, "T3提交后仍由调用方决定何时发出");
        fake.write(operationId, "digest-x", "done");
        check(fake.writes() == 1, "T3提交之后才写假业务");
        passed++;
        System.out.println("通过 T3提交前不允许调用假写");
    }

    private void T4成功而T5未写时按原operation_id复用结果() {
        Database db = running();
        Ids ids = new Ids();
        ExternalAction action = new ExternalAction(ids);
        FakeWrite fake = new FakeWrite();
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node-a", "digest-x");
            tx.commit();
        }
        String lost = action.invoke(fake, operationId, "digest-x", "done", true);
        check(lost == null && fake.writes() == 1, "假写已提交，运行库还没记下结果");
        check("PLANNED".equals(db.operation(operationId).status()), "T5还没写");
        String reused = action.invoke(fake, operationId, "digest-x", "done", false);
        try (Transaction tx = db.begin()) {
            new ResultWriter(ids).record(tx, "task-1", 1, "owner-a", operationId, "SUCCEEDED");
            tx.commit();
        }
        check("done".equals(reused) && fake.writes() == 1, "按原编号复用，没有第二笔假写");
        check("SUCCEEDED".equals(db.operation(operationId).status()), "T5补记结果");
        passed++;
        System.out.println("通过 T4成功而T5未写时按原operation_id复用结果");
    }

    private void T5与T6分开提交且图位置落后时不重做已确认的写() {
        Database db = running();
        Ids ids = new Ids();
        ExternalAction action = new ExternalAction(ids);
        FakeWrite fake = new FakeWrite();
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node-a", "digest-x");
            tx.commit();
        }
        fake.write(operationId, "digest-x", "done");
        try (Transaction tx = db.begin()) {
            new ResultWriter(ids).record(tx, "task-1", 1, "owner-a", operationId, "SUCCEEDED");
            tx.commit();
        }
        try (Transaction tx = db.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "still-at-start"));
            tx.rollback();
        }
        check(db.checkpoint("task-1:epoch-1") == null, "图位置可以落后");
        check("SUCCEEDED".equals(db.operation(operationId).status()), "结果确认还在");
        String again = action.invoke(fake, operationId, "digest-x", "done", false);
        check("done".equals(again) && fake.writes() == 1, "图位置落后也不重做已确认的写");
        passed++;
        System.out.println("通过 T5与T6分开提交且图位置落后时不重做已确认的写");
    }

    private void 同一定位换参数被拒绝() {
        Database db = running();
        ExternalAction action = new ExternalAction(new Ids());
        String left;
        String right;
        try (Transaction tx = db.begin()) {
            left = action.prepare(tx, "task-1", 1, "owner-a", "node-a", "same-digest");
            right = action.prepare(tx, "task-1", 1, "owner-a", "node-b", "same-digest");
            tx.commit();
        }
        check(!left.equals(right), "参数摘要相同也不能充当动作标识");
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            action.prepare(tx, "task-1", 1, "owner-a", "node-a", "other-digest");
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("换参数");
        }
        check(rejected, "同一定位换参数被拒绝");
        passed++;
        System.out.println("通过 同一定位换参数被拒绝");
    }

    private static Database running() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(1_000L)
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
