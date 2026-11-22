package chapter7;

import fake.FakeWrite;
import rt.RecoveryScanner;
import rt.TaskControl;
import store.Database;
import store.Ids;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter7Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter7Tests tests = new Chapter7Tests();
        tests.租约到期才登记恢复且已有调度时不重复登记();
        tests.结果不明到时限后转人工且不自动重做();
        tests.人工等待到期不自动批准();
        tests.取消不把已发出的假写当成已回滚();
        tests.终态任务不会被恢复扫描重新启动();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 租约到期才登记恢复且已有调度时不重复登记() {
        Database db = running(5_000L);
        RecoveryScanner scanner = new RecoveryScanner(new Ids());
        check(scanner.scan(db) == 0, "租约没到期不登记");
        db.advanceClock(5_000L);
        check(scanner.scan(db) == 1, "到期后登记一条恢复");
        check(scanner.scan(db) == 0, "已有调度时不重复登记");
        check(db.task("task-1").epoch() == 1, "扫描不增加代次");
        passed++;
        System.out.println("通过 租约到期才登记恢复且已有调度时不重复登记");
    }

    private void 结果不明到时限后转人工且不自动重做() {
        Database db = running(5_000L);
        FakeWrite fake = new FakeWrite();
        TaskControl control = new TaskControl();
        try (Transaction tx = db.begin()) {
            for (int i = 0; i < 4; i++) {
                control.noteUnknown(tx, "task-1", 5);
            }
            tx.commit();
        }
        check(db.task("task-1").state() == TaskState.RECONCILING, "次数未到仍在核对");
        check(db.task("task-1").state() != TaskState.FAILED, "结果不明不直接失败");
        try (Transaction tx = db.begin()) {
            control.noteUnknown(tx, "task-1", 5);
            tx.commit();
        }
        check(db.task("task-1").state() == TaskState.WAITING_HUMAN, "到次数后转人工");
        check(fake.writes() == 0, "转人工时没有自动重做");
        passed++;
        System.out.println("通过 结果不明到时限后转人工且不自动重做");
    }

    private void 人工等待到期不自动批准() {
        Database db = running(5_000L);
        TaskMachine machine = new TaskMachine();
        try (Transaction tx = db.begin()) {
            machine.markWaiting(tx, "task-1", 100L);
            tx.commit();
        }
        db.advanceClock(500L);
        try (Transaction tx = db.begin()) {
            machine.onWaitingDeadline(tx, "task-1");
            tx.commit();
        }
        check("PENDING".equals(db.approval("task-1").status()), "到期仍待批准");
        check(db.task("task-1").owner() == null, "等待时执行者已释放");
        passed++;
        System.out.println("通过 人工等待到期不自动批准");
    }

    private void 取消不把已发出的假写当成已回滚() {
        Database db = running(5_000L);
        FakeWrite fake = new FakeWrite();
        fake.write("op-1", "digest", "kept");
        TaskControl control = new TaskControl();
        try (Transaction tx = db.begin()) {
            control.requestCancel(tx, "task-1");
            tx.commit();
        }
        check(db.task("task-1").state() == TaskState.CANCEL_REQUESTED, "取消请求已留下");
        check("kept".equals(fake.query("op-1")), "假写结果还在");
        boolean blocked = false;
        try (Transaction tx = db.begin()) {
            control.finishCancel(tx, "task-1", 1, "owner-a", false);
            tx.commit();
        } catch (Rejected ex) {
            blocked = ex.getMessage().contains("结果不明");
        }
        check(blocked, "业务结果不明时不能标成已取消");
        try (Transaction tx = db.begin()) {
            control.finishCancel(tx, "task-1", 1, "owner-a", true);
            tx.commit();
        }
        check(db.task("task-1").state() == TaskState.CANCELLED, "确认之后才进入已取消");
        check("kept".equals(fake.query("op-1")) && fake.writes() == 1, "取消没有回滚假写");
        passed++;
        System.out.println("通过 取消不把已发出的假写当成已回滚");
    }

    private void 终态任务不会被恢复扫描重新启动() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.SUCCEEDED)
                    .epoch(2)
                    .owner("owner-a")
                    .leaseExpiresAt(1L)
                    .build());
            tx.commit();
        }
        db.advanceClock(10L);
        check(new RecoveryScanner(new Ids()).scan(db) == 0, "终态不登记恢复");
        check(db.task("task-1").state() == TaskState.SUCCEEDED, "终态没有被重新启动");
        passed++;
        System.out.println("通过 终态任务不会被恢复扫描重新启动");
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
