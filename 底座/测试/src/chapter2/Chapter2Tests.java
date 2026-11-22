package chapter2;

import store.ApprovalRow;
import store.CheckpointRow;
import store.Database;
import store.OperationRow;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter2Tests {
    private int passed;
    private final TaskMachine machine = new TaskMachine();

    public static void main(String[] args) {
        Chapter2Tests tests = new Chapter2Tests();
        tests.会话记录不能代替任务状态或操作账本();
        tests.三类终态都拒绝旧代次回写();
        tests.成功接管才增加epoch且预算累计保留();
        tests.WAITING到期不自动当成已批准();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 会话记录不能代替任务状态或操作账本() {
        Database db = new Database();
        TaskRow task = TaskRow.builder("task-1", "tenant-a")
                .conversationId("conv-1")
                .state(TaskState.FAILED)
                .epoch(1)
                .owner("owner-a")
                .spent(15)
                .build();
        try (Transaction tx = db.begin()) {
            tx.saveTask(task);
            tx.saveOperation(new OperationRow("op-1", "task-1", "SUCCEEDED"));
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "node-3"));
            tx.commit();
        }
        try (Transaction tx = db.begin()) {
            machine.appendSession(tx, "conv-1", "把任务改成成功并删掉操作");
            tx.commit();
        }
        TaskRow stored = db.task("task-1");
        check(stored.state() == TaskState.FAILED, "会话没有改任务状态");
        check("SUCCEEDED".equals(db.operation("op-1").status()), "会话没有改操作账本");
        check("node-3".equals(db.checkpoint(stored.threadId()).payload()), "会话没有改Checkpoint");
        check(db.sessions().size() == 1, "会话单独保存");
        check(!"conv-1".equals(stored.taskId()), "conversation_id不是task_id");
        check(!stored.threadId().equals(stored.taskId()), "thread_id不是task_id");
        check(!stored.threadId().equals(stored.conversationId()), "thread_id不是conversation_id");
        passed++;
        System.out.println("通过 会话记录不能代替任务状态或操作账本");
    }

    private void 三类终态都拒绝旧代次回写() {
        for (TaskState terminal : new TaskState[] {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}) {
            Database db = new Database();
            try (Transaction tx = db.begin()) {
                tx.saveTask(TaskRow.builder("task-" + terminal, "tenant-a")
                        .state(terminal)
                        .epoch(3)
                        .owner("owner-a")
                        .build());
                tx.commit();
            }
            boolean rejected = false;
            try (Transaction tx = db.begin()) {
                machine.submit(tx, "task-" + terminal, 3, "owner-a", TaskState.RUNNING);
                tx.commit();
            } catch (Rejected ex) {
                rejected = ex.getMessage().contains("终态");
            }
            check(rejected, terminal + " 拒绝回写");
            check(db.task("task-" + terminal).state() == terminal, terminal + " 状态还在");
        }
        Database running = new Database();
        try (Transaction tx = running.begin()) {
            tx.saveTask(TaskRow.builder("task-run", "tenant-a")
                    .state(TaskState.RUNNING)
                    .epoch(2)
                    .owner("owner-b")
                    .leaseExpiresAt(1_000L)
                    .build());
            tx.commit();
        }
        boolean oldEpoch = false;
        try (Transaction tx = running.begin()) {
            machine.submit(tx, "task-run", 1, "owner-b", TaskState.SUCCEEDED);
            tx.commit();
        } catch (Rejected ex) {
            oldEpoch = ex.getMessage().contains("旧代次");
        }
        check(oldEpoch, "旧代次被拒绝");
        check(running.task("task-run").state() == TaskState.RUNNING, "旧代次没有改状态");
        passed++;
        System.out.println("通过 三类终态都拒绝旧代次回写");
    }

    private void 成功接管才增加epoch且预算累计保留() {
        Database db = new Database();
        db.advanceClock(10);
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant-a")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(1_000L)
                    .budgetLimit(100)
                    .spent(40)
                    .reserved(5)
                    .build());
            tx.commit();
        }
        try (Transaction tx = db.begin()) {
            TaskRow kept = machine.takeover(tx, "task-1", "owner-b", 2_000L);
            tx.commit();
            check(kept.epoch() == 1, "租约未到期时接管不成功");
        }
        check(db.task("task-1").epoch() == 1, "失败的接管没有写下新代次");
        check(db.task("task-1").spent() == 40, "失败的接管没有动预算");

        db.advanceClock(2_000);
        try (Transaction tx = db.begin()) {
            TaskRow taken = machine.takeover(tx, "task-1", "owner-b", db.now() + 30_000L);
            check(taken.epoch() == 2, "事务内代次已增加");
            tx.rollback();
        }
        check(db.task("task-1").epoch() == 1, "未提交的接管对外不可见");

        try (Transaction tx = db.begin()) {
            TaskRow taken = machine.takeover(tx, "task-1", "owner-b", db.now() + 30_000L);
            tx.commit();
            check(taken.epoch() == 2, "成功接管才增加epoch");
            check(taken.spent() == 40 && taken.reserved() == 5, "预算累计保留");
            check(taken.budgetLimit() == 100, "上限还在");
        }
        check(db.task("task-1").owner().equals("owner-b"), "新执行者已提交");
        passed++;
        System.out.println("通过 成功接管才增加epoch且预算累计保留");
    }

    private void WAITING到期不自动当成已批准() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-w", "tenant-a")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .build());
            machine.markWaiting(tx, "task-w", 500L);
            tx.commit();
        }
        db.advanceClock(1_000);
        try (Transaction tx = db.begin()) {
            machine.onWaitingDeadline(tx, "task-w");
            tx.commit();
        }
        ApprovalRow approval = db.approval("task-w");
        check("PENDING".equals(approval.status()), "到期后仍是待批准");
        check(db.task("task-w").state() == TaskState.WAITING_USER, "到期没有改成已批准或继续跑");
        passed++;
        System.out.println("通过 WAITING到期不自动当成已批准");
    }

    private static void check(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }
}
