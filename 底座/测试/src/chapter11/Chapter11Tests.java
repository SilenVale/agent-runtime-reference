package chapter11;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import fake.FakeWrite;
import rt.ToolGate;
import rt.ExternalAction;
import store.Database;
import store.Ids;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter11Tests {
    private int passed;

    public static void main(String[] args) throws Exception {
        Chapter11Tests tests = new Chapter11Tests();
        tests.假写只接受operation_id并且同号不写第二次();
        tests.假写模块不能修改任务执行权();
        tests.缺少查询能力的写在超时后进入人工而不是自动重做();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 假写只接受operation_id并且同号不写第二次() {
        FakeWrite fake = new FakeWrite();
        check("done".equals(fake.write("op-1", "digest", "done")), "第一次按编号写下");
        check("done".equals(fake.write("op-1", "digest", "other")), "同号返回已有结果");
        check(fake.writes() == 1, "同号不写第二次");
        check("done".equals(fake.query("op-1")), "可以按编号查询");
        passed++;
        System.out.println("通过 假写只接受operation_id并且同号不写第二次");
    }

    private void 假写模块不能修改任务执行权() throws Exception {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(1_000L)
                    .build());
            tx.commit();
        }
        FakeWrite fake = new FakeWrite();
        boolean refused = false;
        try {
            fake.saveExecutionRight("task-1", "other", 9);
        } catch (IllegalStateException ex) {
            refused = true;
        }
        fake.write("op-9", "digest", "body");
        check(refused, "假写拒绝保存执行权");
        check(db.task("task-1").epoch() == 1 && "owner-a".equals(db.task("task-1").owner()), "假写没有改任务");
        try (Stream<Path> files = Files.walk(Path.of("假业务"))) {
            for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                String text = Files.readString(file);
                check(!text.contains("import store"), file + " 不引用底座运行库");
            }
        }
        passed++;
        System.out.println("通过 假写模块不能修改任务执行权");
    }

    private void 缺少查询能力的写在超时后进入人工而不是自动重做() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(1_000L)
                    .budgetLimit(10)
                    .build());
            tx.commit();
        }
        FakeWrite fake = new FakeWrite();
        ToolGate gate = new ToolGate(new ExternalAction(new Ids()));
        gate.allow("write_row");
        String result;
        try (Transaction tx = db.begin()) {
            result = gate.call(tx, "task-1", 1, "owner-a", "write_row", true, false, "node", "digest", fake, true);
            tx.commit();
        }
        check("HUMAN".equals(result), "缺少查询能力时转人工");
        check(db.task("task-1").state() == TaskState.WAITING_HUMAN, "留下人工处理");
        check(fake.writes() == 0, "没有自动重做");
        passed++;
        System.out.println("通过 缺少查询能力的写在超时后进入人工而不是自动重做");
    }

    private static void check(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }
}
