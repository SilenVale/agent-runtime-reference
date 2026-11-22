package chapter5;

import java.util.List;

import access.AcceptService;
import access.InboundGateway;
import fake.FakeWrite;
import rt.ExternalAction;
import rt.ToolGate;
import store.Database;
import store.Ids;
import store.TaskRow;
import store.TaskRow;
import store.TaskState;
import store.Transaction;
import store.Rejected;

public final class Chapter5Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter5Tests tests = new Chapter5Tests();
        tests.未进工具闸门的调用不能发出();
        tests.写工具超时按原operation_id查询且不自动重写();
        tests.入站断线不取消已受理任务();
        tests.工具发现结果未过闸门不能成为可调用工具();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 未进工具闸门的调用不能发出() {
        Database db = running();
        FakeWrite fake = new FakeWrite();
        ToolGate gate = new ToolGate(new ExternalAction(new Ids()));
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            gate.call(tx, "task-1", 1, "owner-a", "write_row", true, true, "node", "digest", fake, false);
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("闸门");
        }
        check(rejected && fake.writes() == 0, "没进闸门的调用没有发出");
        passed++;
        System.out.println("通过 未进工具闸门的调用不能发出");
    }

    private void 写工具超时按原operation_id查询且不自动重写() {
        Database db = running();
        Ids ids = new Ids();
        FakeWrite fake = new FakeWrite();
        ToolGate gate = new ToolGate(new ExternalAction(ids));
        gate.allow("write_row");
        String first;
        try (Transaction tx = db.begin()) {
            first = gate.call(tx, "task-1", 1, "owner-a", "write_row", true, true, "node", "digest", fake, true);
            tx.commit();
        }
        check("UNKNOWN".equals(first) && fake.writes() == 0, "超时且查无结果时不自动重写");
        String operationId = db.operationForAny("task-1");
        fake.write(operationId, "digest", "done");
        String second;
        try (Transaction tx = db.begin()) {
            second = gate.call(tx, "task-1", 1, "owner-a", "write_row", true, true, "node", "digest", fake, true);
            tx.commit();
        }
        check("done".equals(second) && fake.writes() == 1, "查到原编号后直接复用");
        passed++;
        System.out.println("通过 写工具超时按原operation_id查询且不自动重写");
    }

    private void 入站断线不取消已受理任务() {
        Database db = new Database();
        InboundGateway inbound = new InboundGateway(new AcceptService(db, new Ids()));
        String taskId = inbound.submit("tenant", "actor", "key", "digest", 50).taskId();
        TaskRow after = inbound.disconnect(db, taskId);
        check(after.state() == TaskState.ACCEPTED, "断线后任务仍是已受理");
        check(after.state() != TaskState.CANCELLED, "断线没有取消任务");
        passed++;
        System.out.println("通过 入站断线不取消已受理任务");
    }

    private void 工具发现结果未过闸门不能成为可调用工具() {
        ToolGate gate = new ToolGate(new ExternalAction(new Ids()));
        gate.allow("read_doc");
        List<String> visible = gate.visible(List.of("read_doc", "secret_tool"));
        check(visible.equals(List.of("read_doc")), "发现结果要再过允许名单");
        Database db = running();
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            gate.call(tx, "task-1", 1, "owner-a", "secret_tool", false, true, "node", "digest", new FakeWrite(), false);
            tx.commit();
        } catch (Rejected ex) {
            rejected = true;
        }
        check(rejected, "未过闸门的发现结果不能调用");
        passed++;
        System.out.println("通过 工具发现结果未过闸门不能成为可调用工具");
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
