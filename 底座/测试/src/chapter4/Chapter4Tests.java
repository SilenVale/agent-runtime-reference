package chapter4;

import fake.FakeWrite;
import rt.BudgetLedger;
import rt.ExternalAction;
import rt.RecoveryPoint;
import store.BudgetCall;
import store.CheckpointRow;
import store.Database;
import store.Ids;
import store.OperationRow;
import store.Rejected;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter4Tests {
    private int passed;
    private final TaskMachine machine = new TaskMachine();

    public static void main(String[] args) {
        Chapter4Tests tests = new Chapter4Tests();
        tests.旧epoch提交被拒绝且数据库时钟判断租约();
        tests.operation_id在调用前固定且重试不换号();
        tests.超时后按原operation_id查询假写且不写第二次();
        tests.Checkpoint与操作账本分开提交();
        tests.预算跨接管仍累计且旧代次不能覆盖();
        tests.相同call_id重复结算不重复扣费();
        tests.结果未知的预留在进程退出后仍然占用();
        tests.旧线程新存档不能登记为当前恢复引用();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 旧epoch提交被拒绝且数据库时钟判断租约() {
        Database db = runningTask(1_000L);
        boolean oldEpoch = false;
        try (Transaction tx = db.begin()) {
            machine.submit(tx, "task-1", 0, "owner-a", TaskState.SUCCEEDED);
            tx.commit();
        } catch (Rejected ex) {
            oldEpoch = ex.getMessage().contains("旧代次");
        }
        check(oldEpoch, "旧代次被拒绝");
        check(db.task("task-1").state() == TaskState.RUNNING, "旧代次没有改状态");
        try (Transaction tx = db.begin()) {
            TaskRow renewed = machine.renew(tx, "task-1", 1, "owner-a", 1_500L);
            tx.commit();
            check(renewed.leaseExpiresAt() == 1_500L, "数据库时钟仍有效时可以续租");
        }
        db.advanceClock(1_500L);
        boolean expired = false;
        try (Transaction tx = db.begin()) {
            machine.renew(tx, "task-1", 1, "owner-a", db.now() + 30_000L);
            tx.commit();
        } catch (Rejected ex) {
            expired = ex.getMessage().contains("租约");
        }
        check(expired, "到期由数据库时钟判断");
        check(db.task("task-1").leaseExpiresAt() == 1_500L, "失效后续租没有写上");
        passed++;
        System.out.println("通过 旧epoch提交被拒绝且数据库时钟判断租约");
    }

    private void operation_id在调用前固定且重试不换号() {
        Database db = runningTask(1_000L);
        ExternalAction action = new ExternalAction(new Ids());
        FakeWrite fake = new FakeWrite();
        String first;
        try (Transaction tx = db.begin()) {
            first = action.prepare(tx, "task-1", 1, "owner-a", "node-write", "digest-1");
            tx.commit();
        }
        check(fake.writes() == 0, "调用前还没有写假业务");
        check("PLANNED".equals(db.operation(first).status()), "动作前先落入操作账本");
        String second;
        try (Transaction tx = db.begin()) {
            second = action.prepare(tx, "task-1", 1, "owner-a", "node-write", "digest-1");
            tx.commit();
        }
        check(first.equals(second), "重试取出同一个operation_id");
        boolean swapped = false;
        try (Transaction tx = db.begin()) {
            action.prepare(tx, "task-1", 1, "owner-a", "node-write", "digest-2");
            tx.commit();
        } catch (Rejected ex) {
            swapped = ex.getMessage().contains("换参数");
        }
        check(swapped, "同一定位不能换参数");
        passed++;
        System.out.println("通过 operation_id在调用前固定且重试不换号");
    }

    private void 超时后按原operation_id查询假写且不写第二次() {
        Database db = runningTask(1_000L);
        ExternalAction action = new ExternalAction(new Ids());
        FakeWrite fake = new FakeWrite();
        String operationId;
        try (Transaction tx = db.begin()) {
            operationId = action.prepare(tx, "task-1", 1, "owner-a", "node-write", "digest-1");
            tx.commit();
        }
        String lost = action.invoke(fake, operationId, "digest-1", "done", true);
        check(lost == null && fake.writes() == 1, "响应丢失时假写已经发生");
        String found = action.invoke(fake, operationId, "digest-1", "done", false);
        check("done".equals(found) && fake.writes() == 1, "按原编号查询后不再写第二次");
        check("done".equals(fake.query(operationId)), "可以按原编号查到结果");
        passed++;
        System.out.println("通过 超时后按原operation_id查询假写且不写第二次");
    }

    private void Checkpoint与操作账本分开提交() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveOperation(new OperationRow("op-1", "task-1", "SUCCEEDED"));
            tx.commit();
        }
        try (Transaction tx = db.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "node-9"));
            tx.rollback();
        }
        check(db.operation("op-1") != null, "操作账本已提交");
        check(db.checkpoint("task-1:epoch-1") == null, "Checkpoint回滚不影响账本");
        Database other = new Database();
        try (Transaction tx = other.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "node-1"));
            tx.commit();
        }
        try (Transaction tx = other.begin()) {
            tx.saveOperation(new OperationRow("op-2", "task-1", "SUCCEEDED"));
            tx.rollback();
        }
        check(other.checkpoint("task-1:epoch-1") != null, "图位置单独提交");
        check(other.operation("op-2") == null, "图位置不能证明外部动作已经完成");
        passed++;
        System.out.println("通过 Checkpoint与操作账本分开提交");
    }

    private void 预算跨接管仍累计且旧代次不能覆盖() {
        Database db = runningTask(1_000L);
        db.advanceClock(1_000L);
        try (Transaction tx = db.begin()) {
            TaskRow taken = machine.takeover(tx, "task-1", "owner-b", db.now() + 30_000L);
            tx.commit();
            check(taken.epoch() == 2 && taken.spent() == 40, "接管后累计还在");
        }
        boolean covered = false;
        try (Transaction tx = db.begin()) {
            new BudgetLedger().overwriteSpent(tx, "task-1", 1, "owner-a", 0);
            tx.commit();
        } catch (Rejected ex) {
            covered = ex.getMessage().contains("旧代次");
        }
        check(covered && db.task("task-1").spent() == 40, "旧代次不能把累计清零");
        try (Transaction tx = db.begin()) {
            new BudgetLedger().reserve(tx, "task-1", 2, "owner-b", "call-1", "res-1", 10);
            tx.commit();
        }
        check(db.task("task-1").reserved() == 10 && db.task("task-1").spent() == 40, "新预留加在原累计之外");
        passed++;
        System.out.println("通过 预算跨接管仍累计且旧代次不能覆盖");
    }

    private void 相同call_id重复结算不重复扣费() {
        Database db = runningTask(1_000L);
        BudgetLedger ledger = new BudgetLedger();
        try (Transaction tx = db.begin()) {
            ledger.reserve(tx, "task-1", 1, "owner-a", "call-1", "res-1", 10);
            ledger.reserve(tx, "task-1", 1, "owner-a", "call-1", "res-1", 10);
            tx.commit();
        }
        check(db.task("task-1").reserved() == 10, "同一个call_id不重复预留");
        try (Transaction tx = db.begin()) {
            ledger.settle(tx, "call-1", 7);
            ledger.settle(tx, "call-1", 7);
            tx.commit();
        }
        check(db.task("task-1").spent() == 47, "重复结算只记一次");
        check(db.task("task-1").reserved() == 0, "结算后释放预留");
        check(db.budgetCall("call-1").settled() == 7, "回执记在原来的call_id上");
        passed++;
        System.out.println("通过 相同call_id重复结算不重复扣费");
    }

    private void 结果未知的预留在进程退出后仍然占用() {
        Database db = runningTask(1_000L);
        try (Transaction tx = db.begin()) {
            new BudgetLedger().reserve(tx, "task-1", 1, "owner-a", "call-9", "res-9", 12);
            tx.commit();
        }
        BudgetCall call = db.budgetCall("call-9");
        check(call != null && "RESERVED".equals(call.state()), "进程退出后预留记录还在");
        check(db.task("task-1").reserved() == 12 && db.task("task-1").spent() == 40, "未知结果继续占用额度");
        passed++;
        System.out.println("通过 结果未知的预留在进程退出后仍然占用");
    }

    private void 旧线程新存档不能登记为当前恢复引用() {
        Database db = runningTask(1_000L);
        db.advanceClock(1_000L);
        try (Transaction tx = db.begin()) {
            machine.takeover(tx, "task-1", "owner-b", db.now() + 30_000L);
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "from-epoch-1"));
            tx.commit();
        }
        RecoveryPoint points = new RecoveryPoint();
        try (Transaction tx = db.begin()) {
            tx.saveCheckpoint(new CheckpointRow("task-1", 1, "late-from-old-worker"));
            tx.commit();
        }
        boolean rejected = false;
        try (Transaction tx = db.begin()) {
            points.register(tx, "task-1", 1, "owner-a", "task-1:epoch-1");
            tx.commit();
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("旧代次");
        }
        check(rejected, "旧执行者不能登记恢复点");
        check(db.task("task-1").recoveryRef() == null, "旧存档没有变成当前恢复引用");
        try (Transaction tx = db.begin()) {
            points.register(tx, "task-1", 2, "owner-b", "task-1:epoch-1");
            tx.commit();
        }
        check("task-1:epoch-1".equals(db.task("task-1").recoveryRef()), "当前执行者可以引用较早代次的存档");
        passed++;
        System.out.println("通过 旧线程新存档不能登记为当前恢复引用");
    }

    private static Database runningTask(long leaseUntil) {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant")
                    .state(TaskState.RUNNING)
                    .epoch(1)
                    .owner("owner-a")
                    .leaseExpiresAt(leaseUntil)
                    .budgetLimit(100)
                    .spent(40)
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
