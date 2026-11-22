package chapter8;

import java.util.List;

import access.SseFeed;
import store.Database;
import store.EventRow;
import store.Rejected;
import store.Resync;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

public final class Chapter8Tests {
    private int passed;

    public static void main(String[] args) {
        Chapter8Tests tests = new Chapter8Tests();
        tests.断线后按event_seq补读且重复投递由客户端游标去重();
        tests.任一接入层副本都能读到已提交事件();
        tests.游标超出保留窗口时要求重新同步();
        tests.越权游标被拒绝();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 断线后按event_seq补读且重复投递由客户端游标去重() {
        Database db = events();
        SseFeed feed = new SseFeed();
        List<EventRow> first = feed.read(db, "tenant", "task-1", 1);
        List<EventRow> second = feed.read(db, "tenant", "task-1", 1);
        check(first.size() == 2 && first.get(0).eventSeq() == 2, "从最后成功的序号之后补读");
        check(first.get(0).eventId().equals(second.get(0).eventId()), "重复投递仍是同一个事件");
        List<EventRow> after = feed.read(db, "tenant", "task-1", 3);
        check(after.isEmpty(), "客户端记住游标后不再收下已处理事件");
        passed++;
        System.out.println("通过 断线后按event_seq补读且重复投递由客户端游标去重");
    }

    private void 任一接入层副本都能读到已提交事件() {
        Database db = events();
        List<EventRow> left = new SseFeed().read(db, "tenant", "task-1", 0);
        List<EventRow> right = new SseFeed().read(db, "tenant", "task-1", 0);
        check(left.size() == 3 && right.size() == 3, "两个副本读到同样的已提交事件");
        check(left.get(2).eventId().equals(right.get(2).eventId()), "副本不靠各自的内存进度");
        passed++;
        System.out.println("通过 任一接入层副本都能读到已提交事件");
    }

    private void 游标超出保留窗口时要求重新同步() {
        Database db = events();
        SseFeed feed = new SseFeed(2);
        boolean resync = false;
        try {
            feed.read(db, "tenant", "task-1", 0);
        } catch (Resync ex) {
            resync = ex.getMessage().contains("保留窗口");
        }
        check(resync, "旧游标要求重新同步");
        List<EventRow> kept = feed.read(db, "tenant", "task-1", 2);
        check(kept.size() == 1 && kept.get(0).eventSeq() == 3, "重新对齐后只读保留窗口内的后续事件");
        passed++;
        System.out.println("通过 游标超出保留窗口时要求重新同步");
    }

    private void 越权游标被拒绝() {
        Database db = events();
        boolean rejected = false;
        try {
            new SseFeed().read(db, "other-tenant", "task-1", 0);
        } catch (Rejected ex) {
            rejected = ex.getMessage().contains("越权");
        }
        check(rejected, "别的租户不能读这条任务的事件");
        passed++;
        System.out.println("通过 越权游标被拒绝");
    }

    private static Database events() {
        Database db = new Database();
        try (Transaction tx = db.begin()) {
            tx.saveTask(TaskRow.builder("task-1", "tenant").state(TaskState.RUNNING).epoch(1).build());
            tx.saveEvent(new EventRow("event-1", "task-1", 1, "任务已受理"));
            tx.saveEvent(new EventRow("event-2", "task-1", 2, "已接管"));
            tx.saveEvent(new EventRow("event-3", "task-1", 3, "继续"));
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
