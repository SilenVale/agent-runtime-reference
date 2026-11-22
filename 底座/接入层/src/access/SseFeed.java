package access;

import java.util.ArrayList;
import java.util.List;

import store.Database;
import store.EventRow;
import store.Rejected;
import store.Resync;
import store.TaskRow;

/**
 * 只读已经提交的事件。任意副本都能读。超出保留窗口时要求重新同步。
 */
public final class SseFeed {
    public static final int DEFAULT_RETAIN = 1000;

    private final int retain;

    public SseFeed() {
        this(DEFAULT_RETAIN);
    }

    public SseFeed(int retain) {
        this.retain = retain;
    }

    public List<EventRow> read(Database database, String tenant, String taskId, int afterSeq) {
        TaskRow task = database.task(taskId);
        if (task == null || tenant == null || !tenant.equals(task.tenant())) {
            throw new Rejected("越权游标");
        }
        List<EventRow> all = database.eventsOf(taskId);
        if (all.isEmpty()) {
            return List.of();
        }
        int from = Math.max(0, all.size() - retain);
        List<EventRow> kept = new ArrayList<>(all.subList(from, all.size()));
        int oldest = kept.get(0).eventSeq();
        if (afterSeq < oldest - 1) {
            throw new Resync("游标超出保留窗口");
        }
        List<EventRow> page = new ArrayList<>();
        for (EventRow event : kept) {
            if (event.eventSeq() > afterSeq) {
                page.add(event);
            }
        }
        return page;
    }
}
