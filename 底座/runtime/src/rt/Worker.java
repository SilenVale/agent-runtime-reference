package rt;

import java.util.ArrayList;
import java.util.List;

import store.ConsumerCursor;
import store.TaskRecord;

/**
 * Worker 只消费交给它的调度。它不做全局恢复扫描。
 */
public final class Worker {
    private final List<String> consumed = new ArrayList<>();

    public void consumeOne(String messageId) {
        consumed.add(messageId);
    }

    public List<String> consumed() {
        return List.copyOf(consumed);
    }

    public void scanExpiredTasks() {
        throw new IllegalStateException("Worker不做全局恢复扫描");
    }

    public void commitScheduleOffset(ConsumerCursor cursor, long offset, TaskRecord task) {
        String state = task.state();
        cursor.commit(offset);
        if (!state.equals(task.state())) {
            throw new IllegalStateException("offset提交不得改变任务状态");
        }
    }
}
