package store;

/**
 * 任务状态的内存视图。offset 提交不得改它。
 */
public final class TaskRecord {
    private final String taskId;
    private final String state;
    private final String owner;
    private final int epoch;

    public TaskRecord(String taskId, String state, String owner, int epoch) {
        this.taskId = taskId;
        this.state = state;
        this.owner = owner;
        this.epoch = epoch;
    }

    public String taskId() {
        return taskId;
    }

    public String state() {
        return state;
    }

    public String owner() {
        return owner;
    }

    public int epoch() {
        return epoch;
    }
}
