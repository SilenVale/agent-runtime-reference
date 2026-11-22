package store;

public final class CheckpointRow {
    private final String taskId;
    private final int epoch;
    private final String payload;

    public CheckpointRow(String taskId, int epoch, String payload) {
        this.taskId = taskId;
        this.epoch = epoch;
        this.payload = payload;
    }

    public String taskId() {
        return taskId;
    }

    public int epoch() {
        return epoch;
    }

    public String payload() {
        return payload;
    }

    public String threadId() {
        return taskId + ":epoch-" + epoch;
    }
}
