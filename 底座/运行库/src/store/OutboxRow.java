package store;

public final class OutboxRow {
    private final String messageId;
    private final String taskId;
    private final int dispatchSeq;
    private final int expectedEpoch;
    private final String scheduleType;
    private final String status;
    private final String runVersion;

    public OutboxRow(
            String messageId,
            String taskId,
            int dispatchSeq,
            int expectedEpoch,
            String scheduleType,
            String status,
            String runVersion) {
        this.messageId = messageId;
        this.taskId = taskId;
        this.dispatchSeq = dispatchSeq;
        this.expectedEpoch = expectedEpoch;
        this.scheduleType = scheduleType;
        this.status = status;
        this.runVersion = runVersion;
    }

    public String messageId() {
        return messageId;
    }

    public String taskId() {
        return taskId;
    }

    public int dispatchSeq() {
        return dispatchSeq;
    }

    public int expectedEpoch() {
        return expectedEpoch;
    }

    public String scheduleType() {
        return scheduleType;
    }

    public String status() {
        return status;
    }

    public String runVersion() {
        return runVersion;
    }

    public OutboxRow markSent() {
        return new OutboxRow(messageId, taskId, dispatchSeq, expectedEpoch, scheduleType, "SENT", runVersion);
    }

    public ScheduleMessage toMessage() {
        return new ScheduleMessage(messageId, taskId, dispatchSeq, expectedEpoch, scheduleType, runVersion);
    }
}
