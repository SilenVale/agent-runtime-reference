package store;

public final class ScheduleMessage {
    private final String messageId;
    private final String taskId;
    private final int dispatchSeq;
    private final int expectedEpoch;
    private final String scheduleType;
    private final String runVersion;

    public ScheduleMessage(
            String messageId,
            String taskId,
            int dispatchSeq,
            int expectedEpoch,
            String scheduleType,
            String runVersion) {
        this.messageId = messageId;
        this.taskId = taskId;
        this.dispatchSeq = dispatchSeq;
        this.expectedEpoch = expectedEpoch;
        this.scheduleType = scheduleType;
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

    public String runVersion() {
        return runVersion;
    }
}
