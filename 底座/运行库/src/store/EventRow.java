package store;

public final class EventRow {
    private final String eventId;
    private final String taskId;
    private final int eventSeq;
    private final String type;

    public EventRow(String eventId, String taskId, int eventSeq, String type) {
        this.eventId = eventId;
        this.taskId = taskId;
        this.eventSeq = eventSeq;
        this.type = type;
    }

    public String eventId() {
        return eventId;
    }

    public String taskId() {
        return taskId;
    }

    public int eventSeq() {
        return eventSeq;
    }

    public String type() {
        return type;
    }

    public String id() {
        return taskId + "#" + eventSeq;
    }
}
