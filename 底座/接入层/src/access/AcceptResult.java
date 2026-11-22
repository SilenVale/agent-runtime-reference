package access;

public final class AcceptResult {
    private final int status;
    private final String taskId;
    private final boolean enteredNewTask;

    private AcceptResult(int status, String taskId, boolean enteredNewTask) {
        this.status = status;
        this.taskId = taskId;
        this.enteredNewTask = enteredNewTask;
    }

    public static AcceptResult accepted(String taskId) {
        return new AcceptResult(202, taskId, true);
    }

    public static AcceptResult existing(String taskId) {
        return new AcceptResult(202, taskId, false);
    }

    public static AcceptResult conflict() {
        return new AcceptResult(409, null, false);
    }

    public static AcceptResult notCommitted() {
        return new AcceptResult(500, null, false);
    }

    public int status() {
        return status;
    }

    public String taskId() {
        return taskId;
    }

    public boolean enteredNewTask() {
        return enteredNewTask;
    }
}
