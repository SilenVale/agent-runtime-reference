package store;

public final class IdempotencyRow {
    private final String tenant;
    private final String actor;
    private final String key;
    private final String paramDigest;
    private final String taskId;

    public IdempotencyRow(String tenant, String actor, String key, String paramDigest, String taskId) {
        this.tenant = tenant;
        this.actor = actor;
        this.key = key;
        this.paramDigest = paramDigest;
        this.taskId = taskId;
    }

    public String tenant() {
        return tenant;
    }

    public String actor() {
        return actor;
    }

    public String key() {
        return key;
    }

    public String paramDigest() {
        return paramDigest;
    }

    public String taskId() {
        return taskId;
    }

    public String id() {
        return tenant + "|" + actor + "|" + key;
    }
}
