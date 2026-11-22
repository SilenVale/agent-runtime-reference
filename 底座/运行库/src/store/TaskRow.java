package store;

/**
 * 任务行。金额用最小单位的整数，时间用数据库时钟的毫秒。文档未写死列类型。
 */
public final class TaskRow {
    private final String taskId;
    private final String tenant;
    private final String actor;
    private final String conversationId;
    private final String idempotencyKey;
    private final String paramDigest;
    private final String runVersion;
    private final TaskState state;
    private final int epoch;
    private final String owner;
    private final Long leaseExpiresAt;
    private final long budgetLimit;
    private final long spent;
    private final long reserved;
    private final String recoveryRef;

    private TaskRow(Builder builder) {
        this.taskId = builder.taskId;
        this.tenant = builder.tenant;
        this.actor = builder.actor;
        this.conversationId = builder.conversationId;
        this.idempotencyKey = builder.idempotencyKey;
        this.paramDigest = builder.paramDigest;
        this.runVersion = builder.runVersion;
        this.state = builder.state;
        this.epoch = builder.epoch;
        this.owner = builder.owner;
        this.leaseExpiresAt = builder.leaseExpiresAt;
        this.budgetLimit = builder.budgetLimit;
        this.spent = builder.spent;
        this.reserved = builder.reserved;
        this.recoveryRef = builder.recoveryRef;
    }

    public static Builder builder(String taskId, String tenant) {
        return new Builder(taskId, tenant);
    }

    public Builder toBuilder() {
        return new Builder(taskId, tenant)
                .actor(actor)
                .conversationId(conversationId)
                .idempotencyKey(idempotencyKey)
                .paramDigest(paramDigest)
                .runVersion(runVersion)
                .state(state)
                .epoch(epoch)
                .owner(owner)
                .leaseExpiresAt(leaseExpiresAt)
                .budgetLimit(budgetLimit)
                .spent(spent)
                .reserved(reserved)
                .recoveryRef(recoveryRef);
    }

    public String actor() {
        return actor;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public String paramDigest() {
        return paramDigest;
    }

    public String runVersion() {
        return runVersion;
    }

    public String taskId() {
        return taskId;
    }

    public String tenant() {
        return tenant;
    }

    public String conversationId() {
        return conversationId;
    }

    public TaskState state() {
        return state;
    }

    public int epoch() {
        return epoch;
    }

    public String owner() {
        return owner;
    }

    public Long leaseExpiresAt() {
        return leaseExpiresAt;
    }

    public long budgetLimit() {
        return budgetLimit;
    }

    public long spent() {
        return spent;
    }

    public long reserved() {
        return reserved;
    }

    public String recoveryRef() {
        return recoveryRef;
    }

    public String threadId() {
        return taskId + ":epoch-" + epoch;
    }

    public static final class Builder {
        private final String taskId;
        private final String tenant;
        private String actor;
        private String conversationId;
        private String idempotencyKey;
        private String paramDigest;
        private String runVersion = "v1";
        private TaskState state = TaskState.ACCEPTED;
        private int epoch;
        private String owner;
        private Long leaseExpiresAt;
        private long budgetLimit;
        private long spent;
        private long reserved;
        private String recoveryRef;

        private Builder(String taskId, String tenant) {
            this.taskId = taskId;
            this.tenant = tenant;
        }

        public Builder actor(String value) {
            this.actor = value;
            return this;
        }

        public Builder conversationId(String value) {
            this.conversationId = value;
            return this;
        }

        public Builder idempotencyKey(String value) {
            this.idempotencyKey = value;
            return this;
        }

        public Builder paramDigest(String value) {
            this.paramDigest = value;
            return this;
        }

        public Builder runVersion(String value) {
            this.runVersion = value;
            return this;
        }

        public Builder state(TaskState value) {
            this.state = value;
            return this;
        }

        public Builder epoch(int value) {
            this.epoch = value;
            return this;
        }

        public Builder owner(String value) {
            this.owner = value;
            return this;
        }

        public Builder leaseExpiresAt(Long value) {
            this.leaseExpiresAt = value;
            return this;
        }

        public Builder budgetLimit(long value) {
            this.budgetLimit = value;
            return this;
        }

        public Builder spent(long value) {
            this.spent = value;
            return this;
        }

        public Builder reserved(long value) {
            this.reserved = value;
            return this;
        }

        public Builder recoveryRef(String value) {
            this.recoveryRef = value;
            return this;
        }

        public TaskRow build() {
            return new TaskRow(this);
        }
    }
}
