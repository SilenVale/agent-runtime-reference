package store;

/**
 * Kafka offset 只表示这个消费组读到哪，不表示任务完成。
 */
public final class ConsumerCursor {
    private long offset = -1L;

    public void commit(long next) {
        this.offset = next;
    }

    public long offset() {
        return offset;
    }
}
