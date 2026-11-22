package dispatch;

import store.Database;
import store.KafkaLog;
import store.OutboxRow;
import store.Transaction;

/**
 * 只投递已经提交的 Outbox。Kafka 确认之后才标记已发送。确认丢失就保留原消息 ID。
 */
public final class OutboxDispatcher {
    public int pump(Database database, KafkaLog kafka) {
        int marked = 0;
        for (OutboxRow row : database.pendingOutbox()) {
            boolean acked = kafka.append(row.toMessage());
            if (!acked) {
                return marked;
            }
            try (Transaction tx = database.begin()) {
                OutboxRow current = tx.outbox(row.messageId());
                if (current != null && "PENDING".equals(current.status())) {
                    tx.saveOutbox(current.markSent());
                }
                tx.commit();
            }
            marked++;
        }
        return marked;
    }
}
