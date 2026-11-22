package live;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 消息投递进程：读已提交的 Outbox，发到 Kafka，确认后再标记已发送。
 */
public final class LiveDispatchLoop {
    private static boolean abortAfterSendOnce = "1".equals(System.getenv("DISPATCH_ABORT_AFTER_SEND"));

    public static void main(String[] args) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("process=dispatch abort_after_send=" + abortAfterSendOnce);
            while (true) {
                pumpOnce(config, producer);
                Thread.sleep(200);
            }
        }
    }

    static int pumpOnce(LiveConfig config, KafkaProducer<String, String> producer) throws Exception {
        int marked = 0;
        try (Connection connection = Pg.connect(config);
                PreparedStatement select = connection.prepareStatement(
                        """
                        SELECT message_id, task_id, dispatch_seq, expected_epoch, schedule_type, run_version
                        FROM outbox WHERE status='PENDING' ORDER BY dispatch_seq
                        """);
                ResultSet rs = select.executeQuery()) {
            while (rs.next()) {
                String messageId = rs.getString("message_id");
                String taskId = rs.getString("task_id");
                String payload = String.join("|",
                        messageId,
                        taskId,
                        Integer.toString(rs.getInt("dispatch_seq")),
                        Integer.toString(rs.getInt("expected_epoch")),
                        rs.getString("schedule_type"),
                        rs.getString("run_version"));
                RecordMetadata metadata = producer
                        .send(new ProducerRecord<>(config.topic, taskId, payload))
                        .get();
                if (metadata == null) {
                    connection.rollback();
                    return marked;
                }
                if (abortAfterSendOnce) {
                    abortAfterSendOnce = false;
                    connection.rollback();
                    System.out.println("确认前退出 " + messageId + " kafka_offset=" + metadata.offset());
                    // 退出进程，避免下一轮循环把同一条标成 SENT。
                    System.exit(2);
                }
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE outbox SET status='SENT' WHERE message_id=? AND status='PENDING'")) {
                    update.setString(1, messageId);
                    update.executeUpdate();
                }
                connection.commit();
                marked++;
                System.out.println("投递 " + messageId);
            }
        }
        return marked;
    }
}
