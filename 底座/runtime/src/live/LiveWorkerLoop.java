package live;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Runtime 进程：消费调度、取得执行权、恢复扫描。关闭自动提交。
 */
public final class LiveWorkerLoop {
    public static void main(String[] args) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String podId = args.length > 0 ? args[0] : "pod-" + UUID.randomUUID();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 演示里会强杀进程。把会话超时缩短，好让另一个 Runtime 尽快再均衡。
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        // 不设 group.instance.id：强杀后好尽快再均衡。
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(config.topic));
            System.out.println("process=runtime pod=" + podId + " group=" + config.group);
            long lastScan = 0L;
            while (true) {
                long now = System.currentTimeMillis();
                if (now - lastScan >= 500L) {
                    scanRecovery(config);
                    lastScan = now;
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("收到 " + record.value());
                    handle(config, podId, record.value());
                    consumer.commitSync();
                }
            }
        }
    }

    static void handle(LiveConfig config, String podId, String payload) throws Exception {
        String[] parts = payload.split("\\|", -1);
        String messageId = parts[0];
        String taskId = parts[1];
        int expectedEpoch = Integer.parseInt(parts[3]);
        String scheduleType = parts[4];
        String runVersion = parts[5];
        try (Connection connection = Pg.connect(config)) {
            try (PreparedStatement inbox = connection.prepareStatement(
                    "SELECT conclusion, epoch_after FROM inbox WHERE consumer=? AND message_id=?")) {
                inbox.setString(1, config.group);
                inbox.setString(2, messageId);
                try (ResultSet rs = inbox.executeQuery()) {
                    if (rs.next()) {
                        connection.commit();
                        System.out.println("复用 " + messageId + " " + rs.getString("conclusion"));
                        return;
                    }
                }
            }
            String state;
            int epoch;
            String owner;
            Timestamp lease;
            String version;
            String tenant;
            try (PreparedStatement task = connection.prepareStatement(
                    "SELECT state, epoch, owner, lease_expires_at, run_version, tenant FROM tasks WHERE task_id=? FOR UPDATE")) {
                task.setString(1, taskId);
                try (ResultSet rs = task.executeQuery()) {
                    if (!rs.next()) {
                        connection.rollback();
                        return;
                    }
                    state = rs.getString("state");
                    epoch = rs.getInt("epoch");
                    owner = rs.getString("owner");
                    lease = rs.getTimestamp("lease_expires_at");
                    version = rs.getString("run_version");
                    tenant = rs.getString("tenant");
                }
            }
            boolean leaseValid = owner != null && lease != null && lease.after(new Timestamp(System.currentTimeMillis()));
            boolean eligible = !"SUCCEEDED".equals(state)
                    && !"FAILED".equals(state)
                    && !"CANCELLED".equals(state)
                    && version.equals(runVersion)
                    && expectedEpoch == epoch
                    && !leaseValid
                    && (("START".equals(scheduleType) || "DEFER".equals(scheduleType)) && "ACCEPTED".equals(state)
                            || "RECOVERY".equals(scheduleType) && "RUNNING".equals(state));
            if (!eligible) {
                saveInbox(connection, config.group, messageId, "IGNORED", epoch);
                connection.commit();
                System.out.println("忽略 " + messageId);
                return;
            }
            int newEpoch = epoch + 1;
            String nextState = "ACCEPTED".equals(state) ? "RUNNING" : state;
            Timestamp until = new Timestamp(System.currentTimeMillis() + config.leaseMs);
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE tasks SET state=?, epoch=?, owner=?, lease_expires_at=? WHERE task_id=? AND epoch=?")) {
                update.setString(1, nextState);
                update.setInt(2, newEpoch);
                update.setString(3, podId);
                update.setTimestamp(4, until);
                update.setString(5, taskId);
                update.setInt(6, epoch);
                if (update.executeUpdate() != 1) {
                    connection.rollback();
                    return;
                }
            }
            saveInbox(connection, config.group, messageId, "TAKEN", newEpoch);
            try (PreparedStatement event = connection.prepareStatement(
                    "INSERT INTO events(event_id, task_id, event_seq, type) VALUES (?,?, (SELECT COALESCE(MAX(event_seq),0)+1 FROM events WHERE task_id=?), '已接管')")) {
                event.setString(1, "event-" + UUID.randomUUID());
                event.setString(2, taskId);
                event.setString(3, taskId);
                event.executeUpdate();
            }
            connection.commit();
            System.out.println("接管 " + taskId + " epoch=" + newEpoch + " owner=" + podId + " tenant=" + tenant);
        }
    }

    static void scanRecovery(LiveConfig config) throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement select = connection.prepareStatement(
                        """
                        SELECT task_id, epoch, run_version FROM tasks
                        WHERE state='RUNNING'
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at <= now()
                        """);
                ResultSet rs = select.executeQuery()) {
            while (rs.next()) {
                String taskId = rs.getString("task_id");
                int epoch = rs.getInt("epoch");
                String version = rs.getString("run_version");
                // 这一代已经有恢复调度就不再登记，不论是否已投递。
                try (PreparedStatement pending = connection.prepareStatement(
                        """
                        SELECT 1 FROM outbox
                        WHERE task_id=? AND schedule_type='RECOVERY' AND expected_epoch=?
                        LIMIT 1
                        """)) {
                    pending.setString(1, taskId);
                    pending.setInt(2, epoch);
                    try (ResultSet exists = pending.executeQuery()) {
                        if (exists.next()) {
                            continue;
                        }
                    }
                }
                int nextSeq;
                try (PreparedStatement max = connection.prepareStatement(
                        "SELECT COALESCE(MAX(dispatch_seq),0) FROM outbox WHERE task_id=?")) {
                    max.setString(1, taskId);
                    try (ResultSet seq = max.executeQuery()) {
                        seq.next();
                        nextSeq = seq.getInt(1) + 1;
                    }
                }
                try (PreparedStatement insert = connection.prepareStatement(
                        """
                        INSERT INTO outbox(message_id, task_id, dispatch_seq, expected_epoch, schedule_type, status, run_version)
                        VALUES (?, ?, ?, ?, 'RECOVERY', 'PENDING', ?)
                        """)) {
                    insert.setString(1, "msg-" + UUID.randomUUID());
                    insert.setString(2, taskId);
                    insert.setInt(3, nextSeq);
                    insert.setInt(4, epoch);
                    insert.setString(5, version);
                    insert.executeUpdate();
                }
                connection.commit();
                System.out.println("恢复登记 " + taskId + " expected_epoch=" + epoch);
            }
        }
    }

    private static void saveInbox(
            Connection connection, String group, String messageId, String conclusion, int epoch) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO inbox(consumer, message_id, conclusion, epoch_after) VALUES (?,?,?,?)")) {
            insert.setString(1, group);
            insert.setString(2, messageId);
            insert.setString(3, conclusion);
            insert.setInt(4, epoch);
            insert.executeUpdate();
        }
    }
}
