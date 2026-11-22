package demo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import live.LiveConfig;
import live.Pg;

/**
 * 第 3 章云端取证：对真实 PostgreSQL / Kafka / 三个 Java 进程查原文。
 * 由 scripts/cloud-chapter3.sh 分段调用。
 */
public final class Chapter3Cloud {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("需要子命令");
        }
        switch (args[0]) {
            case "reset" -> reset();
            case "t1" -> System.out.print(t1());
            case "conflict" -> System.out.print(conflict());
            case "epoch0" -> System.out.print(epoch0(args[1]));
            case "wait-owner" -> System.out.print(waitOwner(args[1], args.length > 2 ? args[2] : null));
            case "inbox" -> System.out.print(inbox(args[1]));
            case "set-quota0" -> System.out.print(setQuota0(args[1]));
            case "task-state" -> System.out.print(taskState(args[1]));
            case "defer-outbox" -> System.out.print(deferOutbox(args[1]));
            case "pending-outbox" -> System.out.print(pendingOutbox(args[1]));
            case "recovery" -> System.out.print(recovery(args[1]));
            case "counts" -> System.out.print(counts());
            default -> throw new IllegalArgumentException("未知子命令: " + args[0]);
        }
    }

    static void reset() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        Pg.ensureSchema(config);
        Pg.resetDemoData(config);
        System.out.println("reset-ok");
    }

    static String t1() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        StringBuilder out = new StringBuilder();
        HttpResponse<String> aborted = post(client, config, "t1-abort", "digest-a", true);
        out.append("提交前退出 status=").append(aborted.statusCode()).append(" body=").append(aborted.body().trim()).append('\n');
        int tasksAfterAbort = scalarInt(config, "SELECT COUNT(*) FROM tasks");
        int eventsAfterAbort = scalarInt(config, "SELECT COUNT(*) FROM events");
        int outboxAfterAbort = scalarInt(config, "SELECT COUNT(*) FROM outbox");
        int idemAfterAbort = scalarInt(config, "SELECT COUNT(*) FROM idempotency");
        out.append("提交前四样 tasks=")
                .append(tasksAfterAbort)
                .append(" idem=")
                .append(idemAfterAbort)
                .append(" events=")
                .append(eventsAfterAbort)
                .append(" outbox=")
                .append(outboxAfterAbort)
                .append('\n');
        HttpResponse<String> done = post(client, config, "t1-key", "digest-a", false);
        String taskId = extractTaskId(done.body());
        out.append("受理 status=").append(done.statusCode()).append(" task_id=").append(taskId).append('\n');
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        """
                        SELECT e.event_id, o.message_id, e.type, o.schedule_type, o.dispatch_seq, o.expected_epoch,
                               t.state, t.epoch, t.owner
                        FROM tasks t
                        JOIN events e ON e.task_id=t.task_id AND e.event_seq=1
                        JOIN outbox o ON o.task_id=t.task_id AND o.dispatch_seq=1
                        WHERE t.task_id=?
                        """)) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("T1 四样不齐");
                }
                String eventId = rs.getString("event_id");
                String messageId = rs.getString("message_id");
                out.append("T1已提交 event_id=")
                        .append(eventId)
                        .append(" message_id=")
                        .append(messageId)
                        .append(" type=")
                        .append(rs.getString("type"))
                        .append(" schedule=")
                        .append(rs.getString("schedule_type"))
                        .append(" seq=")
                        .append(rs.getInt("dispatch_seq"))
                        .append(" expected_epoch=")
                        .append(rs.getInt("expected_epoch"))
                        .append(" state=")
                        .append(rs.getString("state"))
                        .append(" epoch=")
                        .append(rs.getInt("epoch"))
                        .append(" owner=")
                        .append(String.valueOf(rs.getString("owner")))
                        .append('\n');
                out.append("事件ID与消息ID分开 same=")
                        .append(eventId.equals(messageId))
                        .append('\n');
            }
            connection.commit();
        }
        HttpResponse<String> retry = post(client, config, "t1-key", "digest-a", false);
        out.append("响应丢失后原键 task_id=")
                .append(extractTaskId(retry.body()))
                .append(" 任务数=")
                .append(scalarInt(config, "SELECT COUNT(*) FROM tasks"))
                .append('\n');
        return out.toString();
    }

    static String conflict() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        post(client, config, "clash-key", "digest-a", false);
        HttpResponse<String> clash = post(client, config, "clash-key", "digest-b", false);
        return "同键异参 status=" + clash.statusCode() + " 任务数=" + scalarInt(config, "SELECT COUNT(*) FROM tasks WHERE idempotency_key='clash-key'")
                + "\n";
    }

    static String epoch0(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT epoch, owner, lease_expires_at, state FROM tasks WHERE task_id=?")) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                String line = "初始 epoch=" + rs.getInt("epoch")
                        + " owner=" + rs.getString("owner")
                        + " lease=" + rs.getTimestamp("lease_expires_at")
                        + " state=" + rs.getString("state")
                        + "\n";
                connection.commit();
                return line;
            }
        }
    }

    static String waitOwner(String taskId, String expectOwner) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = Pg.connect(config);
                    PreparedStatement q = connection.prepareStatement(
                            "SELECT epoch, owner, state FROM tasks WHERE task_id=?")) {
                q.setString(1, taskId);
                try (ResultSet rs = q.executeQuery()) {
                    if (rs.next() && rs.getString("owner") != null) {
                        String owner = rs.getString("owner");
                        int epoch = rs.getInt("epoch");
                        String state = rs.getString("state");
                        connection.commit();
                        if (expectOwner == null || expectOwner.equals(owner)) {
                            return "接管 epoch=" + epoch + " owner=" + owner + " state=" + state + "\n";
                        }
                    }
                }
                connection.commit();
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("等待接管超时 task=" + taskId);
    }

    static String inbox(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        """
                        SELECT i.consumer, i.message_id, i.conclusion, i.epoch_after, t.owner, t.epoch
                        FROM inbox i
                        JOIN outbox o ON o.message_id=i.message_id
                        JOIN tasks t ON t.task_id=o.task_id
                        WHERE t.task_id=?
                        ORDER BY i.message_id
                        """)) {
            q.setString(1, taskId);
            StringBuilder out = new StringBuilder();
            int rows = 0;
            try (ResultSet rs = q.executeQuery()) {
                while (rs.next()) {
                    rows++;
                    String consumer = rs.getString("consumer");
                    out.append("Inbox consumer=")
                            .append(consumer)
                            .append(" message_id=")
                            .append(rs.getString("message_id"))
                            .append(" conclusion=")
                            .append(rs.getString("conclusion"))
                            .append(" epoch_after=")
                            .append(rs.getInt("epoch_after"))
                            .append(" owner=")
                            .append(rs.getString("owner"))
                            .append(" task_epoch=")
                            .append(rs.getInt("epoch"))
                            .append(" 消费组非Pod=")
                            .append(!consumer.equals(rs.getString("owner")))
                            .append('\n');
                }
            }
            connection.commit();
            out.append("Inbox行数=").append(rows).append('\n');
            return out.toString();
        }
    }

    static String setQuota0(String tenant) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        try (Connection connection = Pg.connect(config);
                PreparedStatement upsert = connection.prepareStatement(
                        """
                        INSERT INTO quotas(tenant, limit_count, used_count) VALUES (?,0,0)
                        ON CONFLICT (tenant) DO UPDATE SET limit_count=0, used_count=0
                        """)) {
            upsert.setString(1, tenant);
            upsert.executeUpdate();
            connection.commit();
        }
        return "quota tenant=" + tenant + " limit=0\n";
    }

    static String taskState(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT state, epoch, owner FROM tasks WHERE task_id=?")) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                String line = "task state=" + rs.getString("state")
                        + " epoch=" + rs.getInt("epoch")
                        + " owner=" + rs.getString("owner")
                        + "\n";
                connection.commit();
                return line;
            }
        }
    }

    static String deferOutbox(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = Pg.connect(config);
                    PreparedStatement q = connection.prepareStatement(
                            """
                            SELECT message_id, dispatch_seq, expected_epoch, status, schedule_type
                            FROM outbox WHERE task_id=? AND schedule_type='DEFER'
                            ORDER BY dispatch_seq
                            """)) {
                q.setString(1, taskId);
                try (ResultSet rs = q.executeQuery()) {
                    if (rs.next()) {
                        String line = "延期 outbox message_id=" + rs.getString("message_id")
                                + " seq=" + rs.getInt("dispatch_seq")
                                + " expected_epoch=" + rs.getInt("expected_epoch")
                                + " status=" + rs.getString("status")
                                + " type=" + rs.getString("schedule_type")
                                + "\n";
                        connection.commit();
                        return line;
                    }
                }
                connection.commit();
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("没有延期 Outbox");
    }

    static String pendingOutbox(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT message_id, status, schedule_type FROM outbox WHERE task_id=? ORDER BY dispatch_seq")) {
            q.setString(1, taskId);
            StringBuilder out = new StringBuilder();
            try (ResultSet rs = q.executeQuery()) {
                while (rs.next()) {
                    out.append("outbox message_id=")
                            .append(rs.getString("message_id"))
                            .append(" status=")
                            .append(rs.getString("status"))
                            .append(" type=")
                            .append(rs.getString("schedule_type"))
                            .append('\n');
                }
            }
            connection.commit();
            return out.toString();
        }
    }

    static String recovery(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        long deadline = System.currentTimeMillis() + 40_000;
        String startMessage = null;
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT message_id FROM outbox WHERE task_id=? AND schedule_type='START' ORDER BY dispatch_seq LIMIT 1")) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) {
                    startMessage = rs.getString(1);
                }
            }
            connection.commit();
        }
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = Pg.connect(config);
                    PreparedStatement q = connection.prepareStatement(
                            """
                            SELECT message_id, dispatch_seq, expected_epoch, status
                            FROM outbox WHERE task_id=? AND schedule_type='RECOVERY'
                            ORDER BY dispatch_seq
                            """)) {
                q.setString(1, taskId);
                List<String> rows = new ArrayList<>();
                try (ResultSet rs = q.executeQuery()) {
                    while (rs.next()) {
                        rows.add("recovery message_id=" + rs.getString("message_id")
                                + " seq=" + rs.getInt("dispatch_seq")
                                + " expected_epoch=" + rs.getInt("expected_epoch")
                                + " status=" + rs.getString("status")
                                + " 新消息ID=" + (startMessage != null && !startMessage.equals(rs.getString("message_id"))));
                    }
                }
                int epoch = 0;
                try (PreparedStatement t = connection.prepareStatement("SELECT epoch FROM tasks WHERE task_id=?")) {
                    t.setString(1, taskId);
                    try (ResultSet rs = t.executeQuery()) {
                        if (rs.next()) {
                            epoch = rs.getInt(1);
                        }
                    }
                }
                connection.commit();
                if (!rows.isEmpty()) {
                    StringBuilder out = new StringBuilder();
                    for (String row : rows) {
                        out.append(row).append('\n');
                    }
                    out.append("扫描后任务 epoch=").append(epoch).append(" recovery条数=").append(rows.size()).append('\n');
                    return out.toString();
                }
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("没有恢复 Outbox");
    }

    static String counts() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        return "tasks=" + scalarInt(config, "SELECT COUNT(*) FROM tasks")
                + " inbox=" + scalarInt(config, "SELECT COUNT(*) FROM inbox")
                + "\n";
    }

    private static HttpResponse<String> post(
            HttpClient client, LiveConfig config, String key, String digest, boolean abort) throws Exception {
        String body = "tenant=tenant&actor=actor&key=" + key + "&digest=" + digest + "&budget=100"
                + (abort ? "&abort=1" : "");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + config.accessPort + "/tasks"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String extractTaskId(String body) {
        String prefix = "task_id=";
        int at = body.indexOf(prefix);
        if (at < 0) {
            throw new IllegalStateException("没有 task_id: " + body);
        }
        return body.substring(at + prefix.length()).trim();
    }

    private static int scalarInt(LiveConfig config, String sql) throws Exception {
        try (Connection connection = Pg.connect(config);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            int value = rs.getInt(1);
            connection.commit();
            return value;
        }
    }
}
