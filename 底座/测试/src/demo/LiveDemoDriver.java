package demo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;

import live.LiveConfig;
import live.LiveFakeWrite;
import live.Pg;

/**
 * 演示客户端。分阶段调用，由 scripts/demo.sh 在中间切换 Runtime 进程。
 */
public final class LiveDemoDriver {
    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "all".equals(args[0])) {
            throw new IllegalArgumentException("请用 scripts/demo.sh 分段调用 accept/reject/fakewrite");
        }
        switch (args[0]) {
            case "accept" -> System.out.print(accept());
            case "reject" -> System.out.print(reject(args[1], args[2]));
            case "fakewrite" -> System.out.print(fakeWrite(args[1]));
            default -> throw new IllegalArgumentException("未知阶段: " + args[0]);
        }
    }

    static String accept() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        Pg.ensureSchema(config);
        Pg.resetDemoData(config);
        StringBuilder out = new StringBuilder();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        String first = post(client, config, "same-key", "digest");
        String second = post(client, config, "same-key", "digest");
        String taskId = extractTaskId(first);
        if (!taskId.equals(extractTaskId(second))) {
            throw new IllegalStateException("同键没有返回原任务");
        }
        int taskCount = count(config, "SELECT COUNT(*) FROM tasks");
        out.append("受理 202 ").append(taskId).append('\n');
        out.append("再次受理 ").append(taskId).append(" 任务数=").append(taskCount).append('\n');
        String messageId = waitFor(
                config,
                "SELECT message_id FROM outbox WHERE task_id=? AND status='SENT' ORDER BY dispatch_seq LIMIT 1",
                taskId,
                20_000);
        out.append("投递 ").append(messageId).append('\n');
        return out.toString();
    }

    static String reject(String taskId, String owner) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        boolean rejected = false;
        try (Connection connection = Pg.connect(config);
                PreparedStatement update = connection.prepareStatement(
                        "UPDATE tasks SET state='SUCCEEDED' WHERE task_id=? AND epoch=0 AND owner=?")) {
            update.setString(1, taskId);
            update.setString(2, owner);
            int changed = update.executeUpdate();
            connection.rollback();
            rejected = changed == 0;
        }
        if (!rejected) {
            throw new IllegalStateException("代次没有被拒绝");
        }
        return "代次拒绝\n";
    }

    static String fakeWrite(String taskId) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String operationId = "op-" + UUID.randomUUID();
        try (Connection connection = Pg.connect(config);
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO operations(operation_id, task_id, node_key, param_digest, status) VALUES (?,?,?,?,?)")) {
            insert.setString(1, operationId);
            insert.setString(2, taskId);
            insert.setString(3, "node-write");
            insert.setString(4, "digest");
            insert.setString(5, "PLANNED");
            insert.executeUpdate();
            connection.commit();
        }
        LiveFakeWrite fake = new LiveFakeWrite(config);
        fake.write(operationId, "digest", "done");
        String found = fake.query(operationId);
        fake.write(operationId, "digest", "done");
        int writes = fake.writes(operationId);
        if (writes != 1 || !"done".equals(found)) {
            throw new IllegalStateException("假写结果不对 writes=" + writes + " found=" + found);
        }
        return "按原编号查询 " + operationId + " 结果=" + found + " 写入次数=" + writes + "\n";
    }

    private static String post(HttpClient client, LiveConfig config, String key, String digest) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + config.accessPort + "/tasks"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "tenant=tenant&actor=actor&key=" + key + "&digest=" + digest + "&budget=100"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 202) {
            throw new IllegalStateException("受理失败: " + response.statusCode() + " " + response.body());
        }
        return response.body();
    }

    private static String extractTaskId(String body) {
        String prefix = "task_id=";
        int at = body.indexOf(prefix);
        if (at < 0) {
            throw new IllegalStateException("没有 task_id: " + body);
        }
        return body.substring(at + prefix.length()).trim();
    }

    private static int count(LiveConfig config, String sql) throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            rs.next();
            int value = rs.getInt(1);
            connection.commit();
            return value;
        }
    }

    private static String waitFor(LiveConfig config, String sql, String arg, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = Pg.connect(config);
                    PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, arg);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        String value = rs.getString(1);
                        connection.commit();
                        return value;
                    }
                }
                connection.commit();
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("等待超时: " + sql);
    }
}
