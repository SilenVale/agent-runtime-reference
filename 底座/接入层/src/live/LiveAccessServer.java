package live;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import access.AcceptResult;

/**
 * 接入层进程：幂等受理，返回 202。不执行图。
 */
public final class LiveAccessServer {
    public static void main(String[] args) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        LiveAcceptService accept = new LiveAcceptService(config);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", config.accessPort), 0);
        server.createContext("/tasks", exchange -> handleAccept(exchange, accept));
        server.createContext("/health", exchange -> write(exchange, 200, "ok"));
        server.start();
        System.out.println("process=access port=" + config.accessPort);
    }

    private static void handleAccept(HttpExchange exchange, LiveAcceptService accept) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            write(exchange, 405, "method not allowed");
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> form = parse(body);
        try {
            boolean abortBeforeCommit = "1".equals(form.getOrDefault("abort", "0"));
            AcceptResult result = accept.accept(
                    form.getOrDefault("tenant", "tenant"),
                    form.getOrDefault("actor", "actor"),
                    form.getOrDefault("key", "key"),
                    form.getOrDefault("digest", "digest"),
                    Long.parseLong(form.getOrDefault("budget", "100")),
                    abortBeforeCommit);
            if (result.status() == 409) {
                write(exchange, 409, "conflict");
                return;
            }
            if (result.status() == 500) {
                write(exchange, 500, "not_committed");
                return;
            }
            write(exchange, 202, "task_id=" + result.taskId());
        } catch (Exception ex) {
            write(exchange, 500, ex.getMessage());
        }
    }

    private static Map<String, String> parse(String body) {
        Map<String, String> map = new HashMap<>();
        for (String part : body.split("&")) {
            int at = part.indexOf('=');
            if (at > 0) {
                map.put(part.substring(0, at), part.substring(at + 1));
            }
        }
        return map;
    }

    private static void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
