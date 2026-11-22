package fake;

import java.util.HashMap;
import java.util.Map;

/**
 * 假写操作的 MySQL 侧。只认 operation_id，不保存任务执行权。
 */
public final class FakeWrite {
    private final Map<String, Result> results = new HashMap<>();
    private int writes;

    public void saveExecutionRight(String taskId, String owner, int generation) {
        throw new IllegalStateException("MySQL假写不保存任务执行权");
    }

    public String write(String operationId, String digest, String body) {
        Result existing = results.get(operationId);
        if (existing != null) {
            if (!existing.digest.equals(digest)) {
                throw new IllegalStateException("同号不能换参数");
            }
            return existing.body;
        }
        results.put(operationId, new Result(digest, body));
        writes++;
        return body;
    }

    public String query(String operationId) {
        Result existing = results.get(operationId);
        if (existing == null) {
            return null;
        }
        return existing.body;
    }

    public int writes() {
        return writes;
    }

    private static final class Result {
        private final String digest;
        private final String body;

        private Result(String digest, String body) {
            this.digest = digest;
            this.body = body;
        }
    }
}
