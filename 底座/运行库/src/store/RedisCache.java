package store;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis 只放可丢弃的缓存，不放执行权。
 */
public final class RedisCache {
    private final Map<String, String> cache = new HashMap<>();

    public void putCache(String key, String value) {
        cache.put(key, value);
    }

    public String getCache(String key) {
        return cache.get(key);
    }

    public void putExecutionRight(String taskId, String owner, int epoch) {
        throw new IllegalStateException("Redis不保存执行权");
    }
}
