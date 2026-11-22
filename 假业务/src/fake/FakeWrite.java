package fake;

/**
 * 假写操作的 MySQL 侧。这一章只保证它不保存任务执行权。
 * 认 operation_id 的写入在后面的章节实现。
 */
public final class FakeWrite {
    public void saveExecutionRight(String taskId, String owner, int generation) {
        throw new IllegalStateException("MySQL假写不保存任务执行权");
    }
}
