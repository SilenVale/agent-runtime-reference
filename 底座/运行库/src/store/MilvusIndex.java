package store;

/**
 * Milvus 只做可重建的知识索引，不存任务。
 */
public final class MilvusIndex {
    public void putTask(String taskId) {
        throw new IllegalStateException("Milvus不保存任务");
    }
}
