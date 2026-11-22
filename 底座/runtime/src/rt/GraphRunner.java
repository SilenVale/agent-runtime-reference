package rt;

import store.ExecutionLog;

/**
 * 图只在 Runtime 里跑。接入层不持有这个类型。
 */
public final class GraphRunner {
    private final ExecutionLog log;

    public GraphRunner(ExecutionLog log) {
        this.log = log;
    }

    public void run(String taskId) {
        log.record("graph:" + taskId);
    }
}
