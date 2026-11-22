package access;

import store.ExecutionLog;

/**
 * 接入层进程。只受理，不执行图。
 */
public final class AccessProcess {
    private final ExecutionLog log;

    public AccessProcess(ExecutionLog log) {
        this.log = log;
    }

    public static void main(String[] args) {
        System.out.println("process=access");
    }

    public String accept(String idempotencyKey) {
        log.record("accept:" + idempotencyKey);
        return "accepted-without-graph";
    }
}
