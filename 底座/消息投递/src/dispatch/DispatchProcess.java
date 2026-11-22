package dispatch;

import java.util.List;

import store.TaskRecord;

/**
 * 消息投递进程。只把调度送进日志，不改执行权，也不改终态。
 */
public final class DispatchProcess {
    public static void main(String[] args) {
        System.out.println("process=dispatch");
    }

    public void deliver(TaskRecord task, List<String> kafka) {
        String owner = task.owner();
        int epoch = task.epoch();
        String state = task.state();
        kafka.add(task.taskId());
        if (!same(owner, task.owner()) || epoch != task.epoch() || !state.equals(task.state())) {
            throw new IllegalStateException("投递不得修改执行权或任务状态");
        }
    }

    private static boolean same(String left, String right) {
        return left == null ? right == null : left.equals(right);
    }
}
