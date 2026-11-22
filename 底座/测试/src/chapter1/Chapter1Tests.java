package chapter1;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import access.AccessProcess;
import dispatch.DispatchProcess;
import fake.FakeWrite;
import rt.RecoveryScanner;
import rt.Worker;
import store.ConsumerCursor;
import store.ExecutionLog;
import store.MilvusIndex;
import store.RedisCache;
import store.TaskRecord;

public final class Chapter1Tests {
    private int passed;

    public static void main(String[] args) throws Exception {
        Chapter1Tests tests = new Chapter1Tests();
        tests.接入层受理路径不执行图();
        tests.消息投递不改执行权和任务终态();
        tests.Worker不承担全局恢复扫描();
        tests.Redis与Milvus与MySQL不保存任务执行权();
        tests.offset提交不表示任务完成();
        tests.假业务目录不引用底座的任务表();
        System.out.println("测试 " + tests.passed + " 项，通过 " + tests.passed + " 项");
    }

    private void 接入层受理路径不执行图() {
        ExecutionLog log = new ExecutionLog();
        AccessProcess access = new AccessProcess(log);
        String receipt = access.accept("idem-1");
        check("accepted-without-graph".equals(receipt), "受理有回执");
        check(log.containsPrefix("accept:"), "受理记在接入层");
        check(!log.containsPrefix("graph:"), "接入层受理路径不执行图");
        passed++;
        System.out.println("通过 接入层受理路径不执行图");
    }

    private void 消息投递不改执行权和任务终态() {
        TaskRecord task = new TaskRecord("task-1", "SUCCEEDED", "owner-a", 2);
        List<String> kafka = new ArrayList<>();
        new DispatchProcess().deliver(task, kafka);
        check(kafka.size() == 1, "消息进入投递日志");
        check("SUCCEEDED".equals(task.state()), "终态没被投递改掉");
        check("owner-a".equals(task.owner()), "执行者没被投递改掉");
        check(task.epoch() == 2, "代次没被投递改掉");
        passed++;
        System.out.println("通过 消息投递不改执行权和任务终态");
    }

    private void Worker不承担全局恢复扫描() {
        Worker worker = new Worker();
        worker.consumeOne("msg-1");
        check(worker.consumed().equals(List.of("msg-1")), "Worker只消费交给它的一条");
        boolean refused = false;
        try {
            worker.scanExpiredTasks();
        } catch (IllegalStateException ex) {
            refused = ex.getMessage().contains("全局恢复扫描");
        }
        check(refused, "Worker拒绝全局恢复扫描");
        check("recovery-scanner".equals(new RecoveryScanner().role()), "恢复扫描是另一个角色");
        passed++;
        System.out.println("通过 Worker不承担全局恢复扫描");
    }

    private void Redis与Milvus与MySQL不保存任务执行权() {
        check(refused(() -> new RedisCache().putExecutionRight("t", "o", 1)), "Redis拒绝执行权");
        RedisCache redis = new RedisCache();
        redis.putCache("hot", "1");
        check("1".equals(redis.getCache("hot")), "Redis仍可放可丢弃缓存");
        check(refused(() -> new MilvusIndex().putTask("t")), "Milvus拒绝任务");
        check(refused(() -> new FakeWrite().saveExecutionRight("t", "o", 1)), "MySQL假写拒绝执行权");
        passed++;
        System.out.println("通过 Redis与Milvus与MySQL不保存任务执行权");
    }

    private void offset提交不表示任务完成() {
        TaskRecord task = new TaskRecord("task-2", "RUNNING", "owner-b", 1);
        ConsumerCursor cursor = new ConsumerCursor();
        new Worker().commitScheduleOffset(cursor, 7L, task);
        check(cursor.offset() == 7L, "offset已提交");
        check("RUNNING".equals(task.state()), "offset提交不表示任务完成");
        passed++;
        System.out.println("通过 offset提交不表示任务完成");
    }

    private void 假业务目录不引用底座的任务表() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        Path fake = root.resolve("假业务");
        check(Files.isDirectory(fake), "假业务目录存在");
        check(Files.isDirectory(root.resolve("底座/接入层")), "接入层目录存在");
        check(Files.isDirectory(root.resolve("底座/消息投递")), "消息投递目录存在");
        check(Files.isDirectory(root.resolve("底座/runtime")), "Runtime目录存在");
        try (Stream<Path> files = Files.walk(fake)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                String text = Files.readString(file);
                check(!text.contains("TaskRecord"), file + " 不引用任务表");
                check(!text.contains("import store"), file + " 不引用底座运行库");
                check(!text.contains("epoch"), file + " 不写执行代次");
            }
        }
        passed++;
        System.out.println("通过 假业务目录不引用底座的任务表");
    }

    private static void check(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }

    private static boolean refused(Runnable action) {
        try {
            action.run();
            return false;
        } catch (IllegalStateException ex) {
            return true;
        }
    }
}
