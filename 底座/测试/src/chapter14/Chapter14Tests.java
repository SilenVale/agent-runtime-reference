package chapter14;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class Chapter14Tests {
    public static void main(String[] args) throws Exception {
        String readme = Files.readString(Path.of("README.md"));
        List<String> headings = List.of(
                "## 一句话定位",
                "## 它解决什么问题",
                "## 演示",
                "## 架构图",
                "## 核心行为",
                "## 取舍",
                "## 快速开始",
                "## 验收结果",
                "## 已知局限");
        int cursor = -1;
        for (String heading : headings) {
            int found = readme.indexOf(heading);
            if (found <= cursor) {
                throw new AssertionError("README 缺少或顺序不对：" + heading);
            }
            cursor = found;
        }
        if (!readme.contains("任务接住以后不丢，执行的进程换了还能从登记过的恢复点继续，已经发往外部系统的同一次动作不打第二遍。")) {
            throw new AssertionError("三句保证不在");
        }
        if (readme.contains("面试题对照") || readme.contains("简历条目")) {
            throw new AssertionError("作品集不依赖面试题对照或简历条目");
        }
        if (Files.exists(Path.of("docs/面试题对照.md")) || Files.exists(Path.of("docs/简历条目.md"))) {
            throw new AssertionError("这两份文件不在公开仓库里");
        }
        Path env = Path.of("docs/demo/环境.txt");
        Path demo = Path.of("docs/demo/三段演示.txt");
        if (!Files.exists(env) || !Files.exists(demo)) {
            throw new AssertionError("请先跑 scripts/demo.sh，留下真实 PostgreSQL/Kafka 的演示记录");
        }
        String envText = Files.readString(env);
        String demoText = Files.readString(demo);
        if (!envText.contains("PostgreSQL=") || envText.contains("进程内Database=使用")) {
            throw new AssertionError("演示没有连上真实 PostgreSQL");
        }
        if (!envText.contains("Kafka=") || !envText.contains("进程内KafkaLog=未使用")) {
            throw new AssertionError("演示没有连上真实 Kafka");
        }
        if (!envText.contains("进程内Database=未使用")) {
            throw new AssertionError("演示不能用进程内 Database");
        }
        for (String step : List.of("受理", "投递", "接管", "代次拒绝", "按原编号查询")) {
            if (!demoText.contains(step)) {
                throw new AssertionError("演示缺少步骤：" + step);
            }
        }
        if (!demoText.contains("任务数=1") || !demoText.contains("epoch=2") || !demoText.contains("写入次数=1")) {
            throw new AssertionError("三段演示没有都接住：" + demoText);
        }
        System.out.println("通过 九段说明和真实服务三段演示");
        System.out.println("测试 1 项，通过 1 项");
    }
}
