package chapter13;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class Chapter13Tests {
    public static void main(String[] args) throws Exception {
        String progress = Files.readString(Path.of("验收/进度.md"));
        for (int chapter = 0; chapter <= 12; chapter++) {
            if (!progress.contains("| " + chapter + " |") || !linePassed(progress, chapter)) {
                throw new AssertionError("第 " + chapter + " 行还不是通过");
            }
        }
        String page = Files.readString(Path.of("验收/13-常见架构面试题.md"));
        Matcher questions = Pattern.compile("(?m)^\\d+\\. ").matcher(page);
        int count = 0;
        while (questions.find()) {
            count++;
        }
        if (count != 24) {
            throw new AssertionError("题目不是 24 道：" + count);
        }
        String sources = readJavaSources();
        String architecture = Files.readString(Path.of("docs/架构.md"));
        Matcher evidence = Pattern.compile("`([^`]+)`").matcher(page);
        int pointers = 0;
        while (evidence.find()) {
            String token = evidence.group(1);
            if (token.contains("\n") || token.contains("/") || token.endsWith(".md")) {
                continue;
            }
            pointers++;
            if (!sources.contains(token)) {
                throw new AssertionError("找不到已通过的测试：" + token);
            }
        }
        if (pointers < 24) {
            throw new AssertionError("证据不够");
        }
        for (String heading : List.of("三个进程", "Worker", "执行适配器", "业务图", "存储归属", "Kafka", "六个标识", "MCP 出站", "入站 MCP", "Redis")) {
            if (!architecture.contains(heading)) {
                throw new AssertionError("架构文档缺少：" + heading);
            }
        }
        if (Files.exists(Path.of("docs/面试题对照.md")) || Files.exists(Path.of("docs/简历条目.md"))) {
            throw new AssertionError("公开仓库不放面试题对照或简历条目");
        }
        String readme = Files.readString(Path.of("README.md"));
        if (readme.contains("面试题对照") || readme.contains("简历条目")) {
            throw new AssertionError("README 不链接这两份文件");
        }
        System.out.println("通过 每一题都指到已通过的测试或架构段落");
        System.out.println("测试 1 项，通过 1 项");
    }

    private static boolean linePassed(String progress, int chapter) {
        for (String line : progress.split("\n")) {
            if (line.startsWith("| " + chapter + " |")) {
                return line.contains("| 通过 |")
                        || line.contains("规则通过、运行环境通过");
            }
        }
        return false;
    }

    private static String readJavaSources() throws Exception {
        StringBuilder builder = new StringBuilder();
        try (Stream<Path> files = Files.walk(Path.of("底座/测试"))) {
            for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                builder.append(Files.readString(file)).append('\n');
            }
        }
        return builder.toString();
    }
}
