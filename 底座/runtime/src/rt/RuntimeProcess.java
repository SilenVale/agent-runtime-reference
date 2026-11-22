package rt;

/**
 * Runtime 进程入口。执行权和恢复都在这个进程里，但不在同一个类里。
 */
public final class RuntimeProcess {
    public static void main(String[] args) {
        System.out.println("process=runtime");
    }
}
