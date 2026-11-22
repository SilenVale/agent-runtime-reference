package rt;

/**
 * 恢复扫描是 Runtime 里的另一个角色，不由 Worker 兼任。
 * 这一章只把角色分开。扫描规则在后面的章节实现。
 */
public final class RecoveryScanner {
    public String role() {
        return "recovery-scanner";
    }
}
