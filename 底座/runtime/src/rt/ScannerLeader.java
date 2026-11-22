package rt;

/**
 * 恢复扫描的主实例。切换时不把已经累计的次数清零。
 */
public final class ScannerLeader {
    public static final long LEASE_MS = 15_000L;

    private String holder;
    private long until;
    private int attempts;

    public boolean claim(String id, long now) {
        if (holder == null || until <= now || holder.equals(id)) {
            holder = id;
            until = now + LEASE_MS;
            return true;
        }
        return false;
    }

    public void recordAttempt() {
        attempts++;
    }

    public int attempts() {
        return attempts;
    }

    public String holder() {
        return holder;
    }
}
