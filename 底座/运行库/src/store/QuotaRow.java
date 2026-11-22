package store;

public final class QuotaRow {
    private final String tenant;
    private final int limit;
    private final int used;

    public QuotaRow(String tenant, int limit, int used) {
        this.tenant = tenant;
        this.limit = limit;
        this.used = used;
    }

    public String tenant() {
        return tenant;
    }

    public int limit() {
        return limit;
    }

    public int used() {
        return used;
    }

    public QuotaRow occupy() {
        return new QuotaRow(tenant, limit, used + 1);
    }
}
