package live;

public final class EnsureSchema {
    public static void main(String[] args) throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        Pg.ensureSchema(config);
        System.out.println("schema-ready");
    }
}
