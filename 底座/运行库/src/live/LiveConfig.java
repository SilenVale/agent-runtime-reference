package live;

public final class LiveConfig {
    public final String pgUrl;
    public final String pgUser;
    public final String pgPassword;
    public final String fakeUrl;
    public final String kafkaBootstrap;
    public final String topic;
    public final String group;
    public final long leaseMs;
    public final int accessPort;

    public LiveConfig(
            String pgUrl,
            String pgUser,
            String pgPassword,
            String fakeUrl,
            String kafkaBootstrap,
            String topic,
            String group,
            long leaseMs,
            int accessPort) {
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
        this.fakeUrl = fakeUrl;
        this.kafkaBootstrap = kafkaBootstrap;
        this.topic = topic;
        this.group = group;
        this.leaseMs = leaseMs;
        this.accessPort = accessPort;
    }

    public static LiveConfig fromEnv() {
        String host = env("PGHOST", "127.0.0.1");
        String port = env("PGPORT", "5432");
        String user = env("PGUSER", "runtime");
        String password = env("PGPASSWORD", "runtime");
        String runtimeDb = env("PGDATABASE", "runtime");
        String fakeDb = env("FAKE_DATABASE", "fake_write");
        return new LiveConfig(
                "jdbc:postgresql://" + host + ":" + port + "/" + runtimeDb,
                user,
                password,
                "jdbc:postgresql://" + host + ":" + port + "/" + fakeDb,
                env("KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
                env("KAFKA_TOPIC", "runtime.schedule"),
                env("KAFKA_GROUP", "runtime-workers"),
                Long.parseLong(env("LEASE_MS", "3000")),
                Integer.parseInt(env("ACCESS_PORT", "8080")));
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
