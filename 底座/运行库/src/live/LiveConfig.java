package live;

public final class LiveConfig {
    public final String pgUrl;
    public final String pgUser;
    public final String pgPassword;
    public final String fakeJdbcUrl;
    public final String fakeUser;
    public final String fakePassword;
    public final String kafkaBootstrap;
    public final String topic;
    public final String group;
    public final long leaseMs;
    public final int accessPort;

    public LiveConfig(
            String pgUrl,
            String pgUser,
            String pgPassword,
            String fakeJdbcUrl,
            String fakeUser,
            String fakePassword,
            String kafkaBootstrap,
            String topic,
            String group,
            long leaseMs,
            int accessPort) {
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
        this.fakeJdbcUrl = fakeJdbcUrl;
        this.fakeUser = fakeUser;
        this.fakePassword = fakePassword;
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
        String mysqlHost = env("MYSQL_HOST", "127.0.0.1");
        String mysqlPort = env("MYSQL_PORT", "3306");
        String mysqlDb = env("MYSQL_DATABASE", "fake_write");
        return new LiveConfig(
                "jdbc:postgresql://" + host + ":" + port + "/" + runtimeDb,
                user,
                password,
                "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDb
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                env("MYSQL_USER", "fake"),
                env("MYSQL_PASSWORD", "fake"),
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
