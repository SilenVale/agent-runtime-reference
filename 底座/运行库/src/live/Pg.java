package live;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public final class Pg {
    private Pg() {}

    public static Connection connect(LiveConfig config) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.pgUser);
        props.setProperty("password", config.pgPassword);
        Connection connection = DriverManager.getConnection(config.pgUrl, props);
        connection.setAutoCommit(false);
        return connection;
    }

    public static Connection connectFake(LiveConfig config) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.pgUser);
        props.setProperty("password", config.pgPassword);
        Connection connection = DriverManager.getConnection(config.fakeUrl, props);
        connection.setAutoCommit(false);
        return connection;
    }

    public static void ensureSchema(LiveConfig config) throws SQLException {
        try (Connection connection = connect(config); Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(42001)");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS tasks (
                      task_id TEXT PRIMARY KEY,
                      tenant TEXT NOT NULL,
                      actor TEXT,
                      idempotency_key TEXT,
                      param_digest TEXT,
                      run_version TEXT NOT NULL,
                      state TEXT NOT NULL,
                      epoch INT NOT NULL,
                      owner TEXT,
                      lease_expires_at TIMESTAMPTZ,
                      budget_limit BIGINT NOT NULL,
                      spent BIGINT NOT NULL DEFAULT 0,
                      reserved BIGINT NOT NULL DEFAULT 0,
                      recovery_ref TEXT
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS idempotency (
                      tenant TEXT NOT NULL,
                      actor TEXT NOT NULL,
                      key TEXT NOT NULL,
                      param_digest TEXT NOT NULL,
                      task_id TEXT NOT NULL,
                      PRIMARY KEY (tenant, actor, key)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                      event_id TEXT PRIMARY KEY,
                      task_id TEXT NOT NULL,
                      event_seq INT NOT NULL,
                      type TEXT NOT NULL,
                      UNIQUE (task_id, event_seq)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS outbox (
                      message_id TEXT PRIMARY KEY,
                      task_id TEXT NOT NULL,
                      dispatch_seq INT NOT NULL,
                      expected_epoch INT NOT NULL,
                      schedule_type TEXT NOT NULL,
                      status TEXT NOT NULL,
                      run_version TEXT NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS inbox (
                      consumer TEXT NOT NULL,
                      message_id TEXT NOT NULL,
                      conclusion TEXT NOT NULL,
                      epoch_after INT NOT NULL,
                      PRIMARY KEY (consumer, message_id)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS operations (
                      operation_id TEXT PRIMARY KEY,
                      task_id TEXT NOT NULL,
                      node_key TEXT,
                      param_digest TEXT,
                      status TEXT NOT NULL
                    )
                    """);
            statement.execute("SELECT pg_advisory_unlock(42001)");
            connection.commit();
        }
        try (Connection connection = connectFake(config); Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(42002)");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS fake_results (
                      operation_id TEXT PRIMARY KEY,
                      param_digest TEXT NOT NULL,
                      body TEXT NOT NULL,
                      write_count INT NOT NULL DEFAULT 1
                    )
                    """);
            statement.execute("SELECT pg_advisory_unlock(42002)");
            connection.commit();
        }
    }

    public static void resetDemoData(LiveConfig config) throws SQLException {
        try (Connection connection = connect(config); Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE tasks, idempotency, events, outbox, inbox, operations");
            connection.commit();
        }
        try (Connection connection = connectFake(config); Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE fake_results");
            connection.commit();
        }
    }
}
