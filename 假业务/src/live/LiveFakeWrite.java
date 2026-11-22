package live;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 假写操作：认 operation_id，同号不写第二次。
 */
public final class LiveFakeWrite {
    private final LiveConfig config;

    public LiveFakeWrite(LiveConfig config) {
        this.config = config;
    }

    public String write(String operationId, String digest, String body) throws Exception {
        try (Connection connection = Pg.connectFake(config)) {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT body, param_digest, write_count FROM fake_results WHERE operation_id=?")) {
                select.setString(1, operationId);
                try (ResultSet rs = select.executeQuery()) {
                    if (rs.next()) {
                        if (!digest.equals(rs.getString("param_digest"))) {
                            throw new IllegalStateException("同号不能换参数");
                        }
                        connection.commit();
                        return rs.getString("body");
                    }
                }
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO fake_results(operation_id, param_digest, body, write_count) VALUES (?,?,?,1)")) {
                insert.setString(1, operationId);
                insert.setString(2, digest);
                insert.setString(3, body);
                insert.executeUpdate();
            }
            connection.commit();
            return body;
        }
    }

    public String query(String operationId) throws Exception {
        try (Connection connection = Pg.connectFake(config);
                PreparedStatement select = connection.prepareStatement(
                        "SELECT body FROM fake_results WHERE operation_id=?")) {
            select.setString(1, operationId);
            try (ResultSet rs = select.executeQuery()) {
                if (!rs.next()) {
                    connection.commit();
                    return null;
                }
                String body = rs.getString("body");
                connection.commit();
                return body;
            }
        }
    }

    public int writes(String operationId) throws Exception {
        try (Connection connection = Pg.connectFake(config);
                PreparedStatement select = connection.prepareStatement(
                        "SELECT write_count FROM fake_results WHERE operation_id=?")) {
            select.setString(1, operationId);
            try (ResultSet rs = select.executeQuery()) {
                if (!rs.next()) {
                    connection.commit();
                    return 0;
                }
                int count = rs.getInt(1);
                connection.commit();
                return count;
            }
        }
    }
}
