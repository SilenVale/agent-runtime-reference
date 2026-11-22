package live;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import access.AcceptResult;

public final class LiveAcceptService {
    private final LiveConfig config;

    public LiveAcceptService(LiveConfig config) {
        this.config = config;
    }

    public AcceptResult accept(String tenant, String actor, String key, String digest, long budgetLimit)
            throws Exception {
        try (Connection connection = Pg.connect(config)) {
            try (PreparedStatement existing = connection.prepareStatement(
                    "SELECT task_id, param_digest FROM idempotency WHERE tenant=? AND actor=? AND key=?")) {
                existing.setString(1, tenant);
                existing.setString(2, actor);
                existing.setString(3, key);
                try (ResultSet rs = existing.executeQuery()) {
                    if (rs.next()) {
                        connection.rollback();
                        if (!digest.equals(rs.getString("param_digest"))) {
                            return AcceptResult.conflict();
                        }
                        return AcceptResult.existing(rs.getString("task_id"));
                    }
                }
            }
            String taskId = "task-" + UUID.randomUUID();
            String eventId = "event-" + UUID.randomUUID();
            String messageId = "msg-" + UUID.randomUUID();
            try (PreparedStatement task = connection.prepareStatement(
                    """
                    INSERT INTO tasks(task_id, tenant, actor, idempotency_key, param_digest, run_version,
                      state, epoch, owner, lease_expires_at, budget_limit, spent, reserved)
                    VALUES (?, ?, ?, ?, ?, 'v1', 'ACCEPTED', 0, NULL, NULL, ?, 0, 0)
                    """)) {
                task.setString(1, taskId);
                task.setString(2, tenant);
                task.setString(3, actor);
                task.setString(4, key);
                task.setString(5, digest);
                task.setLong(6, budgetLimit);
                task.executeUpdate();
            }
            try (PreparedStatement idem = connection.prepareStatement(
                    "INSERT INTO idempotency(tenant, actor, key, param_digest, task_id) VALUES (?,?,?,?,?)")) {
                idem.setString(1, tenant);
                idem.setString(2, actor);
                idem.setString(3, key);
                idem.setString(4, digest);
                idem.setString(5, taskId);
                idem.executeUpdate();
            }
            try (PreparedStatement event = connection.prepareStatement(
                    "INSERT INTO events(event_id, task_id, event_seq, type) VALUES (?,?,1,'任务已受理')")) {
                event.setString(1, eventId);
                event.setString(2, taskId);
                event.executeUpdate();
            }
            try (PreparedStatement outbox = connection.prepareStatement(
                    """
                    INSERT INTO outbox(message_id, task_id, dispatch_seq, expected_epoch, schedule_type, status, run_version)
                    VALUES (?, ?, 1, 0, 'START', 'PENDING', 'v1')
                    """)) {
                outbox.setString(1, messageId);
                outbox.setString(2, taskId);
                outbox.executeUpdate();
            }
            connection.commit();
            return AcceptResult.accepted(taskId);
        }
    }
}
