package demo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.UUID;

import live.LiveConfig;
import live.Pg;

/**
 * 第 4 章云端取证（真实 PostgreSQL + MySQL 假写）。由 scripts/cloud-chapter4.sh 调用。
 * 不重跑已通过的三段演示硬条件；4.3 由改存储后的 demo 证据覆盖。
 */
public final class Chapter4Cloud {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("需要子命令");
        }
        switch (args[0]) {
            case "reset" -> {
                LiveConfig c = LiveConfig.fromEnv();
                Pg.ensureSchema(c);
                Pg.resetDemoData(c);
                System.out.println("reset-ok");
            }
            case "r1" -> System.out.print(rule1());
            case "r2" -> System.out.print(rule2());
            case "r4" -> System.out.print(rule4());
            case "r5" -> System.out.print(rule5());
            case "r6" -> System.out.print(rule6());
            case "r7" -> System.out.print(rule7());
            case "r8" -> System.out.print(rule8());
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static String rule1() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String taskId = seedRunning(config, 1, "owner-a", minutesFromNow(10));
        StringBuilder out = new StringBuilder();
        try (Connection connection = Pg.connect(config);
                PreparedStatement bad = connection.prepareStatement(
                        "UPDATE tasks SET state='SUCCEEDED' WHERE task_id=? AND epoch=0 AND owner=?")) {
            bad.setString(1, taskId);
            bad.setString(2, "owner-a");
            int changed = bad.executeUpdate();
            connection.rollback();
            out.append("旧代次提交 changed=").append(changed).append('\n');
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement renew = connection.prepareStatement(
                        """
                        UPDATE tasks SET lease_expires_at=?
                        WHERE task_id=? AND epoch=1 AND owner=?
                          AND lease_expires_at IS NOT NULL AND lease_expires_at > now()
                        """)) {
            Timestamp next = minutesFromNow(20);
            renew.setTimestamp(1, next);
            renew.setString(2, taskId);
            renew.setString(3, "owner-a");
            int ok = renew.executeUpdate();
            connection.commit();
            out.append("数据库时钟未到期续租 changed=").append(ok).append('\n');
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement expire = connection.prepareStatement(
                        "UPDATE tasks SET lease_expires_at=now() - interval '1 second' WHERE task_id=?")) {
            expire.setString(1, taskId);
            expire.executeUpdate();
            connection.commit();
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement renew = connection.prepareStatement(
                        """
                        UPDATE tasks SET lease_expires_at=?
                        WHERE task_id=? AND epoch=1 AND owner=?
                          AND lease_expires_at IS NOT NULL AND lease_expires_at > now()
                        """)) {
            renew.setTimestamp(1, minutesFromNow(30));
            renew.setString(2, taskId);
            renew.setString(3, "owner-a");
            int ok = renew.executeUpdate();
            connection.rollback();
            out.append("数据库时钟已到期续租 changed=").append(ok).append('\n');
        }
        return out.toString();
    }

    static String rule2() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String taskId = seedRunning(config, 1, "owner-a", minutesFromNow(10));
        String first = prepareOp(config, taskId, "node-write", "digest-1");
        String second = prepareOp(config, taskId, "node-write", "digest-1");
        StringBuilder out = new StringBuilder();
        out.append("operation_id first=").append(first).append(" second=").append(second)
                .append(" same=").append(first.equals(second)).append('\n');
        String status;
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT status FROM operations WHERE operation_id=?")) {
            q.setString(1, first);
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                status = rs.getString(1);
            }
            connection.commit();
        }
        out.append("账本 status=").append(status).append('\n');
        boolean rejected = false;
        try {
            prepareOp(config, taskId, "node-write", "digest-2");
        } catch (IllegalStateException ex) {
            rejected = ex.getMessage().contains("换参数");
        }
        out.append("换参数拒绝=").append(rejected).append('\n');
        return out.toString();
    }

    static String rule4() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        Pg.resetDemoData(config);
        StringBuilder out = new StringBuilder();
        try (Connection connection = Pg.connect(config);
                PreparedStatement op = connection.prepareStatement(
                        "INSERT INTO operations(operation_id, task_id, node_key, param_digest, status) VALUES (?,?,?,?,?)")) {
            op.setString(1, "op-ledger");
            op.setString(2, "task-x");
            op.setString(3, "node");
            op.setString(4, "d");
            op.setString(5, "SUCCEEDED");
            op.executeUpdate();
            connection.commit();
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement cp = connection.prepareStatement(
                        "INSERT INTO checkpoints(checkpoint_id, task_id, epoch, node_key) VALUES (?,?,?,?)")) {
            cp.setString(1, "task-x:epoch-1");
            cp.setString(2, "task-x");
            cp.setInt(3, 1);
            cp.setString(4, "node-9");
            cp.executeUpdate();
            connection.rollback();
        }
        int ops = count(config, "SELECT COUNT(*) FROM operations WHERE operation_id='op-ledger'");
        int cps = count(config, "SELECT COUNT(*) FROM checkpoints WHERE checkpoint_id='task-x:epoch-1'");
        out.append("账本已提交 ops=").append(ops).append(" Checkpoint回滚 cps=").append(cps).append('\n');

        try (Connection connection = Pg.connect(config);
                PreparedStatement cp = connection.prepareStatement(
                        "INSERT INTO checkpoints(checkpoint_id, task_id, epoch, node_key) VALUES (?,?,?,?)")) {
            cp.setString(1, "task-y:epoch-1");
            cp.setString(2, "task-y");
            cp.setInt(3, 1);
            cp.setString(4, "node-1");
            cp.executeUpdate();
            connection.commit();
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement op = connection.prepareStatement(
                        "INSERT INTO operations(operation_id, task_id, node_key, param_digest, status) VALUES (?,?,?,?,?)")) {
            op.setString(1, "op-rolled");
            op.setString(2, "task-y");
            op.setString(3, "node");
            op.setString(4, "d");
            op.setString(5, "SUCCEEDED");
            op.executeUpdate();
            connection.rollback();
        }
        int cps2 = count(config, "SELECT COUNT(*) FROM checkpoints WHERE checkpoint_id='task-y:epoch-1'");
        int ops2 = count(config, "SELECT COUNT(*) FROM operations WHERE operation_id='op-rolled'");
        out.append("图位置单独提交 cps=").append(cps2).append(" 未提交账本 ops=").append(ops2).append('\n');
        return out.toString();
    }

    static String rule5() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String taskId = seedRunning(config, 1, "owner-a", minutesFromNow(5));
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        "UPDATE tasks SET spent=40, reserved=0 WHERE task_id=?")) {
            u.setString(1, taskId);
            u.executeUpdate();
            connection.commit();
        }
        // 模拟租约到期后新 owner 接管，保留 spent
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        """
                        UPDATE tasks SET epoch=2, owner='owner-b', lease_expires_at=?
                        WHERE task_id=? AND epoch=1
                        """)) {
            u.setTimestamp(1, minutesFromNow(10));
            u.setString(2, taskId);
            u.executeUpdate();
            connection.commit();
        }
        long spent;
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement("SELECT spent, epoch FROM tasks WHERE task_id=?")) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                spent = rs.getLong("spent");
                outSpentEpoch = rs.getInt("epoch");
            }
            connection.commit();
        }
        StringBuilder out = new StringBuilder();
        out.append("接管后 spent=").append(spent).append(" epoch=").append(outSpentEpoch).append('\n');
        try (Connection connection = Pg.connect(config);
                PreparedStatement bad = connection.prepareStatement(
                        "UPDATE tasks SET spent=0 WHERE task_id=? AND epoch=1 AND owner='owner-a'")) {
            bad.setString(1, taskId);
            int changed = bad.executeUpdate();
            connection.rollback();
            out.append("旧代次清零 changed=").append(changed).append('\n');
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement res = connection.prepareStatement(
                        "UPDATE tasks SET reserved=reserved+10 WHERE task_id=? AND epoch=2 AND owner='owner-b'")) {
            res.setString(1, taskId);
            res.executeUpdate();
            connection.commit();
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT spent, reserved FROM tasks WHERE task_id=?")) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                out.append("新预留后 spent=").append(rs.getLong("spent"))
                        .append(" reserved=").append(rs.getLong("reserved")).append('\n');
            }
            connection.commit();
        }
        return out.toString();
    }

    private static int outSpentEpoch;

    static String rule6() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String taskId = seedRunning(config, 1, "owner-a", minutesFromNow(10));
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        "UPDATE tasks SET spent=40, reserved=0 WHERE task_id=?")) {
            u.setString(1, taskId);
            u.executeUpdate();
            connection.commit();
        }
        reserve(config, taskId, "call-1", "res-1", 10);
        reserve(config, taskId, "call-1", "res-1", 10);
        long reserved = scalarLong(config, "SELECT reserved FROM tasks WHERE task_id='" + taskId + "'");
        settle(config, taskId, "call-1", 7);
        settle(config, taskId, "call-1", 7);
        long spent = scalarLong(config, "SELECT spent FROM tasks WHERE task_id='" + taskId + "'");
        long reservedAfter = scalarLong(config, "SELECT reserved FROM tasks WHERE task_id='" + taskId + "'");
        long settled = scalarLong(config, "SELECT settled FROM budget_calls WHERE call_id='call-1'");
        return "重复预留后 reserved=" + reserved
                + " 重复结算后 spent=" + spent
                + " reserved=" + reservedAfter
                + " settled=" + settled
                + "\n";
    }

    static String rule7() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String taskId = seedRunning(config, 1, "owner-a", minutesFromNow(10));
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        "UPDATE tasks SET spent=40, reserved=0 WHERE task_id=?")) {
            u.setString(1, taskId);
            u.executeUpdate();
            connection.commit();
        }
        reserve(config, taskId, "call-9", "res-9", 12);
        // 模拟进程退出：只查库，不再结算
        String state;
        long reserved;
        long spent;
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT b.state, t.reserved, t.spent FROM budget_calls b JOIN tasks t ON t.task_id=b.task_id WHERE b.call_id='call-9'")) {
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                state = rs.getString(1);
                reserved = rs.getLong(2);
                spent = rs.getLong(3);
            }
            connection.commit();
        }
        return "退出后预留 state=" + state + " reserved=" + reserved + " spent=" + spent + "\n";
    }

    static String rule8() throws Exception {
        LiveConfig config = LiveConfig.fromEnv();
        String taskId = seedRunning(config, 1, "owner-a", minutesFromNow(5));
        try (Connection connection = Pg.connect(config);
                PreparedStatement cp = connection.prepareStatement(
                        "INSERT INTO checkpoints(checkpoint_id, task_id, epoch, node_key) VALUES (?,?,?,?)")) {
            cp.setString(1, taskId + ":epoch-1");
            cp.setString(2, taskId);
            cp.setInt(3, 1);
            cp.setString(4, "from-epoch-1");
            cp.executeUpdate();
            connection.commit();
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        "UPDATE tasks SET epoch=2, owner='owner-b', lease_expires_at=? WHERE task_id=?")) {
            u.setTimestamp(1, minutesFromNow(10));
            u.setString(2, taskId);
            u.executeUpdate();
            connection.commit();
        }
        int oldRegister;
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        "UPDATE tasks SET recovery_ref=? WHERE task_id=? AND epoch=1 AND owner='owner-a'")) {
            u.setString(1, taskId + ":epoch-1");
            u.setString(2, taskId);
            oldRegister = u.executeUpdate();
            connection.rollback();
        }
        try (Connection connection = Pg.connect(config);
                PreparedStatement u = connection.prepareStatement(
                        "UPDATE tasks SET recovery_ref=? WHERE task_id=? AND epoch=2 AND owner='owner-b'")) {
            u.setString(1, taskId + ":epoch-1");
            u.setString(2, taskId);
            u.executeUpdate();
            connection.commit();
        }
        String ref;
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement("SELECT recovery_ref FROM tasks WHERE task_id=?")) {
            q.setString(1, taskId);
            try (ResultSet rs = q.executeQuery()) {
                rs.next();
                ref = rs.getString(1);
            }
            connection.commit();
        }
        return "旧代次登记 changed=" + oldRegister + " 当前引用 recovery_ref=" + ref + "\n";
    }

    private static String seedRunning(LiveConfig config, int epoch, String owner, Timestamp lease)
            throws Exception {
        String taskId = "task-" + UUID.randomUUID();
        try (Connection connection = Pg.connect(config);
                PreparedStatement insert = connection.prepareStatement(
                        """
                        INSERT INTO tasks(task_id, tenant, actor, idempotency_key, param_digest, run_version,
                          state, epoch, owner, lease_expires_at, budget_limit, spent, reserved)
                        VALUES (?, 'tenant', 'actor', ?, 'd', 'v1', 'RUNNING', ?, ?, ?, 100, 0, 0)
                        """)) {
            insert.setString(1, taskId);
            insert.setString(2, "key-" + taskId);
            insert.setInt(3, epoch);
            insert.setString(4, owner);
            insert.setTimestamp(5, lease);
            insert.executeUpdate();
            connection.commit();
        }
        return taskId;
    }

    private static String prepareOp(LiveConfig config, String taskId, String nodeKey, String digest)
            throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT operation_id, param_digest FROM operations WHERE task_id=? AND node_key=?")) {
            q.setString(1, taskId);
            q.setString(2, nodeKey);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) {
                    if (!digest.equals(rs.getString("param_digest"))) {
                        connection.rollback();
                        throw new IllegalStateException("换参数");
                    }
                    String id = rs.getString("operation_id");
                    connection.commit();
                    return id;
                }
            }
            String id = "op-" + UUID.randomUUID();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO operations(operation_id, task_id, node_key, param_digest, status) VALUES (?,?,?,?,?)")) {
                insert.setString(1, id);
                insert.setString(2, taskId);
                insert.setString(3, nodeKey);
                insert.setString(4, digest);
                insert.setString(5, "PLANNED");
                insert.executeUpdate();
            }
            connection.commit();
            return id;
        }
    }

    private static void reserve(LiveConfig config, String taskId, String callId, String resId, long amount)
            throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement existing = connection.prepareStatement(
                        "SELECT call_id FROM budget_calls WHERE call_id=?")) {
            existing.setString(1, callId);
            try (ResultSet rs = existing.executeQuery()) {
                if (rs.next()) {
                    connection.commit();
                    return;
                }
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO budget_calls(call_id, task_id, reservation_id, amount, settled, state) VALUES (?,?,?,?,0,'RESERVED')")) {
                insert.setString(1, callId);
                insert.setString(2, taskId);
                insert.setString(3, resId);
                insert.setLong(4, amount);
                insert.executeUpdate();
            }
            try (PreparedStatement u = connection.prepareStatement(
                    "UPDATE tasks SET reserved=reserved+? WHERE task_id=?")) {
                u.setLong(1, amount);
                u.setString(2, taskId);
                u.executeUpdate();
            }
            connection.commit();
        }
    }

    private static void settle(LiveConfig config, String taskId, String callId, long settled) throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(
                        "SELECT state, amount FROM budget_calls WHERE call_id=? FOR UPDATE")) {
            q.setString(1, callId);
            long amount;
            try (ResultSet rs = q.executeQuery()) {
                if (!rs.next()) {
                    connection.rollback();
                    return;
                }
                if (!"RESERVED".equals(rs.getString("state"))) {
                    connection.commit();
                    return;
                }
                amount = rs.getLong("amount");
            }
            try (PreparedStatement u = connection.prepareStatement(
                    "UPDATE budget_calls SET settled=?, state='SETTLED' WHERE call_id=? AND state='RESERVED'")) {
                u.setLong(1, settled);
                u.setString(2, callId);
                if (u.executeUpdate() != 1) {
                    connection.commit();
                    return;
                }
            }
            try (PreparedStatement t = connection.prepareStatement(
                    "UPDATE tasks SET spent=spent+?, reserved=GREATEST(reserved-?,0) WHERE task_id=?")) {
                t.setLong(1, settled);
                t.setLong(2, amount);
                t.setString(3, taskId);
                t.executeUpdate();
            }
            connection.commit();
        }
    }

    private static Timestamp minutesFromNow(int minutes) {
        return new Timestamp(System.currentTimeMillis() + minutes * 60_000L);
    }

    private static int count(LiveConfig config, String sql) throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(sql);
                ResultSet rs = q.executeQuery()) {
            rs.next();
            int v = rs.getInt(1);
            connection.commit();
            return v;
        }
    }

    private static long scalarLong(LiveConfig config, String sql) throws Exception {
        try (Connection connection = Pg.connect(config);
                PreparedStatement q = connection.prepareStatement(sql);
                ResultSet rs = q.executeQuery()) {
            rs.next();
            long v = rs.getLong(1);
            connection.commit();
            return v;
        }
    }
}
