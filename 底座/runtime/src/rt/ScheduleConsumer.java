package rt;

import java.util.List;

import store.Database;
import store.EventRow;
import store.Ids;
import store.InboxRow;
import store.KafkaLog;
import store.OutboxRow;
import store.QuotaRow;
import store.ScheduleMessage;
import store.TaskMachine;
import store.TaskRow;
import store.TaskState;
import store.Transaction;

/**
 * 关闭自动提交。先占本地名额，再在短事务里决定接管、忽略或延期。
 * Inbox 的键是逻辑消费方加消息 ID。逻辑消费方是消费组，不是 Pod。
 */
public final class ScheduleConsumer {
    public static final long LEASE_MS = 30_000L;

    private final Ids ids;
    private final TaskMachine machine = new TaskMachine();

    public ScheduleConsumer(Ids ids) {
        this.ids = ids;
    }

    public Outcome consumeNext(
            Database database,
            KafkaLog kafka,
            String group,
            String podId,
            LocalSlots slots,
            List<String> trace,
            boolean commitTx,
            boolean commitOffset) {
        KafkaLog.Delivered delivered = kafka.pollOne(group);
        if (delivered == null) {
            return Outcome.empty();
        }
        if (!slots.tryAcquire()) {
            return Outcome.noSlot();
        }
        trace.add("tx");
        ScheduleMessage message = delivered.message();
        boolean committed = false;
        Outcome outcome = Outcome.empty();
        try (Transaction tx = database.begin()) {
            InboxRow existing = tx.inbox(group, message.messageId());
            if (existing != null) {
                outcome = Outcome.reused(existing.conclusion(), existing.epochAfter());
                committed = finish(tx, commitTx);
            } else {
                TaskRow task = tx.requireTask(message.taskId());
                if (!eligible(tx, task, message)) {
                    tx.saveInbox(new InboxRow(group, message.messageId(), "IGNORED", task.epoch()));
                    outcome = Outcome.ignored(task.epoch());
                    committed = finish(tx, commitTx);
                } else {
                    QuotaRow quota = tx.quota(task.tenant());
                    if (quota != null && quota.used() >= quota.limit()) {
                        int nextSeq = tx.maxDispatchSeq(task.taskId()) + 1;
                        tx.saveOutbox(new OutboxRow(
                                ids.next("msg"),
                                task.taskId(),
                                nextSeq,
                                task.epoch(),
                                "DEFER",
                                "PENDING",
                                task.runVersion()));
                        tx.saveInbox(new InboxRow(group, message.messageId(), "DEFERRED", task.epoch()));
                        outcome = Outcome.deferred(task.epoch());
                        committed = finish(tx, commitTx);
                    } else {
                        TaskRow taken = machine.takeover(tx, task.taskId(), podId, tx.now() + LEASE_MS);
                        if (quota != null) {
                            tx.saveQuota(quota.occupy());
                        }
                        tx.saveInbox(new InboxRow(group, message.messageId(), "TAKEN", taken.epoch()));
                        tx.saveEvent(new EventRow(
                                ids.next("event"),
                                task.taskId(),
                                tx.nextEventSeq(task.taskId()),
                                "已接管"));
                        outcome = Outcome.taken(taken.epoch(), taken.state());
                        committed = finish(tx, commitTx);
                    }
                }
            }
        }
        if (committed && commitOffset) {
            kafka.commitOffset(group, delivered.index() + 1);
        }
        if (!committed || outcome.releasesSlot()) {
            slots.release();
        }
        return outcome;
    }

    private static boolean finish(Transaction tx, boolean commitTx) {
        if (commitTx) {
            tx.commit();
            return true;
        }
        tx.rollback();
        return false;
    }

    private static boolean eligible(Transaction tx, TaskRow task, ScheduleMessage message) {
        if (task.state().terminal()) {
            return false;
        }
        if (!message.runVersion().equals(task.runVersion())) {
            return false;
        }
        if (message.expectedEpoch() != task.epoch()) {
            return false;
        }
        if (holdsLease(tx, task)) {
            return false;
        }
        if ("START".equals(message.scheduleType()) || "DEFER".equals(message.scheduleType())) {
            return task.state() == TaskState.ACCEPTED;
        }
        if ("RECOVERY".equals(message.scheduleType())) {
            return task.state() == TaskState.RUNNING;
        }
        return false;
    }

    private static boolean holdsLease(Transaction tx, TaskRow task) {
        return task.owner() != null && task.leaseExpiresAt() != null && task.leaseExpiresAt() > tx.now();
    }

    public static final class Outcome {
        private final String kind;
        private final String conclusion;
        private final int epoch;
        private final TaskState state;

        private Outcome(String kind, String conclusion, int epoch, TaskState state) {
            this.kind = kind;
            this.conclusion = conclusion;
            this.epoch = epoch;
            this.state = state;
        }

        public static Outcome empty() {
            return new Outcome("EMPTY", null, -1, null);
        }

        public static Outcome noSlot() {
            return new Outcome("NO_SLOT", null, -1, null);
        }

        public static Outcome reused(String conclusion, int epoch) {
            return new Outcome("REUSED", conclusion, epoch, null);
        }

        public static Outcome ignored(int epoch) {
            return new Outcome("IGNORED", "IGNORED", epoch, null);
        }

        public static Outcome deferred(int epoch) {
            return new Outcome("DEFERRED", "DEFERRED", epoch, null);
        }

        public static Outcome taken(int epoch, TaskState state) {
            return new Outcome("TAKEN", "TAKEN", epoch, state);
        }

        public String kind() {
            return kind;
        }

        public int epoch() {
            return epoch;
        }

        public TaskState state() {
            return state;
        }

        public boolean releasesSlot() {
            return !"TAKEN".equals(kind);
        }
    }
}
