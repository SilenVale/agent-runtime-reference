package store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 的进程内模型。发送成功只表示日志里有这条消息。消费位移单独提交。
 */
public final class KafkaLog {
    private final List<ScheduleMessage> messages = new ArrayList<>();
    private final Map<String, Integer> offsets = new HashMap<>();
    private boolean loseNextAck;

    public void loseNextAck() {
        loseNextAck = true;
    }

    public boolean append(ScheduleMessage message) {
        messages.add(message);
        if (loseNextAck) {
            loseNextAck = false;
            return false;
        }
        return true;
    }

    public List<ScheduleMessage> messages() {
        return List.copyOf(messages);
    }

    public Delivered pollOne(String group) {
        int offset = offsets.getOrDefault(group, 0);
        if (offset >= messages.size()) {
            return null;
        }
        return new Delivered(offset, messages.get(offset));
    }

    public void commitOffset(String group, int nextOffset) {
        int current = offsets.getOrDefault(group, 0);
        if (nextOffset < current) {
            throw new Rejected("offset不能倒退");
        }
        offsets.put(group, nextOffset);
    }

    public int offset(String group) {
        return offsets.getOrDefault(group, 0);
    }

    public static final class Delivered {
        private final int index;
        private final ScheduleMessage message;

        public Delivered(int index, ScheduleMessage message) {
            this.index = index;
            this.message = message;
        }

        public int index() {
            return index;
        }

        public ScheduleMessage message() {
            return message;
        }
    }
}
