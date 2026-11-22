package rt;

import java.util.ArrayList;
import java.util.List;

public final class LocalSlots {
    private final int capacity;
    private final List<String> trace;
    private int held;

    public LocalSlots(int capacity, List<String> trace) {
        this.capacity = capacity;
        this.trace = trace;
    }

    public boolean tryAcquire() {
        if (held >= capacity) {
            return false;
        }
        held++;
        trace.add("slot");
        return true;
    }

    public void release() {
        if (held == 0) {
            throw new IllegalStateException("没有可释放的名额");
        }
        held--;
        trace.add("release");
    }

    public int held() {
        return held;
    }
}
