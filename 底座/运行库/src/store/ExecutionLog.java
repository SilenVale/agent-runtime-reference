package store;

import java.util.ArrayList;
import java.util.List;

public final class ExecutionLog {
    private final List<String> events = new ArrayList<>();

    public void record(String event) {
        events.add(event);
    }

    public boolean containsPrefix(String prefix) {
        for (String event : events) {
            if (event.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
