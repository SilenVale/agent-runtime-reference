package store;

public final class BudgetCall {
    private final String callId;
    private final String reservationId;
    private final String taskId;
    private final long amount;
    private final long settled;
    private final String state;

    public BudgetCall(String callId, String reservationId, String taskId, long amount, String state) {
        this(callId, reservationId, taskId, amount, 0, state);
    }

    private BudgetCall(String callId, String reservationId, String taskId, long amount, long settled, String state) {
        this.callId = callId;
        this.reservationId = reservationId;
        this.taskId = taskId;
        this.amount = amount;
        this.settled = settled;
        this.state = state;
    }

    public String callId() {
        return callId;
    }

    public String reservationId() {
        return reservationId;
    }

    public String taskId() {
        return taskId;
    }

    public long amount() {
        return amount;
    }

    public long settled() {
        return settled;
    }

    public String state() {
        return state;
    }

    public BudgetCall settle(long actual) {
        return new BudgetCall(callId, reservationId, taskId, amount, actual, "SETTLED");
    }
}
