package store;

public final class InboxRow {
    private final String consumer;
    private final String messageId;
    private final String conclusion;
    private final int epochAfter;

    public InboxRow(String consumer, String messageId, String conclusion, int epochAfter) {
        this.consumer = consumer;
        this.messageId = messageId;
        this.conclusion = conclusion;
        this.epochAfter = epochAfter;
    }

    public String consumer() {
        return consumer;
    }

    public String messageId() {
        return messageId;
    }

    public String conclusion() {
        return conclusion;
    }

    public int epochAfter() {
        return epochAfter;
    }

    public String id() {
        return consumer + "|" + messageId;
    }
}
