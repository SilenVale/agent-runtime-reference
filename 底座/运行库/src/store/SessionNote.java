package store;

public final class SessionNote {
    private final String conversationId;
    private final String text;

    public SessionNote(String conversationId, String text) {
        this.conversationId = conversationId;
        this.text = text;
    }

    public String conversationId() {
        return conversationId;
    }

    public String text() {
        return text;
    }
}
