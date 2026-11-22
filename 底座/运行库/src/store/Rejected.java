package store;

public final class Rejected extends RuntimeException {
    public Rejected(String message) {
        super(message);
    }
}
