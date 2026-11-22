package store;

public final class Resync extends RuntimeException {
    public Resync(String message) {
        super(message);
    }
}
