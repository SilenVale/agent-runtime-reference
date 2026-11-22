package store;

public final class Ids {
    private int sequence;

    public String next(String prefix) {
        sequence++;
        return prefix + "-" + sequence;
    }
}
