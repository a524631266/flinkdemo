package flinkbase.model;

public enum EventEnum {
    PU(0),Click(1),Part(2);
    private final int value;
    EventEnum(int i) {
        this.value = i;
    }
}
