package flinkbase.model;

public enum OrderState {
    // 等待到账
    WAITINGPAY(0),
    WAITINGRECIEVE(1),
    // 配货
    DISTRIBUTE(2),
    ;
    int i ;
    OrderState(int i) {
        this.i = i;
    }
}
