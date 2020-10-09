package flinkbase.stream;

import flinkbase.utils.EnvUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConnectedStreamDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.setCheckpoint(EnvUtil.getLocalWebEnv());
//        env.addSource(new SourceFunction<>() {
//        })
    }
}
