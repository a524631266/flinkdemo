package fink02.stream;

import fink02.utils.EnvUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ConnectedStreamDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.setCheckpoint(EnvUtil.getLocalWebEnv());
//        env.addSource(new SourceFunction<>() {
//        })
    }
}
