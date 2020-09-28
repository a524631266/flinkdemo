package fink02.checkpoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {
    public static void main1(String[] args) {
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        conf.setInteger(RestOptions.PORT, 8050);
        StreamExecutionEnvironment e = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        e.setStateBackend(new FsStateBackend("file:///E://github/flinkdemo/data"));
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = e.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 没1s启动一个检查点
        checkpointConfig.setCheckpointInterval(2000);
        // 最少间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一次checkpoint
        checkpointConfig.setMaxConcurrentCheckpoints(1);
    }

}
