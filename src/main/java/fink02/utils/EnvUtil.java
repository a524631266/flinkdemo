package fink02.utils;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class EnvUtil {
    public static StreamExecutionEnvironment getLocalWebEnv() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        configuration.setInteger(RestOptions.PORT, 8050);
        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        return localEnvironmentWithWebUI;
    }

    public static ExecutionEnvironment getLoalWebDataSetEnv(){
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        configuration.setInteger(RestOptions.PORT, 8050);
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        return env;
    }

    /**
     *  给env 设置checkpoint
     * @param env
     */
    public static StreamExecutionEnvironment setCheckpoint(StreamExecutionEnvironment env){
        CheckpointConfig config = env.getCheckpointConfig();

        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint时间间隔（在本地，会有一个（checkpoint_dir）/chk-1638）
        config.setCheckpointInterval(2000);
        // 最少间隔
        config.setMinPauseBetweenCheckpoints(500);
        config.setCheckpointTimeout(60000);
        // 同一时间只允许进行一次checkpoint
        config.setMaxConcurrentCheckpoints(1);
        // checkpiont 一般默认会把状态存储下来
        env.setStateBackend(new FsStateBackend("file:///E://github/flinkdemo/data"));
        return env;
    }


    public static StreamExecutionEnvironment setWaterMarkerConif(StreamExecutionEnvironment env){
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置自动的water mark的时间间隔，这个是每久可以注入一个watermark
        env.getConfig().setAutoWatermarkInterval(1000L);
        return env;
    }

    public static StreamExecutionEnvironment createRemote(String host, int port, int parallelism, String jarFiles) {
        return StreamExecutionEnvironment.createRemoteEnvironment(host, port , parallelism,jarFiles);
//        return null;
    }
    public static StreamExecutionEnvironment createDefaultRemote() {
        String host = "192.168.10.63";
        int port = 30081;
        int parallelism = 2;
        String jarFiles = "E:\\github\\flinkdemo\\target\\flinkdemo-1.0-SNAPSHOT.jar";
        return createRemote(host, port , parallelism,jarFiles);
//        return null;
    }

}

