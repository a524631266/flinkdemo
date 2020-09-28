package fink02.checkpoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        conf.setInteger(RestOptions.PORT, 8050);
        StreamExecutionEnvironment e = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        e.setStateBackend(new FsStateBackend("file:///E://github/flinkdemo/data"));
    }
}
