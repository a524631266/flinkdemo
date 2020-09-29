package fink02.webui;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StartDemo {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        configuration.setInteger(RestOptions.PORT, 8050);
        StreamExecutionEnvironment e = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    }
}
