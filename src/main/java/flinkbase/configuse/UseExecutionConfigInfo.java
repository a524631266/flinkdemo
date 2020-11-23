package flinkbase.configuse;

import com.mysql.cj.jdbc.MysqlDataSource;
import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.PrintUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class UseExecutionConfigInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();

        ExecutionConfig config = env.getConfig();
        config.setParallelism(10);
        DataStreamSource<Person> source = env.addSource(new RichSourceFunction<Person>() {
            private boolean isRunning = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
                System.out.println(executionConfig.getParallelism());
            }

            @Override
            public void run(SourceContext<Person> ctx) throws Exception {
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        source.map(new RichMapFunction<Person, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(getRuntimeContext().getExecutionConfig().getParallelism());
            }

            @Override
            public Object map(Person value) throws Exception {
                return null;
            }
        }).printToErr();

        env.execute("test config");
    }


}
