package flinkbase.configuse;

import flinkbase.model.Person;
import flinkbase.source.MysqlConfiguration;
import flinkbase.utils.EnvUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

public class GlobalParameterUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();

        ExecutionConfig config = env.getConfig();
        ExecutionConfig.GlobalJobParameters globalConfig = new Configuration();
        ((Configuration) globalConfig).setInteger(
                MysqlConfiguration.class.getDeclaredField("port").getName(),
                MysqlConfiguration.port);
        config.setGlobalJobParameters(globalConfig);



        DataStreamSource<Person> source = env.addSource(new RichSourceFunction<Person>() {
            private boolean isRunning = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
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
                System.out.println(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
            }

            @Override
            public Object map(Person value) throws Exception {
                return null;
            }
        }).printToErr();
        env.execute("test global config");
    }
}
