package flinkbase.source;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import flinkbase.utils.EnvUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlNoCDC {
    public static void main(String[] args) throws Exception {
////        StreamExecutionEnvironment defaultRemote = EnvUtil.createDefaultRemote();
//        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
////        BatchTableEnvironment.create()
////        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
////        streamTableEnvironment
//        https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/common.html#%E5%88%9B%E5%BB%BA-tableenvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String[] strings = tableEnv.listCatalogs();
        for (String string : strings) {
            System.out.println("catalogs: "+ string);
        }

        DebeziumSourceFunction<String> observer = MySQLSource.<String>builder()
                .port(3306)
                .hostname("192.168.10.51")
                .username("observer")
                .password("123456")
                .databaseList("efmapi")
                .tableList("organization", "node", "sensor", "entity")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> stringDataStreamSource = env.addSource(observer);
        stringDataStreamSource.print().setParallelism(1);

        env.execute("asdft");
    }
}
