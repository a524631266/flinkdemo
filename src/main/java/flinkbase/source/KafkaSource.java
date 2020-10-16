package flinkbase.source;

import flinkbase.source.mysql.model.Organization;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class KafkaSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.createDefaultRemote();

//        SourceFunction<?> source = new FlinkKafkaConsumer011<Organization>();
//        env.addSource(source);
    }
}
