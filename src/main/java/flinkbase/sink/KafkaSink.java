package flinkbase.sink;

import flinkbase.constant.PropertiesConstants;
import flinkbase.source.MySqlCDCStarter;
import flinkbase.source.mysql.model.Organization;
import flinkbase.typeinfo.KryoSchema2;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.ExecutionEnvUtil;
import flinkbase.utils.KafkaConfigUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.types.logical.IntType;

import java.util.Properties;

public class KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
//        StreamExecutionEnvironment env = EnvUtil.createDefaultRemote();

        SourceFunction<Organization> mysqlSource = new MySqlCDCStarter().generate();
        DataStreamSource<Organization> startSource = env.addSource(mysqlSource);
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        Properties kafkaProps = KafkaConfigUtil.buildKafkaProps(parameterTool);
        ExecutionConfig config = env.getConfig();
//        startSource.addSink(
//                new FlinkKafkaProducer011<Organization>(
//                        "org",
//                        new TypeInformationSerializationSchema<Organization>(
//                                TypeInformation.of(Organization.class),
//                                new KryoSerializer<Organization>(Organization.class,config)
//                        ),
//                        kafkaProps)
//                );
        String topicId = kafkaProps.getProperty(PropertiesConstants.KAFKA_SINK_TOPIC);
        String brokeList = kafkaProps.getProperty(PropertiesConstants.KAFKA_SINK_BROKERS);
        SerializationSchema<Organization> scheme = new KryoSchema2(config);
        startSource.addSink(new FlinkKafkaProducer011<Organization>(
                brokeList,
                topicId,
                scheme
        ));
//        startSource.print();
        env.execute("orgstart ");
    }

}
