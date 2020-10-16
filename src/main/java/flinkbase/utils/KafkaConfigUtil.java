package flinkbase.utils;

import flinkbase.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;
import static flinkbase.constant.PropertiesConstants.*;

public class KafkaConfigUtil {
    /**
     * 设置基础的 Kafka 配置
     *
     * @return
     */
    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    /**
     * 设置 kafka 基本配置
     *
     * @param parameterTool
     * @return
     */
    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS,
                                                    DEFAULT_KAFKA_BROKERS));
        props.put("zookeeper.connect", parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT,
                                                    DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        props.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID,
                                                    DEFAULT_KAFKA_GROUP_ID));
        props.put("key.deserializer", parameterTool.get(PropertiesConstants.KAFKA_KEY_DESERIALIZER,
                                                    DEFAULT_KAFKA_KEY_DESERIALIZER));
        props.put("value.deserializer", parameterTool.get(PropertiesConstants.KAFKA_VALUE_DESERIALIZER,
                                                    DEFAULT_KAFKA_VALUE_DESERIALIZER));

        props.put("key.serializer", parameterTool.get(PropertiesConstants.KAFKA_KEY_SERIALIZER,
                                                    DEFAULT_KAFKA_KEY_SERIALIZER));
        props.put("value.serializer", parameterTool.get(PropertiesConstants.KAFKA_VALUE_SERIALIZER,
                                                    DEFAULT_KAFKA_VALUE_SERIALIZER));
        props.put("auto.offset.reset", parameterTool.get(PropertiesConstants.KAFKA_AUTO_OFFSET_RESET, DEFAULT_KAFKA_AUTO_OFFSET_RESET));

        return props;
    }

//    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env) throws IllegalAccessException {
//        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
//        String topic = parameter.getRequired(PropertiesConstants.METRICS_TOPIC);
//        Long time = parameter.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);
//        return buildSource(env, topic, time);
//    }
//
//    /**
//     * @param env
//     * @param topic
//     * @param time  订阅的时间
//     * @return
//     * @throws IllegalAccessException
//     */
//    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env, String topic, Long time) throws IllegalAccessException {
//        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
//        Properties props = buildKafkaProps(parameterTool);
//        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(
//                topic,
//                new MetricSchema(),
//                props);
//        //重置offset到time时刻
//        if (time != 0L) {
//            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(props, parameterTool, time);
//            consumer.setStartFromSpecificOffsets(partitionOffset);
//        }
//        return env.addSource(consumer);
//    }
//
//    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties props, ParameterTool parameterTool, Long time) {
//        props.setProperty("group.id", "query_time_" + time);
//        KafkaConsumer consumer = new KafkaConsumer(props);
//        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC));
//        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
//        for (PartitionInfo partitionInfo : partitionsFor) {
//            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
//        }
//        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
//        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
//        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));
//
//        consumer.close();
//        return partitionOffset;
//    }
}
