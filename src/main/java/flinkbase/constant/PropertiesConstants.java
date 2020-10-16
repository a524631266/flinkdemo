package flinkbase.constant;

public class PropertiesConstants {
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    /**
     * application.properties 中的参数列表
     */
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";


    /**
     * kafka默认配置 在官方配置前+kafka前缀
     */
    public static final String KAFKA_BROKERS = "kafka.bootstrap.servers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";

    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";

    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "default";


    // 这个变量是用来做 sink的目标用的
    public static final String KAFKA_SINK_BROKERS = "kafka.sink.brokers";
    public static final String DEFAULT_KAFKA_SINK_BROKERS = "localhost:9092";
    public static final String KAFKA_SINK_TOPIC = "kafka.sink.topic";
    public static final String DEFAULT_KAFKA_SINK_TOPIC = "default";

    //
    public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";
    public static final String DEFAULT_KAFKA_AUTO_OFFSET_RESET = "latest";

    /**
     * kakfa序列化名称
     */
    public static final String KAFKA_KEY_DESERIALIZER = "kafka.key.deserializer";
    public static final String DEFAULT_KAFKA_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String KAFKA_VALUE_DESERIALIZER = "kafka.value.deserializer";
    public static final String DEFAULT_KAFKA_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";


    public static final String KAFKA_KEY_SERIALIZER = "kafka.key.Serializer";
    public static final String DEFAULT_KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KAFKA_VALUE_SERIALIZER = "kafka.value.Serializer";
    public static final String DEFAULT_KAFKA_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String JOB_NAME = "job.name";

}
