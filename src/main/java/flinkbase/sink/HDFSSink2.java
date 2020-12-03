package flinkbase.sink;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 1. 添加这个代码
 *
 *
 */
public class HDFSSink2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =EnvUtil.getLocalEnv();
        String checkPath = "hdfs://192.168.10.61:8020/flink/checkpoint/hdfs";
        String filedataPath = "hdfs://192.168.10.61:8020/flink/hdfs";
        EnvUtil.setCheckpointWithHDFS(env, checkPath);

        // 添加source
        SourceFunction<Person> source = SourceUtil.createStreamSource(Person.class, 1L);

        SingleOutputStreamOperator<Person> returns = env.addSource(source).returns(Person.class);
        SingleOutputStreamOperator<String> map = returns.map(person ->
                String.format("%s,%s,%s,%s,%s",person.getId(),
                                person.getName(),
                                person.getAge(),
                                person.getBirthDay(),
                                person.getAddress().getAddress()));
        StreamingFileSink<String> sink = StreamingFileSink.
                forRowFormat(new Path(filedataPath), new SimpleStringEncoder<String>("UTF-8"))
                // 保证存储的分区为当日的数据
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
                // 这个表示不会分桶存储，只存在某个目录中！
                .withRollingPolicy(
                        OnCheckpointRollingPolicy.build()
                ).build();
        map.addSink(sink);
        map.printToErr();
        env.execute("hdfs");
    }
}
