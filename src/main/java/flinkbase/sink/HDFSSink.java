package flinkbase.sink;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 1. 添加这个代码
 * <dependency>
 * 			<groupId>org.apache.flink</groupId>
 * 			<artifactId>flink-connector-filesystem_2.11</artifactId>
 * 			<version>1.11.2</version>
 * 		</dependency>
 *
 * 	1. 重点，使用hdfs StreamFileSink最终哟的是checkpoint必须开启
 *
 * 	可以完成，不过这个配置该怎么写
 *  2. 一旦 sink有被压，就会直接影响 source的产生速度？？？ why？
 *
 *  3. bucket assignment 用来指定分桶或者自定义分桶
 *
 *  4. 当有个任务是以1天为单位进行数据的存储的时候，该怎么办？
 *  一旦在这个过程中挂掉之后，会从挂掉的地方继续消费吗？
 *  这个是checkpoint一致保证的。因此要实现这个
 *
 */
public class HDFSSink {
    /**
     * 1.以下代码显示了每10分钟会新增一个分区 2020-12-3--13[14..15]/ 默认格式
     * @see
     * 2. 在分区内部会有一个inprogress和 正常的最后的part-0-1（以算子的分区数）作为应用程序的结果【叫做finished files】
     *
     *
     * hdfs 作用是设置 两种条件1. 最大应用存储大小2.时钟间隔到
     * 只要任一条件满足就会触发存储计算
     *
     * @param args
     * @throws Exception
     */
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
//                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
                // 这个表示不会分桶存储，只存在某个目录中！
                .withBucketAssigner(new BasePathBucketAssigner<String>(){
                    @Override
                    public String getBucketId(String element, Context context) {
                        // 185337,李四,1,2019-10-28,骨望就
                        // 在这个应用中，如果以事件时间作为桶名称，就要扩展这个功能！
                        System.out.println("element:"+ element);
                        return "abc";
                    }
                })
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 间隔15分钟，以15分钟为间隔，不过，这个15分钟间隔是根据时钟来判断的，
                                // 非程序开始时间。2 分钟e欸有
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                // 最大的大小？？1 m大小 超过1m大小就直接存到系统中！！，这个非常有用的一个特性
                                // 当应用程序没有多大内存的额时候就应该用大小，以及 时间作为存储的频率，并保证checkpoint!!
                                // 可以设置为128M，这样保证每个块都能够存储到！！
                                .withMaxPartSize(1024 * 1024 )
                                .build()
                ).build();
        map.addSink(sink);
        map.printToErr();
        env.execute("hdfs");
    }
    /**
     * 详解篇
     * 1. 这个程序在重新跑的过程中会新增数据分区，保证程序在后续启动可以继续添加
     *
     * 2. 速度可以多快？
     * 保证的数据不会出现挂了！！
     *
     *
     */
}
