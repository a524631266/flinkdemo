package fink02.stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;



public class KeyedStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = e.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 没1s启动一个检查点
        checkpointConfig.setCheckpointInterval(2000);
        // 最少间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一次checkpoint
        checkpointConfig.setMaxConcurrentCheckpoints(1);


//        Configuration conf = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        conf.setInteger(RestOptions.PORT, 8050);
//        StreamExecutionEnvironment e = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        e.setStateBackend(new FsStateBackend("file:///E://github/flinkdemo/data"));


        DataStreamSource<WC> source = e.addSource(new SourceFunction<WC>() {
            @Override
            public void run(SourceContext<WC> ctx) throws Exception {
                Random random = new Random(30);
                // 不断地输出流
                IntStream ints = random.ints(0, 30);
                ints.forEach((a) -> {
                    String keyName = getWc(a);
                    String keyName2 = getWc(a*a);
                    try {
//                        TimeUnit.SECONDS.sleep(1);
                        TimeUnit.MILLISECONDS.sleep(200);
                    } catch ( InterruptedException e1 ) {
                        e1.printStackTrace();
                    }
                    WC wc = new WC(keyName, UUID.randomUUID().toString(), a);
                    WC wc1 = new WC(keyName2, UUID.randomUUID().toString(), a * a);
                    System.out.println(wc);
                    System.out.println(wc1);
                    ctx.collect(wc);
                    ctx.collect(wc1);
                });
                //   .collect(Collectors.toList());
            }

            private String getWc(int a) {
                String keyName = "";
                switch (a % 5){
                    case 0:
                        keyName = "a";
                        break;
                    case 1:
                        keyName = "b";
                        break;
                    case 2:
                        keyName = "c";
                        break;
                    case 3:
                        keyName = "d";
                        break;
                    case 4:
                        keyName = "e";
                        break;
                    default:
                        keyName = "b";
                }
                return keyName;
            }

            @Override
            public void cancel() {

            }
        });


//        source.keyBy(WC::getWord).addSink(new PrintSinkFunction<>());
        // key by 方案一，使用方法简单的方法
        KeyedStream<WC, String> wcStringKeyedStream = source.keyBy(WC::getWord);

        KeyedStream<WC, Integer> wcIntegerKeyedStream = wcStringKeyedStream.keyBy(new KeySelector<WC, Integer>() {
            @Override
            public Integer getKey(WC value) throws Exception {
                return value.getCount();
            }
        });

        wcIntegerKeyedStream.printToErr();
//        wcStringKeyedStream.sum("count").printToErr();
        // maxBy 为最大的那条记录,包括其他的值也一起变动
        /// select id,max(age) from user group by id
//        wcStringKeyedStream.maxBy("count").printToErr();
        // 只保留最大的结果集合，其他值随机，关注点是在max上
        // select id,max(age) from user 这样记忆最牢固
//        wcStringKeyedStream.max("count").printToErr();
//                .sum("wc.count").printToErr();

        e.execute();
    }
    static class Inner{
        public String getWord(Integer aa){
            return "11";
        }
    }
}
