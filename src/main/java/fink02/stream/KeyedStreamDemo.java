package fink02.stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KeyedStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();

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
                        TimeUnit.SECONDS.sleep(1);
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

        KeyedStream<WC, String> wcStringKeyedStream = source.keyBy(WC::getWord);
//        wcStringKeyedStream.sum("count").printToErr();
        // maxBy 为最大的那条记录
//        wcStringKeyedStream.maxBy("count").printToErr();
         // 只保留最大的结果集合
        wcStringKeyedStream.max("count").printToErr();
//                .sum("wc.count").printToErr();

        e.execute();
    }
}
