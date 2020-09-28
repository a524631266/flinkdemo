package fink02.stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KeyedStreamDemo {
    @Data
    @AllArgsConstructor
    static class WC {
        public String word;
        public int count;
    }
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
                    ctx.collect(new WC(keyName, a));
                    ctx.collect(new WC(keyName2, a * a));
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

        source.keyBy(WC::getWord).addSink(new PrintSinkFunction<>());
        e.execute();
    }
}
