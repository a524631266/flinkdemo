package flinkbase.transformation.window;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.FocusUtil;
import flinkbase.utils.SourceUtil;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Simple01TimeWindow {
    private static class Splitter implements FlatMapFunction<Word, Tuple2<String, Integer>> {
        @Override
        public void flatMap(Word value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] split = value.data.split("\t");
            for (String s : split) {
                out.collect(new Tuple2<>(s, 1));
            }
        }
    }

    @Data
    @ToString
    public static class Word{
        @BasicTokenInfo("/[a-z][0-9]\t[a-z][0-9][7-9]/")
        private String data;
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        SourceFunction<Word> source = SourceUtil.createStreamSource(Word.class);
        SingleOutputStreamOperator<Word> startStream = env.addSource(source).returns(Word.class);
//        startStream.
//        startStream.keyBy()
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream =
                startStream.flatMap(new Splitter())
                .keyBy(v -> v.f0).timeWindow(Time.seconds(3))
                .sum(1);


//        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream =
//                startStream.flatMap(new Splitter())
//                        .keyBy(v -> v.f0)
//                        .window(new As)
//                        .sum(1);
        dataStream.print();
        startStream.printToErr();
        FocusUtil.start(env, "simple", 10);
    }
}
