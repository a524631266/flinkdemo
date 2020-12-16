package flinkbase.transformation;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import flinkbase.transformation.window.Simple01TimeWindow;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.FocusUtil;
import flinkbase.utils.SourceUtil;
import lombok.ToString;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;

/**
 * fold 被遗弃，使用aggregate
 * Funciton
 */
public class FoldDemo {
    private static class MyPathSincnFunction extends RichSinkFunction<String> {
        @Override
        public void invoke(String value, Context context) {
            long watermark = context.currentWatermark();
            Long timestamp = context.timestamp();
            long processingTime = context.currentProcessingTime();
            System.out.println(String.format("watermark: %s , timestamp: %s , processingTime:%s",watermark, timestamp, processingTime));
            System.out.println(value);
        }
    }

    @ToString
    class FoldData {
        @BasicTokenInfo(min = "1", max = "1000", step = "1")
        private Integer ints;
        @BasicTokenInfo({"zhangsan", "lisi" ,"wangwu" , "zhaoliu"})
        private String name;
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        SourceFunction<FoldData> source = SourceUtil.createStreamSource(FoldData.class, 500L);
        SingleOutputStreamOperator<FoldData> startStream = env.addSource(source).returns(FoldData.class);

        SingleOutputStreamOperator<FoldData> hasWaterMarkStream =
                startStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<FoldData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, ts) -> System.currentTimeMillis())
                );
        SingleOutputStreamOperator<String> fold = startStream
                .keyBy(v -> v.name).fold("start,", new FoldFunction<FoldData, String>() {
            @Override
            public String fold(String accumulator, FoldData value) throws Exception {
                return accumulator + value ;
            }
        });
//        fold.writeAsText("data/folddemo.txt");

        fold.addSink(new MyPathSincnFunction());
//        startStream.print();
        FocusUtil.start(env, "folddemo", 10);
    }
}
