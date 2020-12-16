package flinkbase.transformation.window;

import flinkbase.model.OrderForAllWindow;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * window 窗口算子分量两种1. window算子，是基于key
 * @see https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/stream/operators/windows.html
 *
 * @see https://www.jianshu.com/p/4d8e3ec9b624
 * 窗口本质上是一个
 *
 */
public class SimpleAllWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        SourceFunction<OrderForAllWindow> createdTime = SourceUtil.createSourceStreamWithWatherMark(
                                                             OrderForAllWindow.class,
                                        "createdTime",
                                                             Time.seconds(1));

        SingleOutputStreamOperator<OrderForAllWindow> sources = env.addSource(createdTime).returns(OrderForAllWindow.class);

        WindowedStream<OrderForAllWindow, String, TimeWindow> evictor = env.addSource(createdTime).keyBy(new KeySelector<OrderForAllWindow, String>() {
            @Override
            public String getKey(OrderForAllWindow value) throws Exception {
                return value.getUserId().toString();
            }
        }).window(EventTimeSessionWindows.withGap(Time.hours(2)))
                .trigger(CountTrigger.of(1))
                .evictor(TimeEvictor.of(Time.hours(2)));

        sources.print();

        env.execute("about all windows");
    }
}
