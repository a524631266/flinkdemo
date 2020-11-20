package flinkbase.windows;



import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/**
 * 1. TimestampAssigner: 生成时间戳
 * 2. KeySelector: keyby
 * 3. windowAssigner: 划分窗口
 * 4. state: 中间状态.这里中间计算是,优化可能会计算AggregateFunction,只存增量值
 * 5. trigger: window什么时候要输出.
 * 5.1 Evictor: 驱逐,过滤的意思.(option)
 * 6. windowFunction : 真正计算
 * 6.1 Evictor
 */
public class WindowEvictor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment localWebEnv = EnvUtil.getLocalWebEnv();

        SourceFunction<DriverSpeed> stateSource = SourceUtil.createStreamSource(DriverSpeed.class);

        SingleOutputStreamOperator<DriverSpeed> returns = localWebEnv.addSource(stateSource).returns(DriverSpeed.class);

        returns
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DriverSpeed>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(DriverSpeed element) {
                        return element.getCurrentime().getTime();
                    }
                })
                .keyBy(a-> a.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .trigger(CountTrigger.of(2))
                .process(new ProcessWindowFunction<DriverSpeed, Object, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<DriverSpeed> elements, Collector<Object> out) throws Exception {

                        System.out.println(integer + "con:" + context.currentWatermark() + "");
                    }
                });
        localWebEnv.execute("test");
    }
}
