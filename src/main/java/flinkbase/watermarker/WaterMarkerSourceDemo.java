package flinkbase.watermarker;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * 在理解水印之前需要： 触发window计算的依据与 划分窗口区间的依据
 *
 * 一、理解水印生成的方式
 * 周期性的获取数据， 水印是为window 做准备的。即触发window计算的依据。
 * BoundedOutOfOrdernessTimestampExtractor 为周期性地设置水印
 *
 * WaterMark 与Timestamp是不一样的概念，Timestamp是作为数据传递,在其下游算子中会存在，是划分窗口的依据，
 * 而水印是一个标签， 可以通过getCurrentWaterMark可以获取生成的水印，是有条件的timestamp，一般默认是最大的timestamp，用来触发水印计算
 * 1. 那么如果不分配水印或时间戳(env设置EventTime/Process的时候，)，那么在keyProcess中 ctx.timestamp()为 null
 * 2. 那么如果不分配水印或时间戳(env设置的时候，IngestionTime)，那么在keyProcess中 ctx.timestamp()为 当前处理时间
 * 3. 不管env设置的是EventTime/Process/ingestionTime，只要在source源（最好在分配时间的时候越接近source operator越好）有分配，在keyProcess中 ctx.timestamp()为分配的
 *
 *
 *
 * 二、
 */
public class WaterMarkerSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        // 这里，如果使用的是EventTime（或者source中自动分配了时间策略），失效，如果是processing Time是会
        env.getConfig().setAutoWatermarkInterval(Time.seconds(3).toMilliseconds());
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        SourceFunction<Person> source = SourceUtil.createStreamSource(Person.class);
        SingleOutputStreamOperator<Person> source1 = env.addSource(source)
                .returns(Person.class);

        source1
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Person>(Time.seconds(3)) {
//                    @Override
//                    public long extractTimestamp(Person element) {
//                        System.out.println("111111111");
//                        System.out.println(getCurrentWatermark().getTimestamp());
//                        return element.getBirthDay().getTime();
//                    }
//                })
                .process(new ProcessFunction<Person, Person>() {
                    @Override
                    public void processElement(Person value, Context ctx, Collector<Person> out) throws Exception {
                        System.out.println("source:  "+ ctx.timestamp());
                        out.collect(value);
                    }
                })
                .keyBy(person-> person.getName())
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .allowedLateness(Time.seconds(2L))
//                .apply(new WindowFunction<Person, Object, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<Person> input, Collector<Object> out) throws Exception {
//                        System.out.println(s);
//                        // 获取数据中的实际按
//                        Iterator<Person> iterator = input.iterator();
//                        while (iterator.hasNext()) {
//                            Person next = iterator.next();
//                            System.out.println(next);
//                        }
//                    }
//                })
                .process(new KeyedProcessFunction<String, Person, Object>() {
                    @Override
                    public void processElement(Person value, Context ctx, Collector<Object> out) throws Exception {
                        System.out.println(value);
                        // 获取数据中的实际按
                        Long timestamp = ctx.timestamp();
                        String currentKey = ctx.getCurrentKey();
                        TimerService timerService = ctx.timerService();
                        System.out.println("currentKey:"+ currentKey + "; ts:" + new Timestamp(timestamp));
                    }
                })
        ;

        env.execute("water marker1");

    }
}
