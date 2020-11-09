package flinkbase.watermarker;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
//import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * 在source端，生成数据，并发送数据
 */
public class SourceAssignerWaterMarker {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        // 默认10秒依次时间
        env.getConfig().setAutoWatermarkInterval(Time.seconds(10).toMilliseconds());
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1. 在source端 发送水印时间
        SourceFunction<Person> source = SourceUtil.
                createStreamSource(Person.class);
        // 2. 窗口太难用了
        SingleOutputStreamOperator<Person> source1 = env.addSource(source)
                .returns(Person.class);
        WatermarkGenerator waterMarketAssigner = new BoundedOutOfOrdernessGenerator2();
        source1
//                .filter(person -> {
//                    return "王五".equals(person.getName());
//                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .keyBy(person -> person.getName())
                .timeWindow(Time.days(2))
                .apply(new RichWindowFunction<Person, Object, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Person> input, Collector<Object> out) throws Exception {
                        Iterator<Person> iterator = input.iterator();
                        long start = window.getStart();
                        long end = window.getEnd();

                        System.out.println("start : " + start);
                        System.out.println("end : " + end);
                        while (iterator.hasNext()) {
                            Person next = iterator.next();
                            System.out.println("s: " + s + "    apply:" + next);
                        }
                    }
                })
        ;

        env.execute("water marker1");

    }

    static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Person>{
        private final long maxOutOfOrderness = 3500; // 最大乱序时间
        private long currentMaxTimestamp;

        /**
         * 水印是不停地触发
         * @return
         */
        @Nullable
        @Override
        public org.apache.flink.streaming.api.watermark.Watermark getCurrentWatermark() {
//            System.out.println("getCurrentWatermark:" + format.format(new java.util.Date(System.currentTimeMillis())));
            long time = currentMaxTimestamp - maxOutOfOrderness - 1;
            System.out.println(time);
            return new org.apache.flink.streaming.api.watermark.Watermark(time);
        }

        @Override
        public long extractTimestamp(Person element, long recordTimestamp) {
//            System.out.println("extractTimestamp:" + format.format(element.getBirthDay()));
//            System.out.println("currenttime:" + format.format(new java.util.Date(System.currentTimeMillis())));
            long time = element.getBirthDay().getTime();
//            currentMaxTimestamp = Math.max(currentMaxTimestamp, time);
            currentMaxTimestamp = time;
            return time;
        }
    }

    static class BoundedOutOfOrdernessGenerator2 implements WatermarkGenerator<Person>{
        private final long maxOutOfOrderness = 3500; // 最大乱序时间
        private long currentMaxTimestamp;

        @Override
        public void onEvent(Person event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("eventTime:" + eventTimestamp);
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(
                    new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }
    }

}
