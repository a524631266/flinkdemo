package flinkbase.watermarker;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 在source端，生成数据，并发送数据
 */
public class SourceAssignerWaterMarker {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        env.setParallelism(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1. 在source端 发送水印时间
        SourceFunction<Person> source = SourceUtil.
                createStreamSource(Person.class);

        SingleOutputStreamOperator<Person> source1 = env.addSource(source)
                .returns(Person.class);

        source1
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Person>() {
                    @Override
                    public WatermarkGenerator<Person> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return null;
                    }
                })
                .process(new ProcessFunction<Person, Person>() {
                    @Override
                    public void processElement(Person value, Context ctx, Collector<Person> out) throws Exception {
                        System.out.println("source:  "+ ctx.timestamp());
                        out.collect(value);
                    }
                })
                .keyBy(person-> person.getName())
                .process(new KeyedProcessFunction<String, Person, Object>() {
                    @Override
                    public void processElement(Person value, Context ctx, Collector<Object> out) throws Exception {
                        System.out.println("实际处理时间: " + value);
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
