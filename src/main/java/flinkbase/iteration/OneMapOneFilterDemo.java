package flinkbase.iteration;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class OneMapOneFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Long> sequenceSource = env.generateSequence(0, 2);

        DataStreamSource<Long> infiniteSource = env.addSource(new SourceFunction<Long>() {
            private boolean start = true;

            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                Long count = 11L;
                while (start) {
                    sourceContext.collect(count);
                    count-=2;
                    TimeUnit.SECONDS.sleep(5);
                }
            }

            @Override
            public void cancel() {
                start = false;
            }
        });
        IterativeStream<Long> iterate = infiniteSource.iterate();
        SingleOutputStreamOperator<Long> map = iterate.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
//                System.out.println("map "+ aLong);
                return aLong - 2;
            }
        });

        SingleOutputStreamOperator<Long> filter = map.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
//                System.out.println("filter "+ aLong);
                return aLong > 5;
            }
        });
        iterate.closeWith(filter);
        // 三种方法的区别
//        map.printToErr();
//        filter.printToErr();
        iterate.printToErr();
//        sequenceSource.printToErr();

        env.execute("just_for_test");
        sequenceSource.union(infiniteSource);
    }
}
