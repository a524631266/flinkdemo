package fink02.iteration;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 这个规则是在图中，我们保证当值为5时候，可以不断重复上次的循环
 */
public class NoFiterNoMapDemo {
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
                    TimeUnit.SECONDS.sleep(2L);
                }
            }

            @Override
            public void cancel() {
                start = false;
            }
        });
        IterativeStream<Long> iterate = infiniteSource.iterate(10);
//        iterate.process()

        env.execute("just_for_test");

    }
}
