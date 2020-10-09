package flinkbase;

import flinkbase.utils.SourceGenerator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class SplitDemoBetterThanfiler {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource = env.fromCollection(SourceGenerator.generate3tupleList(30));
        // 1. running  filter method
//        runByFilter(streamSource);
        // running by split
        // 2. method using split method
//        runningBySplit(streamSource);
        // 3. next using split
        runBySideOutPut(streamSource);

        env.execute();

    }

    private static void runBySideOutPut(DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource) {
        // a 定义tag
        // B 定义特定函数进行数据拆分
        /**
         * @see ProcessFunction
         * @see org.apache.flink.streaming.api.functions.KeyedProcessFunction
         * @see org.apache.flink.streaming.api.functions.co.CoProcessFunction
         * @see org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
         * @see org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
         * @see org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
         */
        OutputTag<Tuple3<Integer, Integer, Integer>> zeroTag = new OutputTag<Tuple3<Integer, Integer, Integer>>("zeroStream"){};
        OutputTag<Tuple3<Integer, Integer, Integer>> oneTag = new OutputTag<Tuple3<Integer, Integer, Integer>>("oneStream"){};

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> streamOperator = streamSource.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                if (value.f0 % 2 == 0) {
                    ctx.output(zeroTag, value);
                } else {
                    ctx.output(oneTag, value);
                }
            }
        });

//        streamOperator.getSideOutput(zeroTag).print();
//        streamOperator.getSideOutput(oneTag).printToErr();

        SplitStream<Tuple3<Integer, Integer, Integer>> split = streamOperator.getSideOutput(zeroTag).split(getOutputSelector());
        split.select("less2").printToErr();

        split.select("large2").print();

    }

    private static void runningBySplit(DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource) {
        SplitStream<Tuple3<Integer, Integer, Integer>> split = streamSource.split(new OutputSelector());
        split.select("zeroStream").print();

        split.select("oneStream").printToErr();



        //Consecutive multiple splits are not supported. Splits are deprecated. Please use side-outputs.
        SplitStream<Tuple3<Integer, Integer, Integer>> splitStream2 = split.select("zeroStream").split(getOutputSelector());
        splitStream2.select("less2").printToErr();

        splitStream2.select("large2").print();
    }

    private static org.apache.flink.streaming.api.collector.selector.OutputSelector<Tuple3<Integer, Integer, Integer>> getOutputSelector() {
        return new org.apache.flink.streaming.api.collector.selector.OutputSelector<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Iterable<String> select(Tuple3<Integer, Integer, Integer> value) {
                List list = new ArrayList<String>();
                // 设置第二列的魔术小于2
                if (value.f1.intValue() % 4 <= 2) {

                    list.add("less2");
                } else {
                    list.add("large2");
                }
                return list;
            }
        };
    }

    /**
     * running programming using the method filter operator,so this method should call twice,it's not efficiency.
     *
     * @param streamSource
     */
    private static void runByFilter(DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource) {
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> oddStream = streamSource.filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
                return value.f0 % 2 == 0;
            }
        });

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> evenStream = streamSource.filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
                return value.f0 % 2 != 0;
            }
        });

        oddStream.printToErr();
        evenStream.print();
    }


    private static class OutputSelector implements org.apache.flink.streaming.api.collector.selector.OutputSelector<Tuple3<Integer, Integer, Integer>> {
        @Override
        public Iterable<String> select(Tuple3<Integer, Integer, Integer> value) {

            List<String> result = new ArrayList<String>();

            if (value.f0.intValue() % 2 == 0) {
                result.add("zeroStream");
            }else {
                result.add("oneStream");
            }

            return result;

        }
    }
}
