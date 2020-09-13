package fink02;

import fink02.utils.SourceGenerator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.collection.parallel.Splitter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SplitDemoBetterThanfiler {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource = env.fromCollection(SourceGenerator.generate3tupleList());
        // 1. running  filter method
//        runByFilter(streamSource);
        // running by split
        // 2. method using split method
        runningBySplit(streamSource);

        env.execute();

    }

    private static void runningBySplit(DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource) {
        SplitStream<Tuple3<Integer, Integer, Integer>> split = streamSource.split(new OutputSelector());
        split.select("zeroStream").print();

        split.select("oneStream").printToErr();
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
