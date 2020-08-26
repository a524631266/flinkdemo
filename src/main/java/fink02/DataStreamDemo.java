package fink02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataStreamDemo {
    public static void main(String[] args) throws Exception {
        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = e.addSource(
                new FromElementsFunction<>(Types.INT.createSerializer(e.getConfig()), data), Types.INT);

        SingleOutputStreamOperator<Integer> sum = source.map(v -> v * 2).keyBy(value -> 1).sum(0);

        sum.addSink(new PrintSinkFunction<>());

        e.execute();


    }
}
