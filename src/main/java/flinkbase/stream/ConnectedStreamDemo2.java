package flinkbase.stream;

import flinkbase.utils.EnvUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

public class ConnectedStreamDemo2 {
    public static void main(String[] args) {
//        StreamExecutionEnvironment localWebEnv = EnvUtil.getLocalWebEnv();
        StreamExecutionEnvironment localWebEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String , Integer>> source1 =
                localWebEnv.fromElements(
                        new Tuple2("a", 3),
                        new Tuple2("d", 4),
                        new Tuple2("c", 2),
                        new Tuple2("c", 5),
                        new Tuple2("a", 5));

        DataStreamSource<Tuple3<String , Integer, String>> source2 =
                localWebEnv.fromElements(
                new Tuple3("a", 3, "c"),
                new Tuple3("a", 3, "c"),
                new Tuple3("a", 3, "c"),
                new Tuple3("a", 3, "c"),
                new Tuple3("a", 3, "c"));
        ConnectedStreams<Tuple2<String, Integer>, Tuple3<String, Integer, String>> connect = source1.connect(source2);


        // 拿到第一个个流中的第二个字段和第二个流的第一个字段作为分区
        ConnectedStreams<Tuple2<String, Integer>, Tuple3<String, Integer, String>> source = connect.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }, new KeySelector<Tuple3<String, Integer, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, String> value) throws Exception {
                        return value.f0;
                    }
                });
        
        
//        source.process(new CoProcessFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>, Object>() {
//        })

    }
}
