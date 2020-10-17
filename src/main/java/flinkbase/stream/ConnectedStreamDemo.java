package flinkbase.stream;

import flinkbase.utils.EnvUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;


import java.util.concurrent.TimeUnit;

/**
 * connected stream 是把不同类型的流，转换为相同类型的流，所以维表不是这么搞的。
 */
public class ConnectedStreamDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = EnvUtil.setCheckpoint(EnvUtil.getLocalWebEnv());
////        env.addSource(new SourceFunction<>() {
////        })
//
        StreamExecutionEnvironment localWebEnv = EnvUtil.getLocalWebEnv();
        DataStreamSource<Tuple2<String , Integer>> source1 =
                localWebEnv.fromElements(
                        new Tuple2("a", 3),
                        new Tuple2("d", 4),
                        new Tuple2("c", 2),
                        new Tuple2("c", 5),
                        new Tuple2("a", 5));
        DataStreamSource<Integer> source2 = localWebEnv.fromElements(1, 2, 4, 5, 6);
        ConnectedStreams<Tuple2<String , Integer>, Integer> connect = source1.connect(source2);
//        mapMethod(connect);
        flatmapMethod(connect);

        localWebEnv.execute();


    }

    private static void flatmapMethod(ConnectedStreams<Tuple2<String, Integer>, Integer> connect) {
        connect.flatMap(new CoFlatMapFunction<Tuple2<String, Integer>, Integer, Tuple3<String, Integer, Integer>>() {

            private Integer number = 0;

            @Override
            public void flatMap1(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                out.collect(new Tuple3<String, Integer, Integer>(value.f0 , value.f1, number));
            }

            @Override
            public void flatMap2(Integer value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                //   不能保证数据的完整性
                TimeUnit.SECONDS.sleep(1);
                number = value;
            }
        }).printToErr();
    }

    /**
     * Comap union操作
     * @param connect
     */
    private static void mapMethod(ConnectedStreams<Tuple2<String, Integer>, Integer> connect) {
        SingleOutputStreamOperator<Tuple2<Integer, String>> map = connect.map(new CoMapFunction<Tuple2<String, Integer>, Integer, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map1(Tuple2<String, Integer> value) throws Exception {
                return new Tuple2<>(value.f1, value.f0);
            }

            @Override
            public Tuple2<Integer, String> map2(Integer value) throws Exception {
                return new Tuple2<>(value, "default");
            }
        });
        map.printToErr();
    }
}
