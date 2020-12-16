package flinkbase.state;

import com.google.common.collect.Iterables;
import flinkbase.model.Person;
import flinkbase.utils.FocusUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.StringUtils;

import java.util.stream.Stream;

/**
 * 1. getRuntimeContext().getMapState(stateDesc); 是不能跨算子的。
 * 2. getRuntimeContext().getMapState(stateDesc) 只能作用于key stream中
 * 3. getRuntimeContext().getMapState(stateDesc)， 即使并行度为1，也是根据不同的key，存储不同的数据
 *
 * @see org.apache.flink.streaming.api.operators.StreamingRuntimeContext#getMapState(org.apache.flink.api.common.state.MapStateDescriptor)
 * 内部有一个keyedStateStore， 这是通过runtimeContext获取数据上下文的时候必须指定为key的原因
 * 否则在内部会创建一个空的 keyedStateStore，这个keyedStateStore是只有keyStateStore才有的
 */
public class FlinkRunTime {
    public static void main(String[] args) {
        SingleOutputStreamOperator<Person> stream = FocusUtil.
                generateEnableSourceStreamWithCheckpoint(Person.class, 1000);


        MapStateDescriptor<String, String> stateDesc = new MapStateDescriptor<String, String>("test",
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<String>() {})
        );

//        ValueStateDescriptor<?> desc = new ValueStateDescriptor<Object>();
        SingleOutputStreamOperator<String> map = stream.keyBy(
                value -> value.getName()).map(new RichMapFunction<Person, String>() {
            MapState<String, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//                getRuntimeContext().getState(desc);
                mapState = getRuntimeContext().getMapState(stateDesc);
            }

            @Override
            public String map(Person value) throws Exception {
                System.out.println(value);
                // 每个并发实例的runtime是不一样的！！！
                mapState.put(value.getAddress().getAddress(),value.getName() );
                System.out.println("TaskName:" + getRuntimeContext().getTaskName());

                System.out.println(Iterables.size(getRuntimeContext().getMapState(stateDesc).values()));
                System.out.println(Iterables.size(mapState.values()));
                return value.getName();
            }
        }).setParallelism(10);

        map.keyBy(value -> value).filter(new RichFilterFunction<String>() {
            MapState<String, String> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                mapState = getRuntimeContext().getMapState(stateDesc);
            }

            @Override
            public boolean filter(String value) throws Exception {
                MapState<String, String> mapState = getRuntimeContext().getMapState(stateDesc);
                System.out.println("##########");
                System.out.println(mapState);
                System.out.println("is Empty?:" + mapState.isEmpty());
                return !StringUtils.isNullOrWhitespaceOnly(value);
            }
        }).printToErr();

        FocusUtil.execute("123");
    }
}
