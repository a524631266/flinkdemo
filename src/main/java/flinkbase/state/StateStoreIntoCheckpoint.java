package flinkbase.state;

import com.google.common.collect.Iterables;
import flinkbase.model.Person;
import flinkbase.utils.FocusUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.StringUtils;

/**
 * 1. getRuntimeContext().getMapState(stateDesc); 是不能跨算子的。
 * 2. getRuntimeContext().getMapState(stateDesc) 只能作用于key stream中
 * 3. getRuntimeContext().getMapState(stateDesc)， 即使并行度为1，也是根据不同的key，存储不同的数据
 *
 * @see org.apache.flink.streaming.api.operators.StreamingRuntimeContext#getMapState(MapStateDescriptor)
 * 内部有一个keyedStateStore， 这是通过runtimeContext获取数据上下文的时候必须指定为key的原因
 * 否则在内部会创建一个空的 keyedStateStore，这个keyedStateStore是只有keyStateStore才有的
 *
 *
 * 消除疑惑，operate/key都是 基于task（相当于一个slot运行的function实例）
 *
 * 2个并行度1.
 */
public class StateStoreIntoCheckpoint {
    static MapStateDescriptor<String, String> stateDesc = new MapStateDescriptor<String, String>("test",
            TypeInformation.of(new TypeHint<String>() {}),
            TypeInformation.of(new TypeHint<String>() {})
    );
    public static void main(String[] args) {
        SingleOutputStreamOperator<Person> stream = FocusUtil.
                generateEnableSourceStreamWithCheckpoint(Person.class, 1000);

//        ValueStateDescriptor<?> desc = new ValueStateDescriptor<Object>();
        SingleOutputStreamOperator<String> map = stream.keyBy(
                value -> value.getName()).map(new MyRichMapFunction()).setParallelism(4);
        FocusUtil.execute("123");
    }

    private static class MyRichMapFunction extends RichMapFunction<Person, String> implements CheckpointedFunction {
        MapState<String, String> mapState;
        private ListState<Long> listState;

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
            System.out.println(getRuntimeContext());
            System.out.println(Iterables.size(getRuntimeContext().getMapState(stateDesc).values()));
            System.out.println(Iterables.size(mapState.values()));
            listState.add((long) value.getAge());
            System.out.println("####################");
            // #########并行task1###########
            //1111111:2
            //1111111:5
            // ########并行task2############
            //1111111:0
            //1111111:1
            //1111111:3
            //1111111:4
            //1111111:6
            for (Long aLong : listState.get()) {
                System.out.println("1111111:" + aLong);
            }
            System.out.println("####################");
            return value.getName();
        }


        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println(getRuntimeContext().getIndexOfThisSubtask());
            System.out.println("CheckpointId:"+context.getCheckpointId());
            System.out.println("snapshot1111");

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");
            ListStateDescriptor<Long> stateDescripter = new ListStateDescriptor<Long>("offset-state", Types.LONG);
            listState = context.getOperatorStateStore().getListState(stateDescripter);

        }
    }
}
