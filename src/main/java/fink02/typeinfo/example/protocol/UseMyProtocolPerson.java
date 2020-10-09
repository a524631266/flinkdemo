package fink02.typeinfo.example.protocol;

import fink02.model.Person;
import fink02.utils.EnvUtil;
import fink02.utils.SourceUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class UseMyProtocolPerson {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();

//        StreamExecutionEnvironment env = EnvUtil.createDefaultRemote();
        ExecutionConfig config = env.getConfig();
        boolean forceAvroEnabled = config.isForceAvroEnabled();
        boolean forceKryoEnabled = config.isForceKryoEnabled();
        System.out.println(String.format("forceAvroEnabled: %b ;forceKryoEnabled : %b", forceAvroEnabled, forceKryoEnabled));

        SourceFunction<ProtoColType<Person>> sourceStream = SourceUtil.createStreamSourceWrapperProtocol(Person.class);
        DataStreamSource<ProtoColType<Person>> source = env.addSource(sourceStream);
        SingleOutputStreamOperator<Integer> map = source.map(new MapFunction<ProtoColType<Person>, Integer>() {
            @Override
            public Integer map(ProtoColType<Person> value) throws Exception {
                return value.rawObject.getAge();
//                return null;
            }
        });
        map.print();
        source.printToErr();
        env.execute("start");
    }
}
