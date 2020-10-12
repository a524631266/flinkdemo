package flinkbase.windows;

import flinkbase.model.Person;
import flinkbase.typeinfo.example.protocol.ProtoColType;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FixWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();

        SourceFunction<ProtoColType<Person>> source = SourceUtil.createStreamSourceWrapperProtocol(Person.class);

        DataStreamSource<ProtoColType<Person>> person = env.addSource(source);
        person.printToErr();
        person.addSink(new SinkFunction<ProtoColType<Person>>() {
            @Override
            public void invoke(ProtoColType<Person> value, Context context) throws Exception {

            }
        });
        env.execute("asd");
    }
}
