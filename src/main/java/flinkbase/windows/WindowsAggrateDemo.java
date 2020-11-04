package flinkbase.windows;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class WindowsAggrateDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SourceFunction<Person> source = SourceUtil.createStreamSource(Person.class);
        SingleOutputStreamOperator<Person> source1 = env.addSource(source)
                .returns(Person.class);
//        source1.ma

    }
}
