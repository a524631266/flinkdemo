package flinkbase.source;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        SourceFunction<Person> source = SourceUtil.createStreamSource(Person.class);
        SingleOutputStreamOperator<Person> source1 = env.addSource(source).returns(Person.class);

        source1.print();
        env.execute("person job");

    }
}
