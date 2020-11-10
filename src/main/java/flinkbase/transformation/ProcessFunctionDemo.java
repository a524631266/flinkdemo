package flinkbase.transformation;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();

        SourceFunction<Person> stateSource = SourceUtil.createStateSource(Person.class);

        SingleOutputStreamOperator<Person> returns = env.addSource(stateSource).returns(Person.class);

        KeyedStream<Person, String> personStringKeyedStream = returns.keyBy(p -> p.getName());

        SingleOutputStreamOperator<Object> process = personStringKeyedStream.process(new MyProcessFunction());
        process.printToErr();

        env.execute("asdf");
    }

    private static class MyProcessFunction extends KeyedProcessFunction<String, Person, Object> {

        @Override
        public void processElement(Person value, Context ctx, Collector<Object> out) throws Exception {

        }
    }
}
