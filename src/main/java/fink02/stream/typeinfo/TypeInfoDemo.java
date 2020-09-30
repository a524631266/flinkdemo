package fink02.stream.typeinfo;

import fink02.model.Person;
import fink02.model.SubPerson;
import fink02.utils.EnvUtil;
import fink02.utils.SourceUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TypeInfoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        env.setParallelism(1);
        SourceFunction<Person> sourceFunction = SourceUtil.createStreamSource(Person.class);
        SingleOutputStreamOperator<Person> sourceStream = env.addSource(sourceFunction).returns(Person.class);
        sourceStream.print();
//        .returns(Person.class)
//        subPerson
        SingleOutputStreamOperator<String> map = sourceStream.map(new MapFunction<Person, String>() {
            @Override
            public String map(Person person) throws Exception {
                return person.getAddress().getAddress();
            }
        });
        sourceStream.filter()
        sourceStream.split()
        sourceStream.addSink()
        sourceStream.flatMap()
        sourceStream.iterate()
        sourceStream.keyBy()
        sourceStream.connect()
        sourceStream.union()
        sourceStream.rebalance();
        sourceStream.coGroup()
        sourceStream.forward()
        sourceStream.project()
        sourceStream.windowAll()
        sourceStream.timeWindowAll()
        sourceStream.process()
        sourceStream.broadcast()
        sourceStream.join()
        sourceStream.global()
        sourceStream.rescale()
        sourceStream.shuffle()
//                .returns(String.class);
        map.print();

        env.execute("asdf");
    }
}
