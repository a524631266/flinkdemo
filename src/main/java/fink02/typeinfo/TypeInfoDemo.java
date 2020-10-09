package fink02.typeinfo;

import fink02.model.Person;
import fink02.model.SubPerson;
import fink02.utils.EnvUtil;
import fink02.utils.SourceUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.Utils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 两条流都能够打通为什么？
 * 这个是什么类型的？
 */
public class TypeInfoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        env.setParallelism(1);
        SourceFunction<Person> sourceFunction = SourceUtil.createStreamSource(Person.class);
        SingleOutputStreamOperator<Person> sourceStream = env.addSource(sourceFunction).returns(Person.class);
        // 第一条流
        sourceStream.print();
//        .returns(Person.class)
//        subPerson
        // 第二条流
        SingleOutputStreamOperator<String> map22 = sourceStream.map(new MapFunction<Person, String>() {
            @Override
            public String map(Person person) throws Exception {
                String callLocationName = Utils.getCallLocationName();
                System.out.println(callLocationName + ":" + person);
                return person.getAddress().getAddress();
            }
        });
//        sourceStream.filter()
//        sourceStream.split()
//        sourceStream.addSink()
//        sourceStream.flatMap()
//        sourceStream.iterate()
//        sourceStream.keyBy()
//        sourceStream.connect()
//        sourceStream.union()
//        sourceStream.rebalance();
//        sourceStream.coGroup()
//        sourceStream.forward()
//        sourceStream.project()
//        sourceStream.windowAll()
//        sourceStream.timeWindowAll()
//        sourceStream.process()
//        sourceStream.broadcast()
//        sourceStream.join()
//        sourceStream.global()
//        sourceStream.rescale()
//        sourceStream.shuffle()
//                .returns(String.class);
        map22.printToErr();

        env.execute("asdf");
    }
}
