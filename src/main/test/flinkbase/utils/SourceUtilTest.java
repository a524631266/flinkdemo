package flinkbase.utils;

import flinkbase.model.Address;
import flinkbase.model.Person;
import flinkbase.model.SubPerson;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyInt;


public class SourceUtilTest {

    @org.junit.Test
    public void createMockStreamSource() throws Exception {

        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        ExecutionConfig config = env.getConfig();
//        config.registerPojoType(Mockito.mock(Person.class).getClass());
        SourceFunction<Person> person = SourceUtil.createStreamSource(Person.class);

        DataStreamSource<Person> data = env.addSource(person);
        SingleOutputStreamOperator<Person> returns = data.returns(Person.class);
        SingleOutputStreamOperator<Integer> map = returns.map(new MapFunction<Person, Integer>() {

            @Override
            public Integer map(Person value) throws Exception {
                return value.getAge();
            }
        });
//        returns.map(new MapFunction<Person, Address>() {
//            @Override
//            public Address map(Person value) throws Exception {
//                return value.getAddress();
//            }
//        }).print();
        map.printToErr();

        env.execute("josr");

    }

    @org.junit.Test
    public void createStreamSubTypeSource() throws Exception {

        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        ExecutionConfig config = env.getConfig();
//        config.registerPojoType(Mockito.mock(Person.class).getClass());
        SourceFunction<SubPerson> person = SourceUtil.createStreamSubTypeSource(SubPerson.class);

        DataStreamSource<SubPerson> data = env.addSource(person);
        SingleOutputStreamOperator<SubPerson> returns = data.returns(SubPerson.class);
        SingleOutputStreamOperator<Address> map = returns.map(new MapFunction<SubPerson, Address>() {

            @Override
            public Address map(SubPerson value) throws Exception {
                return value.getAddress();
            }
        });

        map.printToErr();

        env.execute("josr");

    }


    @org.junit.Test
    public void createStateSource() throws Exception {
        while (true){

            Person mock = Mockito.mock(Person.class);
//            Person mock = new Person();
            Address address = new Address();
            Mockito.when(mock.getAddress()).thenReturn(address);
            Integer age = anyInt();
            Mockito.when(mock.getAge()).thenReturn(age);
            System.out.println(mock.toString());
            TimeUnit.SECONDS.sleep(1);
        }


    }
}