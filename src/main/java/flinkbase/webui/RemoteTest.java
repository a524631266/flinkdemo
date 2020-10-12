package flinkbase.webui;


import flinkbase.model.Address;
import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.StringUtils;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RemoteTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.createDefaultRemote();
        DataStreamSource<Person> source = env.addSource(new RichSourceFunction<Person>() {
            //            public MetricGroup metricGroup = null;
            private boolean isRunning = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//                metricGroup = getRuntimeContext().getMetricGroup();
            }

            @Override
            public void run(SourceContext<Person> ctx) throws Exception {
                System.out.println(getRuntimeContext().getTaskName());
                while (isRunning) {
                    Person person = new Person();
                    person.setAge(new Random().nextInt(100));
                    Address address = new Address();
                    address.setId(new Random().nextInt(100));
                    address.setAddress(StringUtils.getRandomString(new Random(),4,10));
                    person.setAddress(address);
                    ctx.collect(person);
                    TimeUnit.MILLISECONDS.sleep(500);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        source.map(new MapFunction<Person, String>() {
            @Override
            public String map(Person value) throws Exception {
                if(value.getAddress() != null){
                    return value.getAddress().getAddress();
                }
                return null;
            }
        }).print();
        env.execute("My person map task!");

    }
}
