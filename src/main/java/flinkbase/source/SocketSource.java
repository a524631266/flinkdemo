package flinkbase.source;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 测试使用的环境
 * nc -lk port
 * source源{"age": 12, "name": "zhangll", "address":{"id":12,"address":"乘火车"},"birthDay": "2020-10-12"}
 *
 */
public class SocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        // host name port 分隔符 和 最大重试次数
        // hostname为目的地，
        DataStreamSource<String> source = env.socketTextStream("192.168.1.205", 18888);

        SingleOutputStreamOperator<Person> results = source.map(new RichMapFunction<String, Person>() {
            private ObjectMapper objectMapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                objectMapper = new ObjectMapper();
            }

            @Override
            public Person map(String value) throws Exception {
                Person person = objectMapper.readValue(value.getBytes(), Person.class);
                return person;
            }
        });

        results.print();

        env.execute("Test for socket");
    }
}
