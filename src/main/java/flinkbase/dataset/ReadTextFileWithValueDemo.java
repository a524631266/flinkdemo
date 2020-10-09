package flinkbase.dataset;

import flinkbase.dataset.model.Employee;
import flinkbase.utils.EnvUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.types.StringValue;

/**
 * StringValue可以比较高效地读取数据
 */
public class ReadTextFileWithValueDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = EnvUtil.getLoalWebDataSetEnv();
        DataSource<StringValue> stringValueDataSource = env.readTextFileWithValue("data/emp.txt");
        MapOperator<StringValue, Employee> map = stringValueDataSource.map(new MapFunction<StringValue, Employee>() {
            @Override
            public Employee map(StringValue value) throws Exception {
                int length = value.length();
                System.out.println(length);
                return null;
            }
        });

        map.filter(e -> e!=null).print();
    }
}
