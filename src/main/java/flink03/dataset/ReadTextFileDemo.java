package flink03.dataset;

import flinkbase.utils.EnvUtil;
import flink03.dataset.model.Employee;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.util.StringUtils;

import java.text.SimpleDateFormat;

public class ReadTextFileDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = EnvUtil.getLoalWebDataSetEnv();
        DataSource<String> stringDataSource = env.readTextFile("data/emp.txt");
        MapOperator<String, Employee> map = mapper(stringDataSource);
        map.filter(employee -> employee!=null)
                .print();
    }

    /**
     * 简单映射为数据
     * @param stringDataSource
     * @return
     */
    public static MapOperator<String, Employee> mapper(DataSource<String> stringDataSource) {
        return stringDataSource.map(new MapFunction<String, Employee>() {
                @Override
                public Employee map(String value) throws Exception {
                    String[] s = value.split("\t");
                    if ((s.length == 8)) {
                        /**
                         * 7369	SMITH	CLERK	7902	1980-12-17	800.00		20
                         */
                        Employee employee = new Employee();
                        employee.setId(Integer.valueOf(s[0]));
                        employee.setName(s[1]);
                        employee.setRole(s[2]);
                        employee.setSalary(StringUtils.isNullOrWhitespaceOnly(s[3])?0:Long.valueOf(s[3]));
                        employee.setBirthday(new SimpleDateFormat("yyyy-mm-dd").parse(s[4]));
                        employee.setTax(Double.valueOf(s[5]));
                        employee.setTax1(StringUtils.isNullOrWhitespaceOnly(s[6])?0:Double.valueOf(s[6]));
                        employee.setAge(Integer.valueOf(s[7]));
                        return employee;
                    }
                    return null;
                }
            });
    }
}
