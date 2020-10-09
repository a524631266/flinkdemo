package flink03.dataset;

import flinkbase.utils.EnvUtil;
import flink03.dataset.model.Employee;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;

import static flink03.dataset.ReadTextFileDemo.mapper;

/**
 * CSV如何保证数据写入
 * 要转换为Tuple类型
 */
public class WriteCSVDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = EnvUtil.getLoalWebDataSetEnv();
        env.getConfig().enableForceKryo();
        DataSource<String> stringDataSource = env.readTextFile("data/emp.txt");
        MapOperator<String, Employee> map = mapper(stringDataSource);
        map.filter(employee -> employee != null)
//                .map(new MapFunction<Employee, Tuple8>() {
//
//                    @Override
//                    public Tuple8 map(Employee value) throws Exception {
//                        Tuple8 tuple = new Tuple8<>();
//                        tuple.setField(value.getId(), 0);
//                        tuple.setField(value.getName(), 1);
//                        tuple.setField(value.getRole(), 2);
//                        Date birthday = value.getBirthday();
//                        java.sql.Date date = new java.sql.Date(birthday.getYear(), birthday.getMonth(), birthday.getDay());
//                        tuple.setField(date, 3);
//                        tuple.setField(value.getSalary(), 4);
//                        tuple.setField(value.getTax(), 5);
//                        tuple.setField(value.getTax1(), 6);
//                        tuple.setField(value.getAge(), 7);
//                        return tuple;
//                    }
//                })
//                // Caused by: org.apache.flink.api.common.functions.InvalidTypesException: Tuple needs to be parameterized by using generics.
//                // java.util.Date cannot be cast to java.sql.Date
//                .returns(Types.TUPLE(
//                        Types.INT,
//                        Types.STRING,
//                        Types.STRING,
//                        Types.SQL_DATE,
//                        Types.LONG,
//                        Types.DOUBLE,
//                        Types.DOUBLE,
//                        Types.INT))
////                .returns(new TypeHint<Tuple8>() {
////
////                })
////                .print();
                .returns(TypeInformation.of(Employee.class))
                .print()
//                .writeAsCsv("data/employee.csv","\n","," , FileSystem.WriteMode.OVERWRITE)
                ;
//            env.execute();
    }
}
