package flinkbase.model;

import com.zhangll.flink.annotation.FieldTokenType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Person{
    @FieldTokenType(min = "30" , max = "50")
    int age;
    @FieldTokenType(value = {"张三", "李四" ,"王五" , "赵六"})
    String name;
    Address address;
}

