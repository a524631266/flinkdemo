package flinkbase.model;

import com.zhangll.flink.annotation.BasicTokenInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Timestamp;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Person{
    @BasicTokenInfo(min = "30" , max = "50")
    int age;
    @BasicTokenInfo(value = {"张三", "李四" ,"王五" , "赵六"})
    String name;
    Address address;
    @BasicTokenInfo(min = "1", max = "30")
    Timestamp birthDay;
}

