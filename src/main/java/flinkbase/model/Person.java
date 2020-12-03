package flinkbase.model;


import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Date;
import java.sql.Timestamp;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    @BasicTokenInfo(min = "1", max = "1000000000", step = "1")
    int id;

    @BasicTokenInfo(min = "0", max = "100", step = "1")
    int age;
    //    @BasicTokenInfo(value = {"zhangsan", "lisi" ,"wangwu" , "zhaoliu"} ,step = "3")
//    String name;
    @BasicTokenInfo(value = {"zhangsan", "lisi"})
    String name;

    Address address;
    @BasicTokenInfo(min = "2019-01-01", max = "2019-12-01", step = "1")
    Date birthDay;
}

