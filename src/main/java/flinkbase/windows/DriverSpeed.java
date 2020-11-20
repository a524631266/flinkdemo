package flinkbase.windows;


import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import lombok.Getter;

import java.sql.Timestamp;
@Getter
public class DriverSpeed{
    @BasicTokenInfo(value = {"0", "1","2", "3" , "4"}, step = "1")
    private Timestamp currentime;
    @BasicTokenInfo(min = "1", max = "10")
    private int id;
    @BasicTokenInfo(min = "80", max = "120")
    private int currentSpeed;
    @BasicTokenInfo(min = "80", max = "120")
    private int distance;
}