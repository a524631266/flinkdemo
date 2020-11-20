package flinkbase.model;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CallCity {
    @BasicTokenInfo(value = {"上海", "广州", "杭州西湖"})
    String cityName;
}
