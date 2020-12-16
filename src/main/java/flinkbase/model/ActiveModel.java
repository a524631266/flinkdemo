package flinkbase.model;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import lombok.Data;
import lombok.ToString;

import java.sql.Timestamp;
@Data
@ToString
public class ActiveModel {
    @BasicTokenInfo(value = {"/u00[0-9]/"})
    private String userId;
    @BasicTokenInfo(value = {"/A[0-9]/"})
    private String activeId;
    @BasicTokenInfo(min = "2019-09-02 10:00:00", max = "2019-09-02 20:00:00", step = "60")
    private Timestamp activeTime;
    private EventEnum eventEnum;
    @BasicTokenInfo(value = {"北京", "上海"})
    private String province;
}
