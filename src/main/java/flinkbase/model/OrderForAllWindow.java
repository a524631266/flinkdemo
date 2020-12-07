package flinkbase.model;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import lombok.ToString;

import java.sql.Timestamp;

/**
 * 给allWindow使用的model
 */
@ToString
public class OrderForAllWindow {
    @BasicTokenInfo(value = {"衣服", "鞋子", "袜子", "电子产品"})
    private String categroy;

    @BasicTokenInfo(min = "100", max = "1000")
    private double money;

    @BasicTokenInfo(min = "0", max = "100", step = "1")
    private Integer userId;

    @BasicTokenInfo(min = "2018-01-02 00:00:00", max = "2019-01-02 00:00:00", step = "86400")
    private Timestamp createdTime;

    private OrderState orderState;
}
