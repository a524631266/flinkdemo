package flinkbase.model;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class Entity{
    //        @BasicTokenInfo("/0000000[1-9]/")
    @BasicTokenInfo("/[1-9]/")
    String entity;
    @BasicTokenInfo("00000000")
    String parentId;
}
