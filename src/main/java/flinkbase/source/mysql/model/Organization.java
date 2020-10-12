package flinkbase.source.mysql.model;

import lombok.Data;

@Data
public class Organization {
    int id;
    String name;
    String coding;
    String memo;
    Long importtime;
    int importuserid;
    Long updatetime;
    int updateuserid;
    Long deletetime;
    int deleteuserid;
    byte fast = 0;
    int parentid = 0;
    String description;
    int isfrozen = 0;
    int salt = 0;
}
