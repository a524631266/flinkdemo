package flink03.dataset.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.typeinfo.TypeInfo;

import java.util.Date;

/**
 * 7369	SMITH	CLERK	7902	1980-12-17	800.00		20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@TypeInfo(CustomTypeInfFactory.class)
public class Employee {
    int id;
    String name;
    String role;
    Long salary;
    Date birthday;
    double tax;
    double tax1;
    int age;
}
