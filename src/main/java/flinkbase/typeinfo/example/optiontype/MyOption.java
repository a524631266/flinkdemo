package flinkbase.typeinfo.example.optiontype;

import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(MyOptionTypeInfoFactory.class)
public class MyOption<T> {
}
