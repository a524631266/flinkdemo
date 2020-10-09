package fink02.stream.typeinfo.example.optiontype;

import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(MyOptionTypeInfoFactory.class)
public class MyOption<T> {
}
