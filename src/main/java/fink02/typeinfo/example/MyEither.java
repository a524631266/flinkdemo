package fink02.typeinfo.example;

import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(MyEitherTypeInfoFactory.class)
public class MyEither<T, T1> {

}
