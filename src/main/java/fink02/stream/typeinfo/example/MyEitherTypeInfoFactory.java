package fink02.stream.typeinfo.example;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;


import java.lang.reflect.Type;
import java.util.Map;

public class MyEitherTypeInfoFactory<T, T1> extends TypeInfoFactory<MyEither<T, T1>> {
    @Override
    public TypeInformation<MyEither<T, T1>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new EitherTypeInfo(genericParameters.get("T"), genericParameters.get("T1"));
    }
}
