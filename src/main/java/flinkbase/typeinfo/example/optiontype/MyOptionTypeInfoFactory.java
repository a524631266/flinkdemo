package flinkbase.typeinfo.example.optiontype;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

public class MyOptionTypeInfoFactory<T> extends TypeInfoFactory<MyOption<T>> {

    @Override
    public TypeInformation<MyOption<T>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new MyOptionTypeInfo(genericParameters.get("T"));
    }
}
