package fink02.typeinfo.example;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.Map;

public class IntLikeTypeInfoFactory extends TypeInfoFactory<IntLike> {
    @Override
    public TypeInformation<IntLike> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return (TypeInformation) Types.INT;
    }
}
