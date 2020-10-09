package flinkbase.typeinfo.example.protocol;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;

import java.lang.reflect.Type;
import java.util.Map;

public class ProtoColFactory<T> extends TypeInfoFactory<ProtoColType<T>> {
    @Override
    public TypeInformation<ProtoColType<T>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new ProtoColTypeInfo(TypeExtractionUtils.typeToClass(t), genericParameters.get("T"));
    }
}
