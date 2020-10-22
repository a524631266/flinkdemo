package flinkbase.dataset.model;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import java.lang.reflect.Type;
import java.util.Map;

public class CustomTypeInfFactory extends TypeInfoFactory<Employee> {
    @Override
    public TypeInformation<Employee> createTypeInfo(Type t,
                                                    Map<String, TypeInformation<?>> genericParameters) {
        // TODO
        // 这里 genericParameters 表示泛型参数，这里暂时没有
        return new TypeInformation<Employee>() {
            @Override
            public boolean isBasicType() {
                return false;
            }

            @Override
            public boolean isTupleType() {
                return false;
            }

            @Override
            public int getArity() {
                return 8;
            }

            @Override
            public int getTotalFields() {
                return 8;
            }

            @Override
            public Class<Employee> getTypeClass() {
                return (Class<Employee>) t;
            }

            @Override
            public boolean isKeyType() {
                return false;
            }

            @Override
            public TypeSerializer<Employee> createSerializer(ExecutionConfig config) {
                return new KryoSerializer<Employee>((Class<Employee>) t, config);
            }

            @Override
            public String toString() {
                return null;
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public boolean canEqual(Object obj) {
                return false;
            }
        };
    }
}
