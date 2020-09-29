package flink03.dataset.model;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

public class CustomTypeInfFactory extends TypeInfoFactory<Employee> {
    @Override
    public TypeInformation<Employee> createTypeInfo(Type t,
                                                    Map<String, TypeInformation<?>> genericParameters) {
        // TODO
        return TypeInformation.of(Employee.class);
    }
}
