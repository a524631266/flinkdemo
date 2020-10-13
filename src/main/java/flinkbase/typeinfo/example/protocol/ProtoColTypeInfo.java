package flinkbase.typeinfo.example.protocol;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * 定义自定义Protocol数据类型的时候，需要定义一个名称，用户可以根据这个名称来确定
 * 是否是Protocol
 * @param <T>
 */
public class ProtoColTypeInfo<T> extends TypeInformation<T> {
    private TypeInformation innerTypeInfo;
    private final Class<T> innerClass;

    /**
     *
     * @param cls 类
     * @param t 参数数据类型
     */
    public ProtoColTypeInfo( Class<T> cls) {
        innerClass = cls;
//        this.innerTypeInfo = t;
    }
    /**
     *
     * @param cls 类
     * @param t 参数数据类型
     */
    public ProtoColTypeInfo( Class<T> cls , TypeInformation<T> t) {
        innerClass = cls;
        this.innerTypeInfo = t;
    }

    public TypeInformation getInnerTypeInfo() {
        return innerTypeInfo == null?TypeExtractor.getForClass(innerClass):innerTypeInfo;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    /**
     * 因为是protocol的序列化，所以，不用细分内部有多少个fields
     * 只有在进行分解的时候会使用到
     * @return
     */
    @Override
    public int getArity() {
        return 0;
    }

    /**
     * 因为是protocol的序列化，所以，不用细分内部有多少个fields
     * 只有在进行分解的时候会使用到
     * @return
     */
    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<T> getTypeClass() {
        return innerClass;
    }

    /**
     * 是否可以用于key， 比如在key Select的时候是否可以作为key
     * @return
     */
    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        // TODO 这里可以自定序列化器，用protocol替代
        return  new KryoSerializer<T>(this.innerClass, config);
    }

    @Override
    public String toString() {

        // TODO
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

    /**
     * 当前类型是否可以比较
     * @param obj
     * @return
     */
    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ProtoColTypeInfo;
    }

    @Override
    public Map<String, TypeInformation<?>> getGenericParameters() {
        Map<String, TypeInformation<?>> parameters = super.getGenericParameters();
        if(parameters.isEmpty()){
            // 节约内存1
            Map<String, TypeInformation<?>> result = new HashMap<>(1);
            result.put("T", getInnerTypeInfo());
            return result;
        }
        return parameters;
    }

}
