package flinkbase.typeinfo.example.protocol;

import org.apache.flink.api.common.typeinfo.TypeInfo;


@TypeInfo(ProtoColFactory.class)
public class ProtoColType<T> {
    // 原生对象
    public T rawObject;
}
