package fink02.stream.typeinfo.example.protocol;

import org.apache.flink.api.common.typeinfo.TypeInfo;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@TypeInfo(ProtoColFactory.class)
public class ProtoColType<T> {
    // 原生对象
    public T rawObject;
}
