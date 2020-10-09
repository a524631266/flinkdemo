package flinkbase.typeinfo.example.protocol;

import org.apache.flink.api.common.functions.MapFunction;

public class ProtocolMapMapper<T> implements MapFunction<ProtoColType<T>, String> {
    @Override
    public String map(ProtoColType<T> value) throws Exception {
        return null;
    }
}
