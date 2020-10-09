package flinkbase.typeinfo.example;

import org.apache.flink.api.common.functions.MapFunction;

public class MyEitherMapper<T> implements MapFunction<T, MyEither<T, String>> {
    @Override
    public MyEither<T, String> map(T value) throws Exception {
        return null;
    }
}
